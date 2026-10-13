//go:build functional

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

func proxyTestBytes(field protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, field, protowire.BytesType), value)
}

func proxyTestFenceFrame(request load.FenceRequest) proxyFrame {
	header := proxyTestBytes(1, []byte(request.From.ClusterID))
	header = append(header, proxyTestBytes(2, []byte(request.From.NodeName))...)
	header = protowire.AppendVarint(protowire.AppendTag(header, 3, protowire.VarintType), request.From.MemberID)
	header = append(header, proxyTestBytes(6, []byte(request.From.InstanceID))...)
	data, _ := json.Marshal(request)
	return append(proxyTestBytes(1, header), proxyTestBytes(2, data)...)
}

func TestMetaFaultProxyHoldsActualFenceBoundaryAndJoinsCanceledCalls(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			t.Parallel()
			id := topology.Identity{NodeName: "one", ClusterID: "cluster", MemberID: 1, InstanceID: "process"}
			node := &authorityProtocolNode{id: id}
			a := &application{node: node, cfg: config{Credential: "credential"}, clusterHooks: newClusterHooks(), workCtx: t.Context(), closing: make(chan struct{})}
			external, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer external.Close()
			internal, err := a.newMetaFaultProxy(external)
			if err != nil {
				t.Fatal(err)
			}
			defer internal.Close()
			backend := grpc.NewServer(grpc.ForceServerCodec(proxyCodec{}), grpc.WaitForHandlers(true), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
				method, _ := grpc.MethodFromServerStream(stream)
				var frame proxyFrame
				var frames []byte
				for {
					err := stream.RecvMsg(&frame)
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
					frames = append(frames, frame...)
				}
				if method == "/gquorum.meta.v1.Meta/InstallFence" {
					request, err := proxyFenceRequest(frames)
					if err != nil {
						return err
					}
					result, err := node.InstallLoadFence(stream.Context(), request)
					if err != nil {
						return err
					}
					data, _ := json.Marshal(result)
					frame = proxyTestBytes(4, data)
				} else {
					frame = frames
				}
				return stream.SendMsg(&frame)
			}))
			var servers sync.WaitGroup
			servers.Go(func() { _ = backend.Serve(internal) })
			servers.Go(func() { _ = a.metaProxy.server.Serve(external) })
			t.Cleanup(func() {
				a.metaProxy.server.Stop()
				backend.Stop()
				_ = a.metaProxy.conn.Close()
				servers.Wait()
			})
			conn, err := grpc.NewClient("passthrough:///"+external.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(proxyCodec{})))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("gquorum-cluster-credential", hex.EncodeToString([]byte(a.cfg.Credential))))
			resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: id.NodeName}
			grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: "work", Revision: 9007199254740993}, Purpose: topology.AuthorityFence, Resources: []topology.AuthorityResource{resource}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: 7}}}
			request := load.FenceRequest{From: id, To: id, Resource: resource, Grant: grant}
			frame := proxyTestFenceFrame(request)
			key := fenceControlKey{"work", topology.AuthorityHost}
			arm := func() {
				// Fixture control state is owned under the same mutex as the HTTP
				// control route; this test concentrates on actual protocol delivery.
				a.clusterHooks.mu.Lock()
				a.clusterHooks.fenceBarriers[key] = &fenceControl{identity: id, purpose: topology.AuthorityFence, minRevision: grant.Key.Revision, after: after, release: make(chan struct{})}
				a.clusterHooks.mu.Unlock()
			}
			arm()
			var reply proxyFrame
			if err := conn.Invoke(t.Context(), "/gquorum.meta.v1.Meta/InstallFence", &frame, &reply); status.Code(err) != codes.Unauthenticated {
				t.Fatal("unauthenticated request entered fault delivery", err)
			}
			caller, cancel := context.WithCancel(ctx)
			joined := make(chan error, 1)
			go func() {
				var reply proxyFrame
				joined <- conn.Invoke(caller, "/gquorum.meta.v1.Meta/InstallFence", &frame, &reply)
			}()
			apiEventually(t, func() bool { return a.authorityObservation("work").Barriers[0].Hits > 0 })
			if view := a.authorityObservation("work"); node.fences.Load() != uint64(len(view.Fences)) || after && node.fences.Load() != 1 || !after && node.fences.Load() != 0 || view.Barriers[0].Last.Grant.Key != grant.Key {
				t.Fatal("metadata hold misrepresented the installation boundary", view, node.fences.Load())
			}
			cancel()
			if err := <-joined; status.Code(err) != codes.Canceled {
				t.Fatal("canceled wait fabricated completion", err)
			}
			a.clusterHooks.mu.Lock()
			close(a.clusterHooks.fenceBarriers[key].release)
			delete(a.clusterHooks.fenceBarriers, key)
			a.clusterHooks.mu.Unlock()
			if err := conn.Invoke(ctx, "/gquorum.meta.v1.Meta/InstallFence", &frame, &reply); err != nil {
				t.Fatal("exact retry after releasing metadata hold", err)
			}
			data, err := proxyBytesField(reply, 4)
			var result load.FenceResult
			if err != nil || json.Unmarshal(data, &result) != nil || result.Evidence.Validate(grant) != nil {
				t.Fatal("proxy changed the exact evidence", result, err)
			}
			// Forward multiple opaque frames without re-encoding or buffering a
			// complete metadata stream. This covers Raft/snapshot route mechanics.
			stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, "/gquorum.meta.v1.Meta/StreamMessages")
			if err != nil {
				t.Fatal(err)
			}
			first, second := proxyFrame("first-frame"), proxyFrame("second-frame")
			if err := stream.SendMsg(&first); err != nil {
				t.Fatal(err)
			}
			if err := stream.SendMsg(&second); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if err := stream.RecvMsg(&reply); err != nil || !bytes.Equal(reply, append(bytes.Clone(first), second...)) {
				t.Fatal("proxy altered the metadata stream", reply, err)
			}
			control := func(identity topology.Identity, credential string, drop bool) *httptest.ResponseRecorder {
				body, _ := json.Marshal(map[string]any{"identity": identity, "drop_raft": drop})
				r := httptest.NewRequest("PUT", "/internal/test/meta", bytes.NewReader(body))
				r.Header.Set("X-Gquorum-Test-Credential", credential)
				w := httptest.NewRecorder()
				a.routes().ServeHTTP(w, r)
				return w
			}
			foreign := id
			foreign.InstanceID = "replacement"
			if w := control(foreign, a.cfg.Credential, true); w.Code != 409 {
				t.Fatal("foreign process armed metadata drop", w.Code)
			}
			if w := control(id, "foreign", true); w.Code != 403 {
				t.Fatal("unauthenticated metadata fault", w.Code)
			}
			if w := control(id, a.cfg.Credential, true); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			for _, target := range []uint64{id.MemberID, id.MemberID + 1} {
				stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, "/gquorum.meta.v1.Meta/SendSnapshot")
				if err != nil {
					t.Fatal(err)
				}
				frame := proxyFrame(protowire.AppendVarint(protowire.AppendTag(nil, 2, protowire.VarintType), target))
				if err := stream.SendMsg(&frame); err != nil {
					t.Fatal(err)
				}
				if err := stream.CloseSend(); err != nil {
					t.Fatal(err)
				}
				err = stream.RecvMsg(&reply)
				if target == id.MemberID && status.Code(err) != codes.Unavailable || target != id.MemberID && (err != nil || !bytes.Equal(frame, reply)) {
					t.Fatal("metadata drop acknowledged a snapshot or affected another member", target, reply, err)
				}
			}
			if err := conn.Invoke(ctx, "/gquorum.meta.v1.Meta/InstallFence", &frame, &reply); err != nil {
				t.Fatal("metadata Raft fault also blocked the resource-control path", err)
			}
			a.clusterHooks.mu.Lock()
			dropped := a.clusterHooks.metaRaft.FramesDropped
			a.clusterHooks.mu.Unlock()
			if dropped != 1 {
				t.Fatal("wrong metadata drop scope", dropped)
			}
			arm()
			go func() {
				var reply proxyFrame
				joined <- conn.Invoke(ctx, "/gquorum.meta.v1.Meta/InstallFence", &frame, &reply)
			}()
			apiEventually(t, func() bool { return a.authorityObservation("work").Barriers[0].Hits > 0 })
			stopped := make(chan struct{})
			go func() { a.metaProxy.server.Stop(); close(stopped) }()
			select {
			case <-stopped:
			case <-ctx.Done():
				t.Fatal("proxy shutdown did not join a held call", ctx.Err())
			}
			if err := <-joined; err == nil {
				t.Fatal("stopping proxy manufactured fence acknowledgement")
			}
		})
	}
}
