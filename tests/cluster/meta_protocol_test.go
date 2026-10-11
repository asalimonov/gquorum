//go:build cluster

package cluster

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/asalimonov/gquorum"
	"io"
	"net"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMetaCredentialClusterIsolationThreeNodes(t *testing.T) { metaCredentialIsolation(t, 3) }
func TestMetaCredentialClusterIsolationFiveNodes(t *testing.T)  { metaCredentialIsolation(t, 5) }
func TestMetaCredentialClusterIsolationSevenNodes(t *testing.T) { metaCredentialIsolation(t, 7) }

// Source shared-token founding plus actual unary/stream protocol isolation.
// The private test generation publishes random meta ports only for this case.
func metaCredentialIsolation(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.PublishMeta = true
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	credential := hex.EncodeToString([]byte(c.Credential))
	valid := metadata.AppendToOutgoingContext(ctx, "gquorum-cluster-credential", credential)
	for _, node := range c.Nodes() {
		local, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		if err != nil || len(local.Status.ConfState.Voters) != size || local.Identity.MemberID != created.Document.MemberID(node.Spec.Name) {
			t.Fatal("shared-token founding failed", local, err)
		}
		host, err := node.Container.Host(ctx)
		if err != nil {
			t.Fatal(err)
		}
		port, err := node.Container.MappedPort(ctx, "9000/tcp")
		if err != nil {
			t.Fatal(err)
		}
		conn, err := grpc.NewClient(net.JoinHostPort(host, port.Port()), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		// Scope connection ownership to this iteration instead of retaining N clients.
		func() {
			defer conn.Close()
			rpc := metapb.NewMetaClient(conn)
			header := &metapb.Header{ClusterId: c.ID, NodeName: node.Spec.Name, MemberId: local.Identity.MemberID, InstanceId: local.Identity.InstanceID}
			request := &metapb.MetaRequest{Header: header}
			ping, err := rpc.IdentityPing(valid, request)
			if err != nil || ping.ErrorCode != "" {
				t.Fatal("matched credential failed", ping, err)
			}
			var identity topology.Identity
			if err := json.Unmarshal(ping.Data, &identity); err != nil || identity != local.Identity {
				t.Fatal("RPC returned another process identity", identity, local.Identity, err)
			}
			for _, invalid := range []context.Context{
				ctx,
				metadata.AppendToOutgoingContext(ctx, "gquorum-cluster-credential", "foreign"),
				metadata.AppendToOutgoingContext(valid, "gquorum-cluster-credential", credential),
			} {
				if _, err := rpc.IdentityPing(invalid, request); status.Code(err) != codes.Unauthenticated {
					t.Fatal("unary accepted absent/foreign/duplicate credential", err)
				}
				messages, err := rpc.StreamMessages(invalid)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := messages.CloseAndRecv(); status.Code(err) != codes.Unauthenticated {
					t.Fatal("Raft stream accepted invalid credential", err)
				}
				snapshots, err := rpc.SendSnapshot(invalid)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := snapshots.CloseAndRecv(); status.Code(err) != codes.Unauthenticated {
					t.Fatal("snapshot stream accepted invalid credential", err)
				}
			}
			foreign := &metapb.Header{ClusterId: "foreign-cluster", NodeName: header.NodeName, MemberId: header.MemberId, InstanceId: header.InstanceId}
			refusal, err := rpc.IdentityPing(valid, &metapb.MetaRequest{Header: foreign})
			if err != nil || refusal.ErrorCode != string(fault.IdentityMismatch) {
				t.Fatal("credential bypassed cluster identity", refusal, err)
			}
			messages, err := rpc.StreamMessages(valid)
			if err != nil {
				t.Fatal(err)
			}
			if err := messages.Send(&metapb.RaftMessage{Header: foreign, ToMemberId: header.MemberId}); err != nil && err != io.EOF {
				t.Fatal(err)
			}
			refusal, err = messages.CloseAndRecv()
			if err != nil || refusal.ErrorCode != string(fault.IdentityMismatch) {
				t.Fatal("Raft stream accepted foreign cluster", refusal, err)
			}
			snapshots, err := rpc.SendSnapshot(valid)
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshots.Send(&metapb.SnapshotChunk{Header: foreign, ToMemberId: header.MemberId, Data: []byte("foreign"), Final: true}); err != nil && err != io.EOF {
				t.Fatal(err)
			}
			refusal, err = snapshots.CloseAndRecv()
			if err != nil || refusal.ErrorCode != string(fault.IdentityMismatch) {
				t.Fatal("snapshot stream accepted foreign cluster", refusal, err)
			}
		}()
	}
	for _, node := range c.Nodes() {
		doc, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil || !reflect.DeepEqual(doc, created.Document) {
			t.Fatal("protocol refusals changed approved topology", node.Spec.Name, doc, err)
		}
		status, err := c.Status(ctx, node.Spec.Name)
		if err != nil || !status.SessionLive || status.MetaLeader == 0 {
			t.Fatal("refusals disturbed admission", status, err)
		}
		response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/cluster/parked", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		var warnings []topology.ParkedJoin
		if err := response.Decode(&warnings); err != nil || len(warnings) != 0 {
			t.Fatal("foreign traffic became a parked admission", warnings, err)
		}
	}
	if err := c.Put(ctx, "node2", "work", "authenticated", "authenticated", "committed"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", map[string]string{"authenticated": "committed"}); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}
