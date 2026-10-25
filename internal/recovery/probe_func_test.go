//go:build functional

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type recoveryProbeResolver struct {
	resolve func(context.Context, string) (string, error)
}

func (r recoveryProbeResolver) Resolve(ctx context.Context, name string) (string, error) {
	return r.resolve(ctx, name)
}

type recoveryProbeHandler struct {
	reply func(context.Context) *metapb.Reply
	calls atomic.Int32
}

func (*recoveryProbeHandler) Step(context.Context, *metapb.Header, raftpb.Message) error { return nil }
func (h *recoveryProbeHandler) Call(ctx context.Context, _ string, _ *metapb.Header, _ []byte) *metapb.Reply {
	h.calls.Add(1)
	return h.reply(ctx)
}

func recoveryProbeServer(t *testing.T, cluster string, handler *recoveryProbeHandler) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport, err := metatransport.New(metatransport.Options{Listener: listener,
		Header:  &metapb.Header{ClusterId: cluster, NodeName: "n2", MemberId: 2, InstanceId: "old-process"},
		Handler: handler, Resolve: func(context.Context, uint64) (string, error) { return listener.Addr().String(), nil },
		Credential: []byte("credential"), AllowPlaintext: true, SendTimeout: time.Second, SnapshotTimeout: time.Second,
		RetryInitial: time.Millisecond, RetryMax: 10 * time.Millisecond, MaxSnapshotBytes: 1 << 20})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	if err := transport.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := transport.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return listener.Addr().String()
}

func probeAcceptedFixture(t *testing.T) model.AcceptedPlan {
	t.Helper()
	accepted := recoverytest.Accepted(model.Paths{StorageDir: t.TempDir()}, "probe")
	accepted.Approved.Nodes = append(accepted.Approved.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	meta := accepted.Approved.MetaGroup()
	meta.Replicas = append(meta.Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	meta.NextReplicaID = 3
	accepted.Floors[0].NextID = 3
	accepted.Identity.MemberID = 3
	return accepted
}
func probeFacade(t *testing.T, resolver topology.AddressResolver, credential string) *Recovery {
	t.Helper()
	facade, err := New(Config{Source: model.SourceOptions{Paths: model.Paths{StorageDir: t.TempDir()}, NodeName: "n1", ClusterID: "cluster"}, Probe: ProbeOptions{Resolver: resolver, Credential: []byte(credential), AllowPlaintext: true}})
	if err != nil {
		t.Fatal(err)
	}
	return facade
}

func TestRecoveryProbeClassifiesAuthenticatedOldRepliesAndBindingRejection(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"live", "stopped engine", "wrong cluster", "wrong credential", "deadline"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			handler := &recoveryProbeHandler{reply: func(context.Context) *metapb.Reply {
				data, _ := json.Marshal(topology.Identity{ClusterID: "cluster", NodeName: "n2", MemberID: 2, InstanceID: "restarted-old"})
				return &metapb.Reply{Data: data}
			}}
			if shape == "stopped engine" {
				handler.reply = func(context.Context) *metapb.Reply { return metatransport.Reply(fault.ErrUnavailable) }
			}
			if shape == "deadline" {
				handler.reply = func(ctx context.Context) *metapb.Reply { <-ctx.Done(); return metatransport.Reply(ctx.Err()) }
			}
			cluster, credential := "cluster", "credential"
			if shape == "wrong cluster" {
				cluster = "another-cluster"
			}
			if shape == "wrong credential" {
				credential = "incorrect"
			}
			address := recoveryProbeServer(t, cluster, handler)
			facade := probeFacade(t, recoveryProbeResolver{resolve: func(context.Context, string) (string, error) { return address, nil }}, credential)
			accepted := probeAcceptedFixture(t)
			before, err := model.DigestAccepted(accepted)
			if err != nil {
				t.Fatal(err)
			}
			err = facade.probeAccepted(t.Context(), &accepted)
			blocked := shape == "live" || shape == "stopped engine"
			if (err != nil) != blocked {
				t.Fatal("incorrect reachability result", shape, accepted.Issues, err)
			}
			if blocked && (len(accepted.Issues) != 1 || accepted.Issues[0].Code != "old_member_reachable" || !accepted.Issues[0].Blocking) {
				t.Fatal(accepted.Issues)
			}
			if shape == "wrong cluster" || shape == "wrong credential" {
				if len(accepted.Issues) != 1 || accepted.Issues[0].Blocking || handler.calls.Load() != 0 {
					t.Fatal("binding rejection reached handler or blocked", accepted.Issues, handler.calls.Load())
				}
			}
			after, err := model.DigestAccepted(accepted)
			if err != nil || before != after {
				t.Fatal("network observation changed acceptance", err)
			}
		})
	}
}

func TestRecoveryProbeClosedPortMissingResolverAndCancellation(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	accepted := probeAcceptedFixture(t)
	facade := probeFacade(t, recoveryProbeResolver{resolve: func(context.Context, string) (string, error) { return address, nil }}, "credential")
	if err := facade.probeAccepted(t.Context(), &accepted); err != nil || len(accepted.Issues) != 0 {
		t.Fatal(accepted.Issues, err)
	}
	accepted = probeAcceptedFixture(t)
	missing := probeFacade(t, nil, "credential")
	if err := missing.probeAccepted(t.Context(), &accepted); err != nil || len(accepted.Issues) != 1 || accepted.Issues[0].Code != "peer_probe_unavailable" || accepted.Issues[0].Blocking {
		t.Fatal(accepted.Issues, err)
	}
	accepted = probeAcceptedFixture(t)
	unresolved := probeFacade(t, recoveryProbeResolver{resolve: func(context.Context, string) (string, error) { return "", errors.New("unresolved") }}, "credential")
	if err := unresolved.probeAccepted(t.Context(), &accepted); err != nil {
		t.Fatal(err)
	}
	counted := false
	for _, issue := range accepted.Issues {
		if issue.Code == "peer_probe_incomplete" && strings.Contains(issue.Message, "0 of 1") && !issue.Blocking {
			counted = true
		}
	}
	if !counted {
		t.Fatal("unresolved peers appeared checked", accepted.Issues)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := facade.probeAccepted(ctx, &accepted); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
