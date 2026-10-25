//go:build functional

package recovery_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"

	"github.com/asalimonov/gquorum/internal/recovery/model"
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

func TestRecoveryInspectAndApproveRepeatNetworkPrecheckWithoutChangingDigest(t *testing.T) {
	t.Parallel()
	handler := &recoveryProbeHandler{reply: func(context.Context) *metapb.Reply { return metatransport.Reply(fault.ErrUnavailable) }}
	address := recoveryProbeServer(t, "cluster", handler)
	var reachable atomic.Bool
	resolver := recoveryProbeResolver{resolve: func(context.Context, string) (string, error) {
		if !reachable.Load() {
			return "", errors.New("peer deliberately offline")
		}
		return address, nil
	}}
	options := authorityNodeOptions(t, t.TempDir(), "probe-source")
	options.Meta.Network.Resolver = resolver
	options.Meta.Network.ClusterCredential = []byte("credential")
	participant, err := newNode(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := participant.Cluster().AddNode(ctx, topology.Node{Name: "n2", Type: "worker"}, quorum.MutationOptions{
		ExpectedVersion: 1, Actor: "fixture", Approve: true}); err != nil {
		t.Fatal(err)
	}
	stopAuthorityNode(t, participant)
	facade, err := newRecovery(options)
	if err != nil {
		t.Fatal(err)
	}
	request := model.Request{RecoveryID: "recheck", Mode: model.Rebuild, Actor: "operator", Comment: "permanent loss"}
	plan, err := facade.Inspect(ctx, request)
	if err != nil {
		t.Fatal("single-survivor source rejected", err, plan.Issues)
	}
	reachable.Store(true)
	alive, err := facade.Inspect(ctx, request)
	if err == nil || alive.Digest != plan.Digest {
		t.Fatal("reinspection lost stable plan or live blocker", alive, err)
	}
	acceptance := model.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	if _, err := facade.Approve(ctx, request, acceptance); err == nil {
		t.Fatal("attestation bypassed reachable old endpoint")
	}
	reachable.Store(false)
	operation, err := facade.Approve(ctx, request, acceptance)
	if err != nil {
		t.Fatal(err)
	}
	reachable.Store(true)
	if _, err := facade.Approve(ctx, request, acceptance); err == nil {
		t.Fatal("approval replay skipped reachability precheck")
	}
	stored, err := facade.Status(ctx, operation.RecoveryID)
	if err != nil || stored.Phase != model.Approved || !reflect.DeepEqual(stored.History[0].Phase, model.Approved) {
		t.Fatal(stored, err)
	}
}
