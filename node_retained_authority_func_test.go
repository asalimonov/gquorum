//go:build functional

package gquorum

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// A remote proof double isolates Node ingress/lifetime wiring from elections.
// Real quorum proof reads and mutation validation are tested in metaservice.
type retainedProofPeer struct {
	proof atomic.Pointer[metaengine.AuthorityProof]
}

type retainedRemovalHooks struct {
	lifecycle.Hooks
	block            atomic.Bool
	entered, release chan struct{}
}

func (p *retainedRemovalHooks) Removal(context.Context, lifecycle.RemovalInput) (lifecycle.RemovalDecision, error) {
	if p.block.Load() {
		close(p.entered)
		<-p.release
		return lifecycle.RemovalDecision{Action: lifecycle.StopNode}, nil
	}
	return lifecycle.RemovalDecision{Action: lifecycle.KeepDiagnostics}, nil
}

func (*retainedProofPeer) Step(context.Context, *metapb.Header, raftpb.Message) error {
	return fault.ErrUnsupported
}
func (p *retainedProofPeer) Call(_ context.Context, method string, _ *metapb.Header, data []byte) *metapb.Reply {
	var request struct {
		Key topology.AuthorityKey `json:"key"`
	}
	proof := p.proof.Load()
	if method != "AuthorityRead" || json.Unmarshal(data, &request) != nil || proof == nil || request.Key != proof.Grant.Key {
		return metatransport.Reply(fault.ErrOperationAuthorization)
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func TestNodeRetainsGrantedIngressAfterWholeNodeFence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "process")
	defaults, err := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hooks := &retainedRemovalHooks{Hooks: defaults, entered: make(chan struct{}), release: make(chan struct{})}
	o.Lifecycle.Hooks = hooks
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &retainedProofPeer{}
	remote, err := metatransport.New(metatransport.Options{Header: &metapb.Header{ClusterId: "cluster", NodeName: "proof"}, Listener: listener,
		AllowPlaintext: true, Credential: o.Meta.Network.ClusterCredential, Handler: peer,
		Resolve:     func(context.Context, uint64) (string, error) { return "", fault.ErrUnavailable },
		SendTimeout: time.Second, SnapshotTimeout: time.Second, RetryInitial: time.Millisecond, RetryMax: time.Second, MaxSnapshotBytes: 1 << 20})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if err = remote.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := remote.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	o.Bootstrap.JoinSeeds = []string{listener.Addr().String()}
	var retirements atomic.Int32
	o.Loads = LoadOptions{Transport: unusedAuthorityTransport{}, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			t.Error("fenced node started application")
			return load.Application{}, fault.ErrStopped
		},
		Retire: func(_ context.Context, request load.RetirementRequest) (load.RetirementResult, error) {
			retirements.Add(1)
			return load.RetirementResult{Grant: request.Grant, Identity: request.Identity, Reason: request.Reason, Authorization: request.Authorization, Verified: true}, nil
		}}
	public, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	n := public.(*node)
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(hooks.release) }) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err = n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	id := n.Identity()
	a := topology.AuthorizedStep{OperationID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, StepSeq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "work", NodeName: id.NodeName, ReplicaID: 1}
	target := topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: 1}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: "work", Revision: 100}, Purpose: topology.AuthorityEffect, OperationID: a.OperationID, Action: &a,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "work", NodeName: id.NodeName}}, Targets: []topology.AuthorityTarget{target}}
	peer.proof.Store(&metaengine.AuthorityProof{Grant: &grant, Courier: id, CleanupTargets: []topology.AuthorityTarget{target}})
	if err = n.onRemoved(ctx, "verified test removal"); err != nil {
		t.Fatal(err)
	}
	if err = n.joinMetaHost(ctx, n.meta); err != nil {
		t.Fatal(err)
	}
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	if _, err = n.StartLoadGroup(ctx, ref); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("ordinary ingress reopened", err)
	}
	if _, err = n.InstallLoadFence(ctx, load.FenceRequest{From: id, To: id, Resource: grant.Resources[0], Grant: grant}); err != nil {
		t.Fatal("fenced Node refused exact host fence", err)
	}
	req := load.ActionRequest{From: id, To: load.Peer{Node: id, Replica: ref}, Authorization: a, Grant: grant,
		LocalTarget: storage.Identity{ClusterID: id.ClusterID, NodeName: id.NodeName, GroupID: "work", ReplicaID: 1}}
	result, err := n.ApplyLoadAction(ctx, req)
	if err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(grant) != nil || retirements.Load() != 1 {
		t.Fatal("fenced Node failed cleanup", result, err, retirements.Load())
	}
	req.From.InstanceID = "former"
	if _, err = n.ApplyLoadAction(ctx, req); !errors.Is(err, fault.ErrIdentityMismatch) || retirements.Load() != 1 {
		t.Fatal("stale courier repeated cleanup", err)
	}
	stop := metaHostGrant(id, 101, topology.StepStopReplica)
	if _, err = n.installAuthorityFence(ctx, load.FenceRequest{From: id, To: id, Grant: stop, Resource: stop.Resources[0]}); err != nil {
		t.Fatal(err)
	}
	stopped, err := n.applyMetaHostAction(ctx, metaHostRequest(id, stop))
	if err != nil || len(stopped.Receipts) != 1 {
		t.Fatal(stopped, err)
	}
	proof := metaengine.AuthorityProof{Grant: &stop, Courier: id, Receipts: stopped.Receipts}
	peer.proof.Store(&proof)
	removal := topology.SelfRemovalStatus{Fenced: true, MemberID: id.MemberID, Version: 2, AppliedIndex: 100, Reason: "verified test removal"}
	if n.handleRemoval(n.meta, removal) {
		t.Fatal("metadata stop receipt alone released removal hooks")
	}
	// A newer floor must not erase the completed stop needed for release.
	fence := stop.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	if _, err = n.installAuthorityFence(ctx, load.FenceRequest{From: id, To: id, Grant: fence, Resource: fence.Resources[0]}); err != nil {
		t.Fatal(err)
	}
	released := proof
	released.ReleasedHosts = stop.Targets
	peer.proof.Store(&released)
	if !n.handleRemoval(n.meta, removal) {
		t.Fatal("settled host obligations did not release removal hooks")
	}
	hooks.block.Store(true)
	decided := make(chan bool, 1)
	go func() { decided <- n.handleRemoval(n.meta, removal) }()
	select {
	case <-hooks.entered:
	case <-ctx.Done():
		t.Fatal("removal hooks did not enter")
	}
	// Model the exact ownership reservation made by an authorized readmission.
	// The old hooks result must be rejected before it can request teardown.
	replacement := id
	replacement.MemberID++
	n.mu.Lock()
	n.pending = &replacement
	n.mu.Unlock()
	releaseOnce.Do(func() { close(hooks.release) })
	if <-decided {
		t.Fatal("old removal hooks accepted after readmission reservation")
	}
	n.mu.RLock()
	stopping := n.stopping
	n.mu.RUnlock()
	if stopping {
		t.Fatal("old removal decision stopped the replacement owner")
	}
	stopAuthorityNode(t, n)
	if _, err = n.InstallLoadFence(ctx, load.FenceRequest{From: id, To: id, Resource: grant.Resources[0], Grant: grant}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("full shutdown retained ingress", err)
	}
}
