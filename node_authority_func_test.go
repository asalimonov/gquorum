//go:build functional

package gquorum

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func authorityNodeOptions(t testing.TB, root, instance string) NodeOptions {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return NodeOptions{StorageDir: root, Identity: topology.Identity{ClusterID: "cluster", NodeName: "n1", InstanceID: instance}, Bootstrap: BootstrapOptions{Founders: []FoundingMember{{Node: topology.Node{Name: "n1", Type: "worker"}, MemberID: 1, Address: listener.Addr().String()}}}, Meta: MetaOptions{Network: MetaNetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("test")}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func stopAuthorityNode(t testing.TB, n Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNodeAuthorityLifecycleAndMissingRetainedEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "first")
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	stopAuthorityNode(t, n)
	check, err := authoritystore.Open(o.authorityDBPath(), topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 1,
		InstanceID: "inspect"}, authoritystore.RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := check.MetadataAcquired(t.Context())
	if err != nil || !acquired {
		t.Fatal("Node started before recording metadata acquisition", acquired, err)
	}
	if err := check.Close(); err != nil {
		t.Fatal(err)
	}
	id, err := authoritystore.LoadIdentity(o.authorityDBPath())
	if err != nil || !id.Found || id.Identity.MemberID != 1 || id.Identity.InstanceID != "" {
		t.Fatal(id.Identity, id.Found, err)
	}
	second := authorityNodeOptions(t, root, "second")
	n, err = NewNode(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); err != nil {
		t.Fatal("retained restart", err)
	}
	stopAuthorityNode(t, n)
	before, err := os.ReadFile(o.metaDBPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(o.authorityDBPath()); err != nil {
		t.Fatal(err)
	}
	n, err = NewNode(authorityNodeOptions(t, root, "third"))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("lost authority silently recreated", err)
	}
	stopAuthorityNode(t, n)
	after, err := os.ReadFile(o.metaDBPath())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed startup changed retained metadata", err)
	}
	if _, err := os.Stat(o.authorityDBPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("authority file recreated", err)
	}
}

func TestNodeReboundMetadataAcquisitionCrashBoundaries(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before-pin", "after-pin", "after-marker", "lost-acquired-metadata"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			previous := topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 1, InstanceID: "old"}
			granted := previous
			granted.MemberID, granted.InstanceID = 2, "replacement"
			path := filepath.Join(root, "_authority.bolt")
			s, err := authoritystore.Open(path, previous, authoritystore.FreshEvidence)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			if err := s.MarkMetadataAcquired(t.Context(), previous); err != nil {
				t.Fatal(err)
			}
			if err := s.Rebind(t.Context(), previous, granted, "exact-admission"); err != nil {
				t.Fatal(err)
			}
			metaPath := filepath.Join(root, "_meta.bolt")
			if phase != "before-pin" {
				meta, err := metastore.Open(metaPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := meta.EnsureIdentity(granted.NodeName, granted.ClusterID, granted.MemberID); err != nil {
					_ = meta.Close()
					t.Fatal(err)
				}
				if err := meta.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "after-marker" || phase == "lost-acquired-metadata" {
				if err := s.MarkMetadataAcquired(t.Context(), granted); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "lost-acquired-metadata" {
				if err := os.Remove(metaPath); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			granted.InstanceID = "restarted"
			s, err = authoritystore.Open(path, granted, authoritystore.RetainedEvidence)
			if err != nil {
				t.Fatal(err)
			}
			n := &node{ctx: t.Context(), authority: s, opts: NodeOptions{Identity: granted, StorageDir: root}}
			err = n.acquireMetaAuthority(granted)
			if phase == "lost-acquired-metadata" {
				if !errors.Is(err, fault.ErrUnreadable) {
					t.Fatal("lost established member was recreated", err)
				}
				if _, err := os.Stat(metaPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("lost metadata was mutated", err)
				}
			} else if err != nil {
				t.Fatal("authorized acquisition could not resume", err)
			}
		})
	}
}

type retainedAuthorityStore struct {
	storage.Store
	storage.RetirementStore
}

func (retainedAuthorityStore) HasState(context.Context) (bool, error) { return true, nil }

type unusedAuthorityTransport struct{ load.Transport }

func TestNodeRefusesLoadEvidenceWithoutAuthority(t *testing.T) {
	t.Parallel()
	for _, borrowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "WAL", true: "borrowed"}[borrowed], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			o := authorityNodeOptions(t, root, "new")
			o.Loads = LoadOptions{Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
				t.Error("factory ran before authority validation")
				return load.Application{}, fault.ErrInvalid
			}, Transport: unusedAuthorityTransport{}}
			if borrowed {
				s := retainedAuthorityStore{}
				o.Loads.Storage = LoadStorageOptions{Store: s, Retirement: s}
			} else {
				wal := filepath.Join(root, "loads")
				if err := os.Mkdir(wal, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(wal, "unknown-retained-state"), []byte("evidence"), 0600); err != nil {
					t.Fatal(err)
				}
				o.Loads.Storage.WAL = &WALOptions{RootDir: wal}
			}
			n, err := NewNode(o)
			if err != nil {
				t.Fatal(err)
			}
			if err := n.Start(t.Context()); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal(err)
			}
			stopAuthorityNode(t, n)
			for _, path := range []string{o.metaDBPath(), o.authorityDBPath()} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("created storage over retained evidence", path, err)
				}
			}
		})
	}
}

// The private callback tests supply verified-grant protocol inputs. The service
// suite separately exercises authenticated quorum proof before invoking them.
func metaHostGrant(id topology.Identity, revision uint64, kind topology.StepKind) topology.AuthorityGrant {
	action := topology.AuthorizedStep{OperationID: topology.MakeOperationID(1, 1), Version: 1, Attempt: 1, StepSeq: 1, Kind: kind, GroupID: topology.MetaGroupID, NodeName: id.NodeName, ReplicaID: id.MemberID}
	return topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: topology.MetaGroupID, Revision: revision}, Purpose: topology.AuthorityEffect, OperationID: action.OperationID, Action: &action, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: id.NodeName}}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: id.MemberID}}}
}
func metaHostRequest(id topology.Identity, grant topology.AuthorityGrant) load.ActionRequest {
	return load.ActionRequest{From: id, To: load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}}, Grant: grant, Authorization: *grant.Action, LocalTarget: storage.Identity{ClusterID: id.ClusterID, NodeName: id.NodeName, GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}}
}
func TestNodeMetaHostPrepareStopAndRetainedReceipt(t *testing.T) {
	t.Parallel()
	options := authorityNodeOptions(t, t.TempDir(), "process")
	public, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	n := public.(*node)
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err = n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	id := n.Identity()
	prepare := metaHostGrant(id, 100, topology.StepPrepareReplica)
	if _, err = n.applyMetaHostAction(ctx, metaHostRequest(id, prepare)); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("effect ran before host floor", err)
	}
	install := func(grant topology.AuthorityGrant) topology.FenceEvidence {
		t.Helper()
		result, err := n.installAuthorityFence(ctx, load.FenceRequest{From: id, To: id, Grant: grant, Resource: grant.Resources[0]})
		if err != nil || result.Evidence.Validate(grant) != nil {
			t.Fatal(result, err)
		}
		return result.Evidence
	}
	proof := install(prepare)
	result, err := n.applyMetaHostAction(ctx, metaHostRequest(id, prepare))
	if err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(prepare) != nil || result.Receipts[0].LocalSequence != proof.LocalSequence {
		t.Fatal(result, err)
	}
	again, err := n.applyMetaHostAction(ctx, metaHostRequest(id, prepare))
	if err != nil || !reflect.DeepEqual(again, result) {
		t.Fatal("prepare receipt was not idempotent", again, err)
	}
	if err = n.meta.VerifyPreparedMeta(ctx, id); err != nil {
		t.Fatal("prepare stopped participation", err)
	}
	stopped := metaHostGrant(id, 101, topology.StepStopReplica)
	stopProof := install(stopped)
	if stopProof.LocalSequence <= proof.LocalSequence {
		t.Fatal("host floor did not advance")
	}
	if _, err = n.applyMetaHostAction(ctx, metaHostRequest(id, prepare)); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("completed fence admitted older prepare", err)
	}
	result, err = n.applyMetaHostAction(ctx, metaHostRequest(id, stopped))
	if err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(stopped) != nil {
		t.Fatal(result, err)
	}
	if n.meta.IsLeader() {
		t.Fatal("stopped metadata advertised leadership")
	}
	if err = n.meta.VerifyPreparedMeta(ctx, id); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stop receipt preceded participation fencing", err)
	}
	if _, err = n.meta.ReadTopology(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped participant served ordinary reads", err)
	}
	if err = n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.Open(options.authorityDBPath(), id, authoritystore.RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	receipt, err := store.Receipt(ctx, stopped, stopped.Targets[0])
	if err != nil || !receipt.Found || !reflect.DeepEqual(receipt.Receipt, result.Receipts[0]) {
		t.Fatal("stop receipt did not survive owner teardown", receipt.Receipt, receipt.Found, err)
	}
	if _, err = n.applyMetaHostAction(ctx, metaHostRequest(id, stopped)); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped node reopened authority callback", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewNode(authorityNodeOptions(t, filepath.Dir(options.authorityDBPath()), "replacement-process"))
	if err != nil {
		t.Fatal(err)
	}
	defer stopAuthorityNode(t, restarted)
	if err = restarted.Start(t.Context()); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("process restart bypassed durable meta stop", err)
	}
	stopAuthorityNode(t, restarted)
	joiningOptions := authorityNodeOptions(t, filepath.Dir(options.authorityDBPath()), "readmission-process")
	joiningOptions.Node = topology.Node{Name: "n1", Type: "worker"}
	joiningOptions.Bootstrap = BootstrapOptions{Join: true, JoinSeeds: []string{options.Meta.Network.AdvertiseAddress}}
	joining, err := NewNode(joiningOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer stopAuthorityNode(t, joining)
	if err = joining.Start(t.Context()); err != nil {
		t.Fatal("explicit readmission could not retain its control owner", err)
	}
	local := joining.(*node)
	if _, err = local.meta.ReadTopology(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("readmission restarted stopped metadata participation", err)
	}
	if local.meta.IsLeader() || joining.Identity().MemberID != id.MemberID {
		t.Fatal("control startup changed former identity", joining.Identity())
	}
}

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

func (unusedAuthorityTransport) SendSnapshot(context.Context, snapshot.Send) (snapshot.Ack, error) {
	panic("unused authority transport invoked")
}
