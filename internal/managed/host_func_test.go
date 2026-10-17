//go:build functional

package managed

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
)

type fixture struct {
	mu      sync.Mutex
	node    topology.Identity
	scopes  map[string]metaengine.LoadScope
	floors  map[string]topology.AuthorityGrant
	records map[string]metastore.ReplicaRecord
	store   *walstore.Store
	cfg     Config
}

func newFixture(t *testing.T) *fixture {
	return newFixtureAt(t, "a", 1)
}

func newFixtureAt(t *testing.T, name string, member uint64) *fixture {
	t.Helper()
	prewrite := false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := walstore.Open(walstore.Options{RootDir: t.TempDir(), NodeName: name, ClusterID: "cluster", SegmentBytes: 32 << 10, Prewrite: &prewrite, Logger: logger})
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	f := &fixture{node: topology.Identity{NodeName: name, ClusterID: "cluster", MemberID: member, InstanceID: "process-" + name}, scopes: make(map[string]metaengine.LoadScope), floors: make(map[string]topology.AuthorityGrant), records: make(map[string]metastore.ReplicaRecord), store: s}
	f.setScope("work", 1, true, false)
	f.setScope("other", 1, true, false)
	f.cfg = Config{Identity: func() topology.Identity { return f.node }, Store: s, Logger: logger, Transport: discardTransport{},
		EffectTimeout: time.Second, CallbackTimeout: 3 * time.Second, RecoveryInterval: time.Hour,
		Engine: load.EngineOptions{Timing: load.Timing{TickInterval: 10 * time.Millisecond}},
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			return load.Application{StateMachine: &stateMachine{}}, nil
		},
		Scope: func(ctx context.Context, id storage.Identity) (metaengine.LoadScope, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			scope, found := f.scopes[id.GroupID]
			if !found || scope.Replica.ReplicaID != id.ReplicaID || id.NodeName != f.node.NodeName || id.ClusterID != f.node.ClusterID {
				return metaengine.LoadScope{}, fault.ErrIdentityMismatch
			}
			return scope, ctx.Err()
		},
		LookupRecord: func(ctx context.Context, id string) (metastore.ReplicaLookup, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			r, ok := f.records[id]
			return metastore.ReplicaLookup{Record: r, Found: ok}, ctx.Err()
		},
		RecordReplica: func(ctx context.Context, group string, record metastore.ReplicaRecord, previous uint64) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			if f.scopes[group].Replica.ReplicaID != record.ReplicaID {
				return fault.ErrIdentityMismatch
			}
			if old, found := f.records[group]; found {
				if old.ReplicaID == record.ReplicaID {
					return nil
				}
				if old.ReplicaID != previous {
					return fault.ErrIdentityMismatch
				}
			}
			f.records[group] = record
			return nil
		},
		ResolvePeer: func(ctx context.Context, ref load.ReplicaRef) (load.Peer, error) {
			node := f.node
			if ref.ReplicaID != 1 {
				node.NodeName, node.MemberID, node.InstanceID = "b", 2, "remote"
			}
			return load.Peer{Node: node, Replica: ref}, ctx.Err()
		},
		ValidatePeer: func(ctx context.Context, peer load.Peer) error {
			if !completePeer(peer) || peer.Node.ClusterID != "cluster" {
				return fault.ErrIdentityMismatch
			}
			return ctx.Err()
		},
		ValidateNode: func(ctx context.Context, node topology.Identity) error {
			if !completeIdentity(node) || node.ClusterID != "cluster" {
				return fault.ErrIdentityMismatch
			}
			return ctx.Err()
		},
	}
	hooks, err := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
	must(t, err)
	f.cfg.Retirement, f.cfg.Retention = s, hooks.Retention
	f.cfg.Catalog = func(ctx context.Context, group string) ([]storage.Identity, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var ids []storage.Identity
		for _, r := range f.scopes[group].Group.Replicas {
			ids = append(ids, storage.Identity{NodeName: r.NodeName, ClusterID: f.node.ClusterID, GroupID: group, ReplicaID: r.ReplicaID})
		}
		return ids, ctx.Err()
	}
	f.cfg.Progress = func(ctx context.Context) (topology.ProgressPage, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		page := topology.ProgressPage{}
		for _, scope := range f.scopes {
			group := scope.Group
			row := topology.GroupProgress{GroupID: group.ID, Approved: &group}
			if scope.Established {
				row.ConvergedVersion, row.Converged = scope.Version, &group
			}
			page.Groups = append(page.Groups, row.Clone())
			page.TargetVersion = max(page.TargetVersion, scope.Version)
		}
		return page, ctx.Err()
	}
	f.cfg.ForgetGrantedReplica = func(ctx context.Context, grant topology.AuthorityGrant, expected uint64) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if grant.Validate() != nil || grant.Action == nil ||
			!slices.Contains(grant.Targets, topology.AuthorityTarget{NodeName: f.node.NodeName, MemberID: f.node.MemberID, ReplicaID: expected}) {
			return fault.ErrOperationAuthorization
		}
		a := *grant.Action
		if record, found := f.records[a.GroupID]; found && record.ReplicaID != expected {
			return fault.ErrIdentityMismatch
		}
		delete(f.records, a.GroupID)
		return nil
	}
	fixtureAuthority(t, f)
	f.cfg.AuthorityFloors = func(ctx context.Context) ([]topology.AuthorityGrant, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var grants []topology.AuthorityGrant
		for _, grant := range f.floors {
			grants = append(grants, grant.Clone())
		}
		return grants, ctx.Err()
	}
	return f
}

func (f *fixture) setScope(group string, id uint64, founding, established bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := topology.Replica{NodeName: f.node.NodeName, ReplicaID: id, Role: topology.ReplicaVoter}
	f.scopes[group] = metaengine.LoadScope{Version: 1, Group: topology.Group{ID: group, Replicas: []topology.Replica{r}}, Replica: r, Founding: founding, Started: established, Established: established}
	old := f.floors[group]
	if old.Key.Revision != 0 && old.Targets[0].ReplicaID == id {
		return
	}
	kind := topology.StepPrepareReplica
	if founding {
		kind = topology.StepCreateFoundingGroup
	}
	a := f.approve(kind, storage.Identity{NodeName: f.node.NodeName, ReplicaID: id})
	a.GroupID = group
	resources := []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: group, NodeName: f.node.NodeName}}
	if founding {
		resources = append([]topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: group}}, resources...)
	}
	f.floors[group] = topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: f.node.ClusterID, GroupID: group, Revision: max(10, old.Key.Revision+1)}, Purpose: topology.AuthorityEffect, OperationID: a.OperationID, Action: &a, Resources: resources, Targets: []topology.AuthorityTarget{{NodeName: f.node.NodeName, MemberID: f.node.MemberID, ReplicaID: id}}}
}

func (f *fixture) host(t *testing.T) *Host {
	t.Helper()
	var loopback *loopbackTransport
	if _, ok := f.cfg.Transport.(discardTransport); ok {
		loopback = &loopbackTransport{}
		f.cfg.Transport = loopback
	}
	h, err := New(f.cfg)
	if loopback != nil {
		loopback.host = h
	}
	must(t, err)
	must(t, h.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		must(t, h.Stop(ctx))
	})
	return h
}

type stateMachine struct{}

func (*stateMachine) Apply(context.Context, load.Command) (load.Result, error) {
	return load.Result{Code: "ok"}, nil
}

func (*stateMachine) Snapshot(context.Context) ([]byte, error) { return []byte("{}"), nil }

func (*stateMachine) Restore(context.Context, []byte) error { return nil }

type discardTransport struct{}

func (discardTransport) SendRaft(ctx context.Context, _ load.RaftBatch) error { return ctx.Err() }

func (discardTransport) ReadReplica(context.Context, load.ReplicaQuery) (load.ReplicaStatus, error) {
	return load.ReplicaStatus{}, fault.ErrUnavailable
}

func (discardTransport) ApplyAction(context.Context, load.ActionRequest) (load.ActionResult, error) {
	return load.ActionResult{}, fault.ErrUnavailable
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not reach expected boundary")
	}
}

func functional(t *testing.T) {
	t.Helper()
	t.Parallel()
}

func deadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCanceledWaiterDoesNotCancelOwnedStartOrBlockAnotherGroup(t *testing.T) {
	functional(t)
	f := newFixture(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	f.cfg.Factory = func(ctx context.Context, spec load.ReplicaSpec) (load.Application, error) {
		if spec.Identity.GroupID == "work" {
			calls.Add(1)
			close(entered)
			select {
			case <-proceed:
			case <-ctx.Done():
				return load.Application{}, ctx.Err()
			}
		}
		// Factory-owned data must not alter approved founding peers.
		spec.Group.Replicas[0].ReplicaID = 99
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	caller, cancel := context.WithCancel(deadline(t))
	first := make(chan error, 1)
	go func() { _, err := h.StartReplica(caller, ref, StartExplicit); first <- err }()
	waitSignal(t, entered)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	other, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "other", ReplicaID: 1}, StartExplicit)
	must(t, err)
	if !other.Core().Running() {
		t.Fatal("unrelated group blocked")
	}
	type result struct {
		handle *loadhandle.Handle
		err    error
	}
	second := make(chan result, 1)
	go func() { g, err := h.StartReplica(deadline(t), ref, StartExplicit); second <- result{g, err} }()
	close(proceed)
	got := <-second
	must(t, got.err)
	again, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	if got.handle != again || calls.Load() != 1 || !slices.Equal(again.Core().Status().ConfState.Voters, []uint64{1}) {
		t.Fatal("startup was duplicated or factory changed consensus peers", calls.Load(), again.Core().Status())
	}
}

func TestExplicitHoldRequiresAnAuthorizedExplicitRestart(t *testing.T) {
	functional(t)
	f := newFixture(t)
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	if _, err := h.StartReplica(deadline(t), ref, StartRecovery); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	if _, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2}, StartExplicit); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
	if _, err := h.StartReplica(deadline(t), ref, StartOperator); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("invalid explicit start cleared another identity's hold", err)
	}
	g, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	if g.Core().Running() {
		t.Fatal("successful Stop left workers running")
	}
	if _, found := h.Lookup("work"); found {
		t.Fatal("stopped handle stayed registered")
	}
	if got := h.Snapshot(); len(got) != 1 || got[0].State != "held" {
		t.Fatal(got)
	}
	must(t, h.Stop(deadline(t)))
	// A new owner has no in-memory hold and adopts the retained exact WAL.
	next := f.host(t)
	if _, err := next.StartReplica(deadline(t), ref, StartRecovery); err != nil {
		t.Fatal(err)
	}
}

func TestCloseFailureRetainsOwnershipAndRetriesSerially(t *testing.T) {
	functional(t)
	f := newFixture(t)
	failure := errors.New("application close incomplete")
	entered, proceed := make(chan struct{}), make(chan struct{})
	var closeCalls atomic.Int32
	var h *Host
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: &stateMachine{}, Close: func(ctx context.Context) error {
			_ = h.Snapshot() // callbacks can inspect the host without a registry lock
			switch closeCalls.Add(1) {
			case 1:
				return failure
			case 2:
				close(entered)
				select {
				case <-proceed:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}, nil
	}
	h = f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	old, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	if err := h.StopReplica(deadline(t), ref, StopExplicit); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := h.StartReplica(deadline(t), ref, StartExplicit); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("Close failure lost ownership", err)
	}
	replacement := load.ReplicaRef{GroupID: "work", ReplicaID: 2}
	if _, err := h.StartReplica(deadline(t), replacement, StartOperator); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("successor overlapped failed Close", err)
	}
	results := make(chan error, 2)
	go func() { results <- h.StopReplica(deadline(t), ref, StopExplicit) }()
	waitSignal(t, entered)
	go func() { results <- h.StopReplica(deadline(t), ref, StopExplicit) }()
	close(proceed)
	// The second request can arrive after unregistering; it is then no longer
	// assigned. Both outcomes preserve exact ownership and never repeat Close.
	joined := false
	for i := 0; i < 2; i++ {
		err = <-results
		joined = joined || err == nil
		if err != nil && !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal(err)
		}
	}
	if !joined {
		t.Fatal("neither stop joined cleanup")
	}
	if closeCalls.Load() != 2 {
		t.Fatal("concurrent cleanup repeated Close", closeCalls.Load())
	}
	if _, err := f.store.Quarantine(deadline(t), "work", 1); err != nil {
		t.Fatal(err)
	}
	f.setScope("work", 2, false, false)
	newHandle, err := h.StartReplica(deadline(t), replacement, StartExplicit)
	must(t, err)
	if newHandle.Identity().ReplicaID != 2 || old.Core().Running() {
		t.Fatal("replacement redirected the old handle")
	}
}

func TestSuccessfulFactoryResultAfterCancellationIsStillClosed(t *testing.T) {
	functional(t)
	f := newFixture(t)
	entered := make(chan struct{})
	var closed atomic.Int32
	f.cfg.Factory = func(ctx context.Context, _ load.ReplicaSpec) (load.Application, error) {
		close(entered)
		<-ctx.Done()
		return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	done := make(chan error, 1)
	go func() { _, err := h.StartReplica(deadline(t), ref, StartExplicit); done <- err }()
	waitSignal(t, entered)
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	if err := <-done; err == nil {
		t.Fatal("canceled startup succeeded")
	}
	if closed.Load() != 1 {
		t.Fatal("successful factory ownership was discarded", closed.Load())
	}
}

type delayedRestore struct {
	stateMachine
	entered, canceled, proceed, finished chan struct{}
}

func (m *delayedRestore) Restore(ctx context.Context, _ []byte) error {
	close(m.entered)
	<-ctx.Done()
	close(m.canceled)
	<-m.proceed
	close(m.finished)
	return ctx.Err()
}

func TestStopTimeoutKeepsStartupOwnedUntilRestoreAndCloseJoin(t *testing.T) {
	functional(t)
	f := newFixture(t)
	m := &delayedRestore{entered: make(chan struct{}), canceled: make(chan struct{}), proceed: make(chan struct{}), finished: make(chan struct{})}
	var closeCalls atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: m, Close: func(context.Context) error {
			select {
			case <-m.finished:
			default:
				t.Error("Close overlapped Restore")
			}
			closeCalls.Add(1)
			return nil
		}}, nil
	}
	h := f.host(t)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(m.proceed) }) })
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	started := make(chan error, 1)
	go func() { _, err := h.StartReplica(deadline(t), ref, StartExplicit); started <- err }()
	waitSignal(t, m.entered)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := h.StopReplica(ctx, ref, StopExplicit); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	waitSignal(t, m.canceled)
	if closeCalls.Load() != 0 {
		t.Fatal("timed-out stop lost the application")
	}
	if _, err := h.StartReplica(deadline(t), ref, StartExplicit); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	once.Do(func() { close(m.proceed) })
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	if err := <-started; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if closeCalls.Load() != 1 {
		t.Fatal(closeCalls.Load())
	}
}

func TestEstablishedMissingStoreParksBeforeFactoryAndPreservesEvidence(t *testing.T) {
	functional(t)
	f := newFixture(t)
	f.setScope("work", 1, false, true)
	var calls atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		calls.Add(1)
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	var first ParkedReplica
	for i := 0; i < 2; i++ {
		_, err := h.StartReplica(deadline(t), ref, StartExplicit)
		var parked *ParkedError
		if !errors.As(err, &parked) || parked.Reason != ParkStoreMissing || !errors.Is(err, fault.ErrReplicaNotReady) {
			t.Fatal(err)
		}
		if i == 0 {
			first = parked.ParkedReplica
		} else if parked.ParkedReplica != first {
			t.Fatal("retry discarded parking evidence")
		}
		copy := h.Parked()
		delete(copy, "work")
		if len(h.Parked()) != 1 {
			t.Fatal("observation exposed host map")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("factory ran before missing-store refusal")
	}
	probe, err := f.store.Probe(deadline(t), "work")
	must(t, err)
	if probe.Exists {
		t.Fatal("missing established replica was silently re-created")
	}
}

type loopbackTransport struct {
	discardTransport
	host *Host
}

func (t *loopbackTransport) ApplyAction(ctx context.Context, request load.ActionRequest) (load.ActionResult, error) {
	if request.To.Node != t.host.cfg.Identity() {
		return load.ActionResult{}, fault.ErrUnavailable
	}
	return t.host.ApplyAction(ctx, request)
}

func (discardTransport) InstallFence(context.Context, load.FenceRequest) (load.FenceResult, error) {
	return load.FenceResult{}, fault.ErrUnavailable
}
func (t *loopbackTransport) InstallFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if request.To != t.host.cfg.Identity() {
		return load.FenceResult{}, fault.ErrUnavailable
	}
	return t.host.InstallFence(ctx, request)
}
