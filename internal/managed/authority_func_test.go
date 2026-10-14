//go:build functional

package managed

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func fixtureAuthority(t *testing.T, f *fixture) (*authoritystore.Store, topology.AuthorityGrant) {
	t.Helper()
	if f.cfg.Authority == nil {
		s, err := authoritystore.Open(filepath.Join(t.TempDir(), "authority.bolt"), f.node, authoritystore.FreshEvidence)
		must(t, err)
		t.Cleanup(func() { must(t, s.Close()) })
		f.cfg.Authority = func() *authoritystore.Store { return s }
	}
	s := f.cfg.Authority()
	f.cfg.AuthorityFloors = func(ctx context.Context) ([]topology.AuthorityGrant, error) { return nil, ctx.Err() }
	f.cfg.ValidateGrant = func(ctx context.Context, g topology.AuthorityGrant) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return g.Validate()
	}
	f.cfg.ValidateCourier = func(ctx context.Context, grant topology.AuthorityGrant, courier topology.Identity) error {
		if err := f.cfg.ValidateNode(ctx, courier); err != nil {
			return err
		}
		return f.cfg.ValidateGrant(ctx, grant)
	}
	a := f.approve(topology.StepCreateFoundingGroup, f.localID(1))
	g := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: f.node.ClusterID, GroupID: "work", Revision: 10}, Purpose: topology.AuthorityEffect, OperatorID: a.OperatorID, Action: &a, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "work"}, {Kind: topology.AuthorityHost, GroupID: "work", NodeName: f.node.NodeName}}, Targets: []topology.AuthorityTarget{{NodeName: f.node.NodeName, MemberID: f.node.MemberID, ReplicaID: 1}}}
	return s, g
}

func TestHostFenceOwnsCanceledFactoryAndCloseUntilCompletion(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, initial := fixtureAuthority(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	closing, releaseClose := make(chan struct{}), make(chan struct{})
	var factoryOnce, closeOnce sync.Once
	var factories, closes atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		close(entered)
		<-release
		return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closes.Add(1); close(closing); <-releaseClose; return nil }}, nil
	}
	h := f.host(t)
	t.Cleanup(func() { factoryOnce.Do(func() { close(release) }); closeOnce.Do(func() { close(releaseClose) }) })
	proof, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	must(t, proof.Validate(initial))
	started := make(chan error, 1)
	go func() {
		_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
		started <- err
	}()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("factory did not start")
	}
	fence := initial.Clone()
	fence.Key.Revision = 20
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	waitCtx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	waited := make(chan error, 1)
	go func() { _, err := h.InstallHostFence(waitCtx, fence); waited <- err }()
	await(deadline(t), t, func() bool {
		r, err := s.Current(t.Context(), "work")
		must(t, err)
		return r.Found && r.Record.Grant.Key == fence.Key && !r.Record.Complete
	})
	cancel()
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("pending fence admitted another start", err)
	}
	newer := fence.Clone()
	newer.Key.Revision++
	completed := make(chan topology.FenceEvidence, 1)
	failed := make(chan error, 1)
	go func() { e, err := h.InstallHostFence(deadline(t), newer); completed <- e; failed <- err }()
	await(deadline(t), t, func() bool {
		r, err := s.Current(t.Context(), "work")
		must(t, err)
		return r.Record.Grant.Key == newer.Key
	})
	factoryOnce.Do(func() { close(release) })
	select {
	case <-closing:
	case <-deadline(t).Done():
		t.Fatal("late application was not released")
	}
	r, err := s.Current(t.Context(), "work")
	must(t, err)
	if r.Record.Complete {
		t.Fatal("fence completed while Close was owned")
	}
	closeOnce.Do(func() { close(releaseClose) })
	select {
	case err := <-failed:
		must(t, err)
	case <-deadline(t).Done():
		t.Fatal("owned fence did not complete")
	}
	must(t, (<-completed).Validate(newer))
	if err := <-started; err == nil {
		t.Fatal("canceled startup published")
	}
	if factories.Load() != 1 || closes.Load() != 1 {
		t.Fatal(factories.Load(), closes.Load())
	}
	probe, err := f.store.Probe(deadline(t), "work")
	must(t, err)
	if probe.Exists {
		t.Fatal("fenced factory opened storage", probe)
	}
	if _, ok := h.Lookup("work"); ok {
		t.Fatal("old handle was published")
	}
	if _, err := h.InstallHostFence(deadline(t), fence); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
}

func TestHostFenceReconcilesInterruptedIntentAndKeepsReadyReplica(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, initial := fixtureAuthority(t, f)
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	handle, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	uncertain := initial.Clone()
	uncertain.Key.Revision++
	a := f.approve(topology.StepStopReplica, f.localID(1))
	uncertain.Action, uncertain.Resources = &a, uncertain.Resources[1:]
	_, err = h.InstallHostFence(deadline(t), uncertain)
	must(t, err)
	_, err = s.BeginEffect(t.Context(), uncertain, uncertain.Targets[0])
	must(t, err)
	fence := uncertain.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	// Simulate a retained pending fence without a worker in this process.
	_, err = s.Fence(t.Context(), fence)
	must(t, err)
	proof, err := h.InstallHostFence(deadline(t), fence)
	must(t, err)
	must(t, proof.Validate(fence))
	if current, ok := h.Lookup("work"); !ok || current != handle {
		t.Fatal("administrative fencing stopped an already-ready data replica")
	}
	if records, err := s.Running(t.Context(), "work"); err != nil || len(records) != 0 {
		t.Fatal(records, err)
	}
	if found, err := s.Receipt(t.Context(), uncertain, uncertain.Targets[0]); err != nil || found.Found {
		t.Fatal("uncertain old effect became a success receipt", found.Found, err)
	}
}

func TestStartupImportsCurrentFloorAndRequiresQuorumOnlyForNewOwner(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, initial := fixtureAuthority(t, f)
	var unavailable atomic.Bool
	var reads, factories atomic.Int32
	var grantMu sync.Mutex
	current := initial.Clone()
	f.cfg.AuthorityFloors = func(ctx context.Context) ([]topology.AuthorityGrant, error) {
		reads.Add(1)
		if unavailable.Load() {
			return nil, fault.ErrUnavailable
		}
		grantMu.Lock()
		defer grantMu.Unlock()
		return []topology.AuthorityGrant{current.Clone()}, ctx.Err()
	}
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	unavailable.Store(true)
	if _, err := h.StartReplica(deadline(t), ref, StartExplicit); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("recovery ignored unavailable authoritative floors", err)
	}
	if factories.Load() != 0 {
		t.Fatal("factory ran before recovery barrier")
	}
	unavailable.Store(false)
	handle, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	if receipt, err := s.Receipt(t.Context(), initial, initial.Targets[0]); err != nil || !receipt.Found || receipt.Receipt.Validate(initial) != nil {
		t.Fatal("startup published before persisting its creation receipt", receipt.Receipt, receipt.Found, err)
	}
	unavailable.Store(true)
	before := reads.Load()
	if again, err := h.StartReplica(deadline(t), ref, StartExplicit); err != nil || again != handle || reads.Load() != before {
		t.Fatal("existing replica unexpectedly needs metadata quorum", again, err)
	}
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	if _, err := h.StartReplica(deadline(t), ref, StartExplicit); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("new owner bypassed recovery barrier", err)
	}
	grantMu.Lock()
	current.Key.Revision++
	current.Purpose, current.Action = topology.AuthorityFence, nil
	grantMu.Unlock()
	unavailable.Store(false)
	_, err = h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	if factories.Load() != 2 {
		t.Fatal("retained replica was not resumed", factories.Load())
	}
	floor, err := s.Current(t.Context(), "work")
	must(t, err)
	if !floor.Record.Complete || floor.Record.Grant.Key.Revision != initial.Key.Revision+1 {
		t.Fatal("recovery did not import new floor", floor.Record)
	}
}

func TestFenceOnlyGrantCannotCreateFreshReplica(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, initial := fixtureAuthority(t, f)
	initial.Key.Revision++
	initial.Purpose, initial.Action = topology.AuthorityFence, nil
	f.cfg.AuthorityFloors = func(context.Context) ([]topology.AuthorityGrant, error) {
		return []topology.AuthorityGrant{initial.Clone()}, nil
	}
	var factories atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{}, errors.New("must not run")
	}
	h := f.host(t)
	if _, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("fence-only grant authorized fresh creation", err)
	}
	if factories.Load() != 0 {
		t.Fatal("fresh factory called under fence-only grant")
	}
}
