//go:build functional

package managed

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestFencedHostRetainsGrantedCleanupUntilFullStop(t *testing.T) {
	t.Parallel()
	for _, kind := range []topology.StepKind{topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepDeleteGroupState} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			s, initial := fixtureAuthority(t, f)
			var factories, closes, retired atomic.Int32
			f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
				factories.Add(1)
				return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closes.Add(1); return nil }}, nil
			}
			f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
				retired.Add(1)
				return retirementProof(req), nil
			}
			h := f.host(t)
			_, err := h.InstallHostFence(deadline(t), initial)
			must(t, err)
			ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
			handle, err := h.StartReplica(deadline(t), ref, StartOperator)
			must(t, err)
			must(t, h.FenceParticipation(deadline(t)))
			if handle.Core().Running() || closes.Load() != 1 || len(h.IDs()) != 0 {
				t.Fatal("participation was not joined", closes.Load(), h.IDs())
			}
			if _, err := h.StartReplica(deadline(t), ref, StartOperator); !errors.Is(err, fault.ErrStopped) {
				t.Fatal("fenced host admitted a start", err)
			}

			grant := initial.Clone()
			grant.Key.Revision++
			a := f.approve(kind, f.localID(1))
			grant.Action, grant.Resources = &a, grant.Resources[1:]
			fence, err := h.InstallFence(deadline(t), load.FenceRequest{From: f.node, To: f.node, Grant: grant, Resource: grant.Resources[0]})
			must(t, err)
			must(t, fence.Evidence.Validate(grant))
			req := f.request(a, f.localID(1))
			req.Grant = grant
			result, err := h.ApplyAction(deadline(t), req)
			must(t, err)
			if len(result.Receipts) != 1 {
				t.Fatal(result)
			}
			must(t, result.Receipts[0].Validate(grant))
			again, err := h.ApplyAction(deadline(t), req)
			must(t, err)
			if !reflect.DeepEqual(result, again) || factories.Load() != 1 || closes.Load() != 1 {
				t.Fatal("receipt retry repeated participation", again, factories.Load(), closes.Load())
			}
			if kind != topology.StepStopReplica {
				probe, err := f.store.Probe(t.Context(), "work")
				must(t, err)
				if probe.Exists || retired.Load() != 1 {
					t.Fatal("cleanup did not complete exactly once", probe, retired.Load())
				}
			}

			// Even a newer installed creation grant cannot restart participation.
			prepare := initial.Clone()
			prepare.Key.Revision = grant.Key.Revision + 1
			_, err = h.InstallHostFence(deadline(t), prepare)
			must(t, err)
			create := f.request(*prepare.Action, f.localID(1))
			create.Grant = prepare
			if _, err := h.ApplyAction(deadline(t), create); !errors.Is(err, fault.ErrStopped) {
				t.Fatal("fenced host admitted granted creation", err)
			}
			if _, err := h.InstallFence(deadline(t), load.FenceRequest{From: f.node, To: f.node, Grant: prepare, Resource: prepare.Resources[0], ReplicaID: 1}); !errors.Is(err, fault.ErrStopped) {
				t.Fatal("fenced host admitted native authority work", err)
			}
			must(t, h.Stop(deadline(t)))
			if _, err := h.InstallHostFence(deadline(t), prepare); !errors.Is(err, fault.ErrStopped) {
				t.Fatal("fully stopped host accepted authority work", err)
			}
			if _, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrStopped) {
				t.Fatal("fully stopped host accepted cleanup", err)
			}
			cached, err := s.Receipt(t.Context(), grant, actionTarget(req))
			must(t, err)
			if !cached.Found || !reflect.DeepEqual(cached.Receipt, result.Receipts[0]) {
				t.Fatal("stop lost durable cleanup proof", cached.Receipt, cached.Found)
			}
		})
	}
}

func TestFullStopJoinsCleanupAfterParticipationFence(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, grant := fixtureAuthority(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		close(entered)
		<-release
		return retirementProof(req), nil
	}
	h := f.host(t)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	must(t, h.FenceParticipation(deadline(t)))
	a := f.approve(topology.StepCleanupReplicaState, f.localID(1))
	grant.Action, grant.Resources = &a, grant.Resources[1:]
	_, err := h.InstallHostFence(deadline(t), grant)
	must(t, err)
	req := f.request(a, f.localID(1))
	req.Grant = grant
	waited := make(chan error, 1)
	go func() { _, err := h.ApplyAction(deadline(t), req); waited <- err }()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("cleanup did not start after participation fence")
	}
	// Participation joining does not wait for an independent cleanup callback.
	must(t, h.FenceParticipation(deadline(t)))
	ctx, cancel := context.WithTimeout(deadline(t), 20*time.Millisecond)
	defer cancel()
	if err := h.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("stop released an outstanding cleanup callback", err)
	}
	if _, err := h.InstallHostFence(deadline(t), grant); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("shutdown reopened authority admission", err)
	}
	if found, err := s.Receipt(t.Context(), grant, actionTarget(req)); err != nil || found.Found {
		t.Fatal("unjoined callback has a success receipt", found.Found, err)
	}
	once.Do(func() { close(release) })
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatal("shutdown did not cancel the authority waiter", err)
	}
	must(t, h.Stop(deadline(t)))
}

func TestReadmissionJoinsRetainedAuthorityBeforeRetiringMetadata(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, grant := fixtureAuthority(t, f)
	readmission := configureReadmission(t, f, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var readmissionCalls atomic.Int32
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		if req.Reason == load.RetirementReadmission {
			readmissionCalls.Add(1)
		} else {
			close(entered)
			<-release
		}
		return retirementProof(req), nil
	}
	h := f.host(t)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	must(t, h.FenceParticipation(deadline(t)))
	a := f.approve(topology.StepCleanupReplicaState, f.localID(1))
	grant.Action, grant.Resources = &a, grant.Resources[1:]
	_, err := h.InstallHostFence(deadline(t), grant)
	must(t, err)
	req := f.request(a, f.localID(1))
	req.Grant = grant
	waited := make(chan error, 1)
	go func() { _, err := h.ApplyAction(deadline(t), req); waited <- err }()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("cleanup did not enter application callback")
	}
	ctx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	readmitting := make(chan error, 1)
	go func() { readmitting <- h.Readmit(ctx, readmission) }()
	await(deadline(t), t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.authorityClosed })
	floor, err := s.Current(t.Context(), "work")
	must(t, err)
	if !floor.Found || floor.Record.Complete || !floor.Record.Grant.Equal(readmission.Grants[1]) {
		t.Fatal("readmission did not persist pending admission before joining", floor.Record, floor.Found)
	}
	cancel()
	if err := <-readmitting; !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("readmission passed an unjoined authority callback", err)
	}
	if found, err := metastore.LoadIdentity(readmission.MetaDBPath); err != nil || !found.Found || readmissionCalls.Load() != 0 {
		t.Fatal("readmission retired metadata before authority work joined", found.Found, err, readmissionCalls.Load())
	}
	if _, err := h.InstallHostFence(deadline(t), grant); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("readmission reopened old authority admission", err)
	}
	once.Do(func() { close(release) })
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	must(t, h.Readmit(deadline(t), readmission))
	if readmissionCalls.Load() != 1 {
		t.Fatal("readmission retry did not finish retirement", readmissionCalls.Load())
	}
}
