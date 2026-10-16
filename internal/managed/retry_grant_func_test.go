//go:build functional

package managed

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestRetryGrantReusesReadyAndJoinedIdentityWithFreshProof(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, founding := fixtureAuthority(t, f)
	var factories, closes atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closes.Add(1); return nil }}, nil
	}
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), founding)
	must(t, err)
	handle, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	grant := founding.Clone()
	grant.Resources = grant.Resources[1:]
	var previous topology.EffectReceipt
	for _, kind := range []topology.StepKind{topology.StepPrepareReplica, topology.StepPrepareReplica, topology.StepStopReplica, topology.StepStopReplica} {
		grant.Key.Revision++
		action := f.approve(kind, f.localID(1))
		action.Attempt = uint32(grant.Key.Revision)
		action.OperatorID = topology.MakeOperatorID(action.Version, action.Attempt)
		grant.Action, grant.OperatorID = &action, action.OperatorID
		req := f.request(action, f.localID(1))
		req.Grant = grant.Clone()
		// The exact state is already ready/joined, but the new grant cannot use
		// it until the new floor has joined all prior owned work.
		if _, err := h.ApplyAction(deadline(t), req); !errors.Is(err, fault.ErrAuthorityFenced) {
			t.Fatal("state bypassed floor", err)
		}
		_, err := h.InstallHostFence(deadline(t), grant)
		must(t, err)
		var result load.ActionResult
		await(deadline(t), t, func() bool {
			result, err = h.ApplyAction(deadline(t), req)
			if errors.Is(err, fault.ErrAuthorityPending) {
				return false
			}
			must(t, err)
			return true
		})
		if len(result.Receipts) != 1 {
			t.Fatal(result)
		}
		proof := result.Receipts[0]
		must(t, proof.Validate(grant))
		if proof.Key == previous.Key || proof.LocalSequence <= previous.LocalSequence {
			t.Fatal("relabelled old receipt", previous, proof)
		}
		cached, err := s.Receipt(t.Context(), grant, grant.Targets[0])
		must(t, err)
		if !cached.Found || !reflect.DeepEqual(cached.Receipt, proof) {
			t.Fatal("fresh proof not persisted", cached)
		}
		if kind == topology.StepPrepareReplica {
			current, ok := h.Lookup("work")
			if !ok || current != handle || factories.Load() != 1 || closes.Load() != 0 {
				t.Fatal("prepare replaced ready owner")
			}
		} else if factories.Load() != 1 || closes.Load() != 1 {
			t.Fatal("stop repeated release", factories.Load(), closes.Load())
		}
		previous = proof
	}
}

func TestRetryGrantParksMissingRetainedReplicaBeforeFactory(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, grant := fixtureAuthority(t, f)
	f.setScope("work", 1, false, true)
	var factories atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	h := f.host(t)
	for attempt := uint32(1); attempt <= 2; attempt++ {
		a := f.approve(topology.StepPrepareReplica, f.localID(1))
		a.Attempt, a.OperatorID = attempt, topology.MakeOperatorID(1, attempt)
		grant.Key.Revision++
		grant.Action, grant.OperatorID = &a, a.OperatorID
		grant.Resources = []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "work", NodeName: f.node.NodeName}}
		_, err := h.InstallHostFence(deadline(t), grant)
		must(t, err)
		req := f.request(a, f.localID(1))
		req.Grant = grant.Clone()
		_, err = h.ApplyAction(deadline(t), req)
		var parked *ParkedError
		if !errors.As(err, &parked) || parked.Reason != ParkStoreMissing {
			t.Fatal(err)
		}
		proof, err := s.Receipt(t.Context(), grant, grant.Targets[0])
		must(t, err)
		if proof.Found || factories.Load() != 0 {
			t.Fatal("missing state treated as fresh/complete", proof, factories.Load())
		}
	}
	probe, err := f.store.Probe(t.Context(), "work")
	must(t, err)
	if probe.Exists {
		t.Fatal("recreated lost retained state")
	}
}
