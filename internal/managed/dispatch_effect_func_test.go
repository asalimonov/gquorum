//go:build functional

package managed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestExactDispatchPreservesFullGrantAndDoesNotFanOut(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, grant := fixtureAuthority(t, f)
	configureGrantDispatch(f)
	grant.Resources = append(grant.Resources, topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "b"})
	grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2})
	must(t, grant.Validate())
	var actions atomic.Int32
	f.cfg.Transport = grantedProtocolTransport{
		fence: func(_ context.Context, req load.FenceRequest) (load.FenceResult, error) {
			if !req.Grant.Equal(grant) || req.To.NodeName != "a" || req.ReplicaID != 0 {
				t.Fatal("trimmed or misdirected fence", req)
			}
			return hostFenceReply(req), nil
		},
		action: func(_ context.Context, req load.ActionRequest) (load.ActionResult, error) {
			actions.Add(1)
			if !req.Grant.Equal(grant) || req.To.Node.NodeName != "a" || req.LocalTarget != f.localID(1) {
				t.Fatal("trimmed or misdirected action", req)
			}
			return hostActionReply(req), nil
		},
	}
	h := f.host(t)
	receipt, err := h.Effects().DispatchEffect(deadline(t), grant, grant.Resources[1], grant.Targets[0])
	must(t, err)
	must(t, receipt.Validate(grant))
	if actions.Load() != 1 {
		t.Fatal("unexpected fanout", actions.Load())
	}
	for _, selection := range []struct {
		resource topology.AuthorityResource
		target   topology.AuthorityTarget
	}{
		{grant.Resources[1], grant.Targets[1]},
		{grant.Resources[0], topology.AuthorityTarget{}},
		{topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "foreign"}, grant.Targets[0]},
		{grant.Resources[1], topology.AuthorityTarget{NodeName: "a", MemberID: 2, ReplicaID: 1}},
	} {
		if _, err := h.DispatchEffect(deadline(t), grant, selection.resource, selection.target); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal(err)
		}
	}
	if actions.Load() != 1 {
		t.Fatal("foreign selection dispatched")
	}
}

func TestExactDispatchRevalidatesGrantAfterFenceWait(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, grant := fixtureAuthority(t, f)
	configureGrantDispatch(f)
	var fenced atomic.Bool
	f.cfg.ValidateGrant = func(_ context.Context, g topology.AuthorityGrant) error {
		if fenced.Load() {
			return fault.ErrAuthorityFenced
		}
		if !g.Equal(grant) {
			return fault.ErrOperationAuthorization
		}
		return nil
	}
	f.cfg.RecordFence = func(context.Context, topology.FenceEvidence) error { fenced.Store(true); return nil }
	f.cfg.Transport = grantedProtocolTransport{
		fence: func(_ context.Context, req load.FenceRequest) (load.FenceResult, error) {
			return hostFenceReply(req), nil
		},
		action: func(context.Context, load.ActionRequest) (load.ActionResult, error) {
			t.Fatal("stale effect submitted after fence wait")
			return load.ActionResult{}, nil
		},
	}
	h := f.host(t)
	if _, err := h.DispatchEffect(deadline(t), grant, grant.Resources[1], grant.Targets[0]); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
}

func TestExactDispatchNativeUsesOnlyExplicitObservedTarget(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, grant := fixtureAuthority(t, f)
	configureGrantDispatch(f)
	grant.Resources = append(grant.Resources, topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "b"})
	grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2})
	var actions atomic.Int32
	f.cfg.Transport = grantedProtocolTransport{
		fence: func(_ context.Context, req load.FenceRequest) (load.FenceResult, error) {
			if req.To.NodeName != "b" || req.ReplicaID != 2 || req.Resource != grant.Resources[0] {
				t.Fatal(req)
			}
			return load.FenceResult{From: req.To, Evidence: topology.FenceEvidence{Key: grant.Key, Resource: req.Resource, Receiver: req.To, ReplicaID: 2, AppliedIndex: 5, AppliedTerm: 1}}, nil
		},
		action: func(_ context.Context, req load.ActionRequest) (load.ActionResult, error) {
			actions.Add(1)
			if req.To.Node.NodeName != "b" || req.To.Replica.ReplicaID != 2 {
				t.Fatal(req)
			}
			return load.ActionResult{}, fault.ErrNotLeader
		},
	}
	h := f.host(t)
	if _, err := h.DispatchEffect(deadline(t), grant, grant.Resources[0], grant.Targets[1]); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	if actions.Load() != 1 {
		t.Fatal("native request rerouted", actions.Load())
	}
}
