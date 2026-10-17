//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestLoadScopeCreationRequiresDurableReceipt(t *testing.T) {
	t.Parallel()
	s := loadMembershipScopeFixture(t, topology.StepAddLearner)
	id := storage.Identity{NodeName: "n2", ClusterID: s.Target.ClusterID, GroupID: "shard0", ReplicaID: 2}
	m := restoreMachineFixture(t, s)
	check := func(started bool) {
		t.Helper()
		if err := m.WithLoadScope(id, 2, func(scope LoadScope) error {
			if scope.Started != started || scope.Established || scope.Founding {
				t.Fatal("prepared learner was mistaken for an established replica", scope)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	check(false)
	op := m.scoped.Operations[0]
	issued := applyMachine(t, m, BeginEffectProposal{ProposalID: "prepare", Action: scopedPlanAction(&op, op.Plan.Steps[0])})
	if !issued.Accepted || issued.Grant == nil {
		t.Fatal(issued)
	}
	check(false) // Permission to create is not proof of creation.
	for _, receipt := range fixtureEffectReceipts(*issued.Grant) {
		accepted := applyMachine(t, m, RecordEffectProposal{Receipt: receipt, At: op.StartedAt, ProposalID: "prepared"})
		if !accepted.Accepted {
			t.Fatal(accepted)
		}
	}
	check(true) // A prepared learner can still have an empty log.
}

func TestLoadScopeRejectsOldOwnerAndRetiredGroupBeforeCallback(t *testing.T) {
	t.Parallel()
	s, p := deletionApprovalFixture(t)
	m := restoreMachineFixture(t, s)
	id := storage.Identity{NodeName: "n1", ClusterID: s.Target.ClusterID, GroupID: "shard0", ReplicaID: 1}
	called := false
	if err := m.WithLoadScope(id, 9, func(LoadScope) error { called = true; return nil }); !errors.Is(err, fault.ErrIdentityMismatch) || called {
		t.Fatal(called, err)
	}
	result := applyMachine(t, m, p)
	if !result.Accepted {
		t.Fatal(result)
	}
	if err := m.WithLoadScope(id, 1, func(LoadScope) error { called = true; return nil }); !errors.Is(err, fault.ErrGroupRetired) || called {
		t.Fatal(called, err)
	}
}
