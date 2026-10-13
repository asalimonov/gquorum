//go:build !functional

package metaengine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestGrantedCleanupBindsRetainedProofAndProtectsAssignments(t *testing.T) {
	for _, mode := range []string{"closed", "unknown", "future", "foreign_future", "altered", "other_member", "assigned", "wrong_kind", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			m := NewMachine(testRules(), nil)
			m.target = baseDoc()
			m.target.Group("shard0").Replicas = m.target.Group("shard0").Replicas[1:]
			target := topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}
			action := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(2, 1), Version: 2, Attempt: 1, StepSeq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
			grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: m.target.ClusterID, GroupID: "shard0", Revision: 10}, Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID, Action: &action,
				Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "shard0", NodeName: "n1"}}, Targets: []topology.AuthorityTarget{target}}
			if mode == "wrong_kind" {
				grant.Action.Kind = topology.StepPrepareReplica
			}
			if err := grant.Validate(); err != nil {
				t.Fatal(err)
			}
			m.insertGrantLocked(grant)
			m.appliedIndex = grant.Key.Revision
			m.authority.records[grant.Key].Closed = true
			var want error = fault.ErrOperatorAuthorization
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "closed":
				want = nil
			case "unknown":
				delete(m.authority.records, grant.Key)
			case "future":
				grant.Key.Revision++
				want = fault.ErrUnavailable
			case "foreign_future":
				grant.Key.ClusterID = "foreign"
				grant.Key.Revision++
			case "altered":
				grant.Action.Attempt++
			case "other_member":
				target.MemberID++
			case "assigned":
				m.target = baseDoc()
				want = fault.ErrIdentityMismatch
			case "canceled":
				cancel()
				want = context.Canceled
			}
			called := false
			err := m.WithGrantedLoadCleanup(ctx, grant, target, func() error {
				called = true
				if m.mu.TryLock() {
					m.mu.Unlock()
					t.Fatal("assignment could change during ledger deletion")
				}
				return nil
			})
			if !errors.Is(err, want) || called != (want == nil) {
				t.Fatal(called, err)
			}
			if mode == "future" {
				// Once that exact committed grant applies, the same retry may
				// mutate the ledger under the assignment lock.
				m.insertGrantLocked(grant)
				m.appliedIndex = grant.Key.Revision
				err = m.WithGrantedLoadCleanup(ctx, grant, target, func() error {
					called = true
					return nil
				})
				if err != nil || !called {
					t.Fatal("cleanup did not resume after exact grant catch-up", called, err)
				}
			}
		})
	}
}

func TestAuthorityProofCleanupRequiresExactUnassignedTarget(t *testing.T) {
	t.Parallel()
	m := NewMachine(testRules(), nil)
	m.latest, m.target = baseDoc(), baseDoc()
	target := topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}
	a := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(2, 1), Version: 2, Attempt: 1, StepSeq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: m.target.ClusterID, GroupID: "shard0", Revision: 10}, Purpose: topology.AuthorityEffect, OperatorID: a.OperatorID, Action: &a,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "shard0", NodeName: "n1"}}, Targets: []topology.AuthorityTarget{target}}
	m.insertGrantLocked(grant)
	m.authority.records[grant.Key].Closed = true
	query := func() AuthorityProof {
		t.Helper()
		data, _ := json.Marshal(QueryRequest{Kind: "authority_proof", AuthorityKey: &grant.Key})
		result, err := m.Query(t.Context(), data)
		if err != nil {
			t.Fatal(err)
		}
		var proof AuthorityProof
		if err = json.Unmarshal(result.Data, &proof); err != nil {
			t.Fatal(err)
		}
		return proof
	}
	if got := query(); got.Grant == nil || len(got.CleanupTargets) != 0 {
		t.Fatal("assigned target authorized cleanup", got)
	}
	m.target.Group("shard0").Replicas = m.target.Group("shard0").Replicas[1:]
	got := query()
	if len(got.CleanupTargets) != 1 || got.CleanupTargets[0] != target {
		t.Fatal("closed retained grant lost exact cleanup proof", got)
	}
	got.CleanupTargets[0].MemberID++
	if query().CleanupTargets[0] != target {
		t.Fatal("proof aliases stored authority")
	}
	m.authority.records[grant.Key].Grant.Action.Kind = topology.StepPrepareReplica
	if got := query(); len(got.CleanupTargets) != 0 {
		t.Fatal("preparation authorized cleanup", got)
	}
}
