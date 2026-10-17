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
	t.Parallel()
	for _, mode := range []string{"closed", "unknown", "future", "foreign_future", "altered", "other_member", "assigned", "wrong_kind", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			m := restoreMachineFixture(t, scopedCodecFixture())
			debt := m.scoped.Cleanup[0]
			record := m.scoped.indexes.grants[debt.Retired.Key]
			grant := record.Grant.Clone()
			target := grant.Targets[0]
			record.Closed = true
			current, err := m.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var want error = fault.ErrOperationAuthorization
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "closed":
				want = nil
			case "unknown":
				delete(m.scoped.indexes.grants, grant.Key)
			case "future":
				delete(m.scoped.indexes.grants, grant.Key)
				m.scoped.AppliedIndex = grant.Key.Revision - 1
				want = fault.ErrUnavailable
			case "foreign_future":
				grant.Key.ClusterID = "foreign"
				grant.Key.Revision = m.scoped.AppliedIndex + 1
			case "altered":
				grant.Action.Attempt++
			case "other_member":
				target.MemberID++
			case "assigned":
				m.scoped.Target.Group(grant.Key.GroupID).Replicas = append(m.scoped.Target.Group(grant.Key.GroupID).Replicas,
					topology.Replica{NodeName: target.NodeName, ReplicaID: target.ReplicaID, Role: topology.ReplicaVoter})
				want = fault.ErrIdentityMismatch
			case "wrong_kind":
				grant.Action.Kind = topology.StepPrepareReplica
				record.Grant = grant.Clone()
			case "canceled":
				cancel()
				want = context.Canceled
			}
			called := false
			err = m.WithGrantedLoadCleanup(ctx, grant, target, func() error {
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
				if err := m.Restore(t.Context(), current); err != nil {
					t.Fatal(err)
				}
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
	m := restoreMachineFixture(t, scopedCodecFixture())
	debt := m.scoped.Cleanup[0]
	record := m.scoped.indexes.grants[debt.Retired.Key]
	grant := record.Grant.Clone()
	target := grant.Targets[0]
	record.Closed = true
	group := m.scoped.Target.Group(grant.Key.GroupID)
	group.Replicas = append(group.Replicas, topology.Replica{NodeName: target.NodeName, ReplicaID: target.ReplicaID})
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
	group.Replicas = group.Replicas[:len(group.Replicas)-1]
	got := query()
	if len(got.CleanupTargets) != 1 || got.CleanupTargets[0] != target {
		t.Fatal("closed retained grant lost exact cleanup proof", got)
	}
	got.CleanupTargets[0].MemberID++
	if query().CleanupTargets[0] != target {
		t.Fatal("proof aliases stored authority")
	}
	record.Grant.Action.Kind = topology.StepPrepareReplica
	if got := query(); len(got.CleanupTargets) != 0 {
		t.Fatal("preparation authorized cleanup", got)
	}
}
