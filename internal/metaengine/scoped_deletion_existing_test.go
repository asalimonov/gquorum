//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func existingDebtDeletionFixture(t *testing.T) (scopedState, BeginOperationProposal) {
	t.Helper()
	s := scopedReadmissionCleanupFixture()
	s.Cleanup[0] = pendingReplacementCleanup().Cleanup[0]
	s.AuthorityRecords = slices.DeleteFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool {
		return r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].RetireAction
	})
	for i := range s.AuthorityRecords {
		r := &s.AuthorityRecords[i]
		if r.Grant.Purpose == topology.AuthorityReadmit || r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction {
			r.Effects = nil
		}
		if r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction {
			r.Class = "cleanup"
		}
	}
	origin := &s.Operations[2]
	origin.Plan.Blocks[0].Steps, origin.Plan.Blocks[0].Cleanup = []int{1}, []int{2, 3}
	origin.Status.Blocks[0].CurrentStep, origin.Status.Blocks[0].TotalSteps = 1, 1
	// A replacement member never reuses an old member's load identity.
	for _, doc := range []*topology.Document{&s.Target, &s.Latest, &s.History[2], &s.History[3]} {
		doc.Group("a").Replicas[0].ReplicaID, doc.Group("a").NextReplicaID = 9, 10
		doc.Group("live").Replicas[0].ReplicaID, doc.Group("live").NextReplicaID = 2, 3
	}
	s.Operations[0].Plan.Steps[4].ReplicaID = 9
	for i := range s.Allocations {
		if s.Allocations[i].GroupID == "a" {
			s.Allocations[i].NextReplicaID = 10
		}
		if s.Allocations[i].GroupID == "live" {
			s.Allocations[i].NextReplicaID = 3
		}
	}
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 7, 5
	s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "a" })
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	return s, BeginOperationProposal{Version: 7, At: origin.UpdatedAt.Add(time.Second),
		Plan:   topology.Plan{Version: 7, Blocks: []topology.PlanBlock{{GroupID: "a", SourceVersion: 1}}},
		Scopes: []ScopeExpectation{{GroupID: "a", SourceVersion: 1}}}
}

func TestScopedExistingDebtDeletionRequiresUsableWholeBatch(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing", "pending", "fenced", "partial", "complete"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, proposal := existingDebtDeletionFixture(t)
			for i := range s.AuthorityRecords {
				r := &s.AuthorityRecords[i]
				if r.Grant.Purpose != topology.AuthorityReadmit {
					continue
				}
				if mode == "fenced" {
					r.Closed = true
				}
				if mode == "complete" || mode == "partial" && r.Grant.Key.GroupID == "a" {
					r.Effects = fixtureEffectReceipts(r.Grant)
				}
			}
			if mode == "missing" {
				s.AuthorityRecords = slices.DeleteFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool {
					return r.Grant.Purpose == topology.AuthorityReadmit
				})
				s.AuthorityFloors = slices.DeleteFunc(s.AuthorityFloors, func(f resourceFloor) bool {
					return f.Key.Revision == 20
				})
			}
			before, _ := json.Marshal(s)
			next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
			if mode == "missing" || mode == "fenced" {
				want := fault.ErrUnavailable
				if mode == "fenced" {
					want = fault.ErrAuthorityFenced
				}
				if !errors.Is(err, want) || next != nil {
					t.Fatal("existing debt bypassed omitted-stop coverage", mode, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				index, err := prepareScopedGraph(next, next.Rules)
				if err != nil {
					t.Fatal("prepared deletion cannot restore", err)
				}
				d := index.cleanup[s.Cleanup[0].Identity]
				if (d.Stopped != nil) != (mode == "complete") || index.groups["a"].Deleting != (mode != "complete") ||
					d.OperationID != s.Cleanup[0].OperationID || !reflect.DeepEqual(d.RetireAction, s.Cleanup[0].RetireAction) {
					t.Fatal("deletion lost original attribution or fabricated stop", mode, d)
				}
				if mode != "complete" {
					// The ownerless failed tip must retain the same omitted-stop dependency.
					next.Operations[len(next.Operations)-1].Status.State = topology.OperationFailed
					next.Operations[len(next.Operations)-1].Status.Blocks[0].State = topology.OperationFailed
					index.groups["a"].Owner = ""
					if _, err := prepareScopedGraph(next, next.Rules); err != nil {
						t.Fatal("failed deletion lost its pending proof", err)
					}
					next.AuthorityRecords = slices.Clone(next.AuthorityRecords)
					for i := range next.AuthorityRecords {
						if next.AuthorityRecords[i].Grant.Purpose == topology.AuthorityReadmit {
							next.AuthorityRecords[i].Closed = true
						}
					}
					if _, err := prepareScopedGraph(next, next.Rules); !errors.Is(err, fault.ErrUnreadable) {
						t.Fatal("existing detached debt hid unusable deletion proof", err)
					}
				}
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("candidate/refusal changed original evidence")
			}
		})
	}
}

func TestScopedExistingDebtDeletionProtectsReadmissionFromCancellation(t *testing.T) {
	t.Parallel()
	s, proposal := existingDebtDeletionFixture(t)
	next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	collector := s.planningCollector()
	if _, err := collector.collect(s.planningDeclarations(), s.Latest); err != nil {
		t.Fatal(err)
	}
	collector.markConvergedOwners(s.planningDeclarations().Groups)
	for _, member := range []uint64{0, 2, 3} {
		candidate := *next
		candidate.Target = next.Target.Clone()
		candidate.Target.MetaGroup().Replicas[0].ReplicaID = member
		prep := deletionPreparation{source: &s, next: &candidate, collector: collector}
		err := prep.protectReadmission()
		if member == 2 && err != nil || member != 2 && !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("detached original stop bypassed deletion's batch dependency", member, err)
		}
	}
}
