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

func pendingReplacementCleanup() scopedState {
	s := scopedCodecFixture()
	d := &s.Cleanup[0]
	d.Stopped, d.StoppedAt, d.Retired, d.RetiredAt, d.Collection, d.ReadmissionKey = nil, nil, nil, nil, nil, nil
	s.AuthorityRecords = slices.DeleteFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool {
		return r.Grant.Action != nil && *r.Grant.Action == *d.RetireAction
	})
	for i := range s.AuthorityRecords {
		r := &s.AuthorityRecords[i]
		if r.Grant.Action != nil && *r.Grant.Action == *d.StopAction {
			r.Effects = nil
			for j := range s.AuthorityFloors {
				if s.AuthorityFloors[j].Resource == r.Grant.Resources[0] {
					s.AuthorityFloors[j].Key = r.Grant.Key
				}
			}
		}
	}
	return s
}

func cleanupApprovalFixture(t *testing.T) (scopedState, BeginOperationProposal) {
	t.Helper()
	s := pendingReplacementCleanup()
	s.Target.MetaGroup().NextReplicaID = 4
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 7, 5
	s.Latest.MetaGroup().NextReplicaID = 4
	s.Latest.Group("a").Replicas[0].ReplicaID, s.Latest.Group("a").NextReplicaID = 9, 10
	for i := range s.Allocations {
		if s.Allocations[i].GroupID == "a" {
			s.Allocations[i].NextReplicaID = 10
		}
	}
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	p := BeginOperationProposal{Version: 7, At: s.Cleanup[0].UpdatedAt.Add(time.Second),
		Actor: "admin", Origin: topology.OriginAPI, Scopes: []ScopeExpectation{{GroupID: "a", SourceVersion: 1}},
		Plan: topology.Plan{Version: 7, Steps: []topology.Step{{Seq: 1, Kind: topology.StepStopReplica,
			GroupID: "a", NodeName: "n", ReplicaID: 7}}, Blocks: []topology.PlanBlock{{GroupID: "a", SourceVersion: 1, Steps: []int{1}}}}}
	return s, p
}

func TestScopedApprovalRebindsExactRequiredStopAcrossVersions(t *testing.T) {
	t.Parallel()
	s, p := cleanupApprovalFixture(t)
	before, _ := json.Marshal(s)
	next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	d := next.Cleanup[0]
	if d.StopAction.OperatorID != "exec-v7-a1" || d.StopAction.StepSeq != 1 ||
		s.Cleanup[0].StopAction.StepSeq != 2 || d.OperationID != s.Cleanup[0].OperationID ||
		!reflect.DeepEqual(d.RetireAction, s.Cleanup[0].RetireAction) || !reflect.DeepEqual(d.Removed, s.Cleanup[0].Removed) {
		t.Fatal("rebound stop lost distinct origin or new sequence", d)
	}
	next.History = append(slices.Clone(next.History), s.Target.Clone())
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("cross-version stop cannot restore", err)
	}
	// A late receipt for the old exact action is evidence, not the new milestone.
	next.AuthorityRecords = slices.Clone(next.AuthorityRecords)
	for i := range next.AuthorityRecords {
		r := &next.AuthorityRecords[i]
		if r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction {
			r.Effects = fixtureEffectReceipts(r.Grant)
		}
	}
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("late old receipt replaced successor stop", err)
	}
	// Failed ownerless tip retains the new binding and both plans.
	op := &next.Operations[len(next.Operations)-1]
	op.Status.State, op.Status.Blocks[0].State = topology.OperationFailed, topology.OperationFailed
	for i := range next.Groups {
		if next.Groups[i].GroupID == "a" {
			next.Groups[i].Owner = ""
		}
	}
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("failed successor lost required stop", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("preparation modified source rows")
	}
}

func TestScopedCleanupHandoverRefusesOmittedOrForeignStops(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*BeginOperationProposal){
		"omitted": func(p *BeginOperationProposal) { p.Plan.Steps, p.Plan.Blocks[0].Steps = nil, nil },
		"host":    func(p *BeginOperationProposal) { p.Plan.Steps[0].NodeName = "other" },
		"replica": func(p *BeginOperationProposal) { p.Plan.Steps[0].ReplicaID = 8 },
		"detached": func(p *BeginOperationProposal) {
			p.Plan.Blocks[0].Cleanup, p.Plan.Blocks[0].Steps = []int{1}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := cleanupApprovalFixture(t)
			mutate(&p)
			next := s.prepareApprovalOwners(s.Latest, p)
			before, _ := json.Marshal(s)
			if err := prepareCleanupHandovers(&s, next, s.planningCollector()); !errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("lost required-stop boundary", err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) || !reflect.DeepEqual(next.Cleanup, s.Cleanup) {
				t.Fatal("refusal partially rebound debt")
			}
		})
	}
}

func TestScopedCleanupHandoverPreservesConsumedAndReadmissionProof(t *testing.T) {
	t.Parallel()
	for _, consumed := range []bool{false, true} {
		s := scopedReadmissionCleanupFixture()
		old := pendingReplacementCleanup()
		s.Cleanup[0] = old.Cleanup[0]
		s.AuthorityRecords = slices.DeleteFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool {
			return r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].RetireAction
		})
		for i := range s.AuthorityRecords {
			r := &s.AuthorityRecords[i]
			if r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction ||
				!consumed && r.Grant.Purpose == topology.AuthorityReadmit {
				r.Effects = nil
			}
		}
		var err error
		s.indexes, err = prepareScopedGraph(&s, s.Rules)
		if err != nil {
			t.Fatal(err)
		}
		next := s
		next.Operations = append(slices.Clone(s.Operations), newScopedOperation(BeginOperationProposal{Version: 7,
			At: s.Cleanup[0].UpdatedAt.Add(time.Second), Plan: topology.Plan{Version: 7,
				Blocks: []topology.PlanBlock{{GroupID: "a", SourceVersion: 1}}}}))
		before, _ := json.Marshal(s)
		err = prepareCleanupHandovers(&s, &next, s.planningCollector())
		if !consumed {
			if !errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("approval alone supplied omitted retirement", err)
			}
		} else if err != nil || next.Cleanup[0].ReadmissionKey == nil || next.Cleanup[0].Stopped == nil ||
			!reflect.DeepEqual(next.Cleanup[0].StopAction, s.Cleanup[0].StopAction) ||
			!reflect.DeepEqual(next.Cleanup[1], s.Cleanup[1]) {
			t.Fatal("complete exact readmission/consumed debt lost original proof", err)
		}
		if consumed {
			// Check the changed debt against the original exact catalog; the
			// unrelated incomplete candidate is installed in the following task.
			proof := s
			proof.Cleanup = next.Cleanup
			if _, err := prepareScopedGraph(&proof, proof.Rules); err != nil {
				t.Fatal("readmission milestones cannot restore", err)
			}
		}
		after, _ := json.Marshal(s)
		if !bytes.Equal(before, after) {
			t.Fatal("readmission handover changed source")
		}
	}
}

func TestScopedCleanupHandoverPreservesDetachedStop(t *testing.T) {
	t.Parallel()
	s, p := cleanupApprovalFixture(t)
	origin := s.indexes.operations[s.Cleanup[0].OperationID]
	origin.Plan.Blocks[0].Steps, origin.Plan.Blocks[0].Cleanup = []int{1}, []int{2, 3}
	origin.Status.Blocks[0].CurrentStep, origin.Status.Blocks[0].TotalSteps = 1, 1
	for i := range s.AuthorityRecords {
		r := &s.AuthorityRecords[i]
		if r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction {
			r.Class = "cleanup"
		}
	}
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
	if err != nil || !reflect.DeepEqual(next.Cleanup[0], s.Cleanup[0]) {
		t.Fatal("a new required stop relabeled previously detached cleanup", err)
	}
	next.History = append(slices.Clone(next.History), s.Target.Clone())
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("detached original proof cannot restore beside successor work", err)
	}
}

func TestScopedCleanupHandoverRebindsFailedRetry(t *testing.T) {
	t.Parallel()
	s := pendingScopedDeletion()
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	next := retryScopedDeletion()
	// Exercise preparation rather than the fixture's manually rebound action.
	next.Cleanup[1] = s.Cleanup[1]
	if err := prepareCleanupHandovers(&s, &next, s.planningCollector()); err != nil {
		t.Fatal(err)
	}
	if next.Cleanup[1].StopAction.OperatorID != "exec-v2-a2" ||
		!reflect.DeepEqual(next.Cleanup[1].RetireAction, s.Cleanup[1].RetireAction) {
		t.Fatal("retry lost exact pending stop or rebound detached retirement")
	}
	if _, err := prepareScopedGraph(&next, next.Rules); err != nil {
		t.Fatal("prepared retry cannot restore", err)
	}
}
