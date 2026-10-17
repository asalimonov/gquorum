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
	if d.StopAction.OperationID != "exec-v7-a1" || d.StopAction.StepSeq != 1 ||
		s.Cleanup[0].StopAction.StepSeq != 2 || d.OperationID != s.Cleanup[0].OperationID ||
		!reflect.DeepEqual(d.RetireAction, s.Cleanup[0].RetireAction) || !reflect.DeepEqual(d.Removed, s.Cleanup[0].Removed) {
		t.Fatal("rebound stop lost distinct origin or new sequence", d)
	}
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("cross-version stop cannot restore", err)
	}
	// A late receipt supplies identity-level stop proof with its original action.
	next.AuthorityRecords = slices.Clone(next.AuthorityRecords)
	for i := range next.AuthorityRecords {
		r := &next.AuthorityRecords[i]
		if r.Grant.Action != nil && *r.Grant.Action == *s.Cleanup[0].StopAction {
			r.Effects = fixtureEffectReceipts(r.Grant)
			next.Cleanup[0].Stopped = cloneCleanupReceipt(&r.Effects[0])
			next.Cleanup[0].StoppedAt = copyCleanupValue(&next.Cleanup[0].UpdatedAt)
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

func TestScopedCleanupHandoverPromotesDetachedStop(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}
	if next.Cleanup[0].StopAction.OperationID != "exec-v7-a1" ||
		next.Cleanup[0].OperationID != s.Cleanup[0].OperationID ||
		!reflect.DeepEqual(next.Cleanup[0].RetireAction, s.Cleanup[0].RetireAction) {
		t.Fatal("required promotion lost its exact binding or original retirement")
	}
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("detached original proof cannot restore beside successor work", err)
	}
}

func TestScopedCleanupHandoverRelocationReuseAndDeletion(t *testing.T) {
	t.Parallel()
	s, proposal := cleanupApprovalFixture(t)
	documents := []*topology.Document{&s.Target, &s.Latest}
	for i := range s.History {
		documents = append(documents, &s.History[i])
	}
	for _, doc := range documents {
		node := doc.Nodes[0]
		node.Name = "away"
		doc.Nodes = append(doc.Nodes, node)
		doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas,
			topology.Replica{NodeName: "away", ReplicaID: 3, Role: topology.ReplicaLearner})
		doc.MetaGroup().NextReplicaID = 4
	}
	for i := range s.Groups {
		if s.Groups[i].GroupID == topology.MetaGroupID {
			s.Groups[i].Declaration = s.History[1].MetaGroup()
		}
	}
	s.Nodes = append(s.Nodes, nodeConvergence{NodeName: "away", Declaration: s.History[1].Node("away"),
		MemberID: 3, SourceVersion: 1, CompletedAt: s.Nodes[1].CompletedAt})
	s.Latest.Group("a").Replicas[0].NodeName = "away"
	proposal.Plan.Blocks[0].Steps, proposal.Plan.Blocks[0].Cleanup = nil, []int{1}
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	original := s.Cleanup[0]
	for _, version := range []uint64{7, 8, 9, 10} {
		if version == 8 {
			duplicate := s.Target.Clone()
			duplicate.Version, duplicate.ParentVersion = 8, 7
			duplicate.Group("a").Labels = map[string]string{"owner": "duplicate"}
			bad := proposal
			bad.Version, bad.Plan = 8, proposal.Plan.Clone()
			bad.Plan.Version, bad.At = 8, proposal.At.Add(time.Second)
			bad.Scopes = []ScopeExpectation{{GroupID: "a", SourceVersion: 1, Owner: "exec-v7-a1"}}
			if _, err := prepareScopedApproval(&s, duplicate, bad, explicitApproval); err == nil {
				t.Fatal("already detached debt accepted another detached stop")
			}
		}
		if version != 7 {
			s.Latest = s.Target.Clone()
			s.Latest.Version, s.Latest.ParentVersion = version, version-1
			group := "a"
			if version == 8 {
				group = "live"
				s.Latest.Group(group).Labels = map[string]string{"owner": "unrelated"}
			} else if version == 9 {
				s.Latest.Group("a").Replicas[0].NodeName = "n"
				s.Latest.Group("a").Replicas[0].ReplicaID, s.Latest.Group("a").NextReplicaID = 10, 11
				for i := range s.Allocations {
					if s.Allocations[i].GroupID == "a" {
						s.Allocations[i].NextReplicaID = 11
					}
				}
			} else {
				s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "a" })
			}
			row := s.indexes.groups[group]
			proposal.Version, proposal.At = version, proposal.At.Add(time.Second)
			proposal.Scopes = []ScopeExpectation{{GroupID: group, SourceVersion: row.SourceVersion, Owner: row.Owner}}
			proposal.Plan = topology.Plan{Version: version, Blocks: []topology.PlanBlock{{GroupID: group, SourceVersion: row.SourceVersion}}}
			if version != 8 {
				proposal.Plan.Steps = []topology.Step{{Seq: 1, GroupID: group, Kind: topology.StepStopReplica, NodeName: "n", ReplicaID: 7}}
				proposal.Plan.Blocks[0].Steps = []int{1}
			}
		}
		next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
		if err != nil {
			t.Fatal(version, err)
		}
		next.indexes, err = prepareScopedGraph(next, next.Rules)
		if err != nil {
			t.Fatal("handover cannot restore", version, err)
		}
		debt := next.indexes.cleanup[original.Identity]
		bound := version
		if version == 8 {
			bound = 7
		}
		if debt.StopAction.Version != bound || debt.OperationID != original.OperationID ||
			!reflect.DeepEqual(debt.RetireAction, original.RetireAction) || !reflect.DeepEqual(debt.Removed, original.Removed) {
			t.Fatal("handover changed original attribution or lost the exact binding", version, debt)
		}
		if requiredCleanupStop(next.indexes.operations[topology.OperationID(debt.StopAction.OperationID)], *debt.StopAction) != (version >= 9) {
			t.Fatal("stop stayed required after relocation or detached during reuse/deletion", version)
		}
		if version == 7 {
			assertReboundCleanupAuthorization(t, next, *original.StopAction)
		}
		s = *next
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
	if next.Cleanup[1].StopAction.OperationID != "exec-v2-a2" ||
		!reflect.DeepEqual(next.Cleanup[1].RetireAction, s.Cleanup[1].RetireAction) {
		t.Fatal("retry lost exact pending stop or rebound detached retirement")
	}
	if _, err := prepareScopedGraph(&next, next.Rules); err != nil {
		t.Fatal("prepared retry cannot restore", err)
	}
}
