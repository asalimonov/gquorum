//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func planDetachedApproval(t *testing.T, source *scopedState, proposal BeginOperationProposal) BeginOperationProposal {
	t.Helper()
	collector := source.planningCollector()
	input, err := collector.collect(source.planningDeclarations(), source.Latest)
	if err != nil {
		t.Fatal(err)
	}
	proposal.Plan, err = topology.BuildPlan(input, source.Latest, source.Rules)
	if err != nil {
		t.Fatal(err)
	}
	proposal.DeletionSemantics = DeletionSemanticsDetached
	return proposal
}

func TestDeletionApprovalCompletesWithExactPendingCleanup(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"ordinary", "pending readmission", "complete readmission", "inherited debt"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source, proposal := deletionApprovalFixture(t)
			group := "shard0"
			if name == "pending readmission" || name == "complete readmission" {
				source, proposal = readmissionDeletionFixture(t)
			}
			if name == "complete readmission" {
				for i := range source.AuthorityRecords {
					r := &source.AuthorityRecords[i]
					if r.Grant.Purpose == topology.AuthorityReadmit {
						r.Effects = fixtureEffectReceipts(r.Grant)
					}
				}
				prepareScopedFixture(t, &source)
			}
			if name == "inherited debt" {
				source, proposal = existingDebtDeletionFixture(t)
				group = "a"
			}
			proposal = planDetachedApproval(t, &source, proposal)
			readmissionFloors := make(map[topology.AuthorityResource]topology.AuthorityKey)
			for _, floor := range source.AuthorityFloors {
				if record := source.indexes.grants[floor.Key]; record.Grant.Purpose == topology.AuthorityReadmit {
					readmissionFloors[floor.Resource] = floor.Key
				}
			}
			var prior *cleanupRecord
			for _, debt := range source.Cleanup {
				if debt.Identity.GroupID == group {
					copy := debt.clone()
					prior = &copy
				}
			}
			applyScopedApprovalFixture(t, &source, proposal)
			for resource, key := range readmissionFloors {
				if got := source.authorityCatalog().floors[resource]; got != key {
					t.Fatal("logical deletion fenced its still-bound readmission", resource, key, got)
				}
			}
			id := topology.MakeOperationID(proposal.Version, 1)
			op := source.indexes.operations[id]
			row := source.indexes.groups[group]
			if op.Status.State != topology.OperationSucceeded || len(op.Plan.Blocks[0].Steps) != 0 ||
				op.Status.Blocks[0].TotalSteps != 0 || row.SourceVersion != proposal.Version ||
				row.Declaration != nil || row.Deleting || row.Owner != "" {
				t.Fatal("logical deletion waited for a physical effect", op, row)
			}
			var debts []cleanupRecord
			for _, debt := range source.Cleanup {
				if debt.Identity.GroupID == group {
					debts = append(debts, debt)
				}
			}
			if len(debts) != 1 {
				t.Fatal("deletion lost its exact obligation", debts)
			}
			debt := debts[0]
			if name != "complete readmission" && (debt.Stopped != nil || debt.Retired != nil || debt.Collection != nil) {
				t.Fatal("logical deletion manufactured physical proof", debt)
			}
			if prior != nil && (debt.OperationID != prior.OperationID || *debt.RetireAction != *prior.RetireAction) {
				t.Fatal("deletion replaced inherited retirement provenance")
			}
			if name == "complete readmission" && (debt.ReadmissionKey == nil || debt.Stopped == nil ||
				debt.Retired == nil || debt.StopAction != nil || debt.RetireAction != nil) {
				t.Fatal("complete batch lost its original retirement proof", debt)
			}
			if name == "ordinary" {
				work, err := queryReadyCleanup(&source, storage.Identity{}, 10)
				if err != nil || len(work) != 1 || work[0].Action.Kind != topology.StepStopReplica {
					t.Fatal("completed deletion lost exact deferred stop", work, err)
				}
			}
			before, _ := json.Marshal(source)
			replay, err := source.applyBeginOperation(source.AppliedIndex+1, proposal)
			after, _ := json.Marshal(source)
			if err != nil || replay.changed || replay.result.OperationID != id || !bytes.Equal(before, after) {
				t.Fatal("uncertain approval replay changed the first operation", replay, err)
			}
		})
	}
}

func TestDeletionApprovalDoesNotCompleteRequiredSibling(t *testing.T) {
	t.Parallel()
	s, p := deletionApprovalFixture(t)
	p = planDetachedApproval(t, &s, p)
	s.Latest.MetaGroup().Labels = map[string]string{"owner": "other"}
	for i := range p.Plan.Steps {
		p.Plan.Steps[i].Seq++
	}
	p.Plan.Steps = append([]topology.Step{{Seq: 1, GroupID: topology.MetaGroupID,
		Kind: topology.StepTransferLeader, NodeName: "n1", ReplicaID: 1}}, p.Plan.Steps...)
	for i := range p.Plan.Blocks[0].Cleanup {
		p.Plan.Blocks[0].Cleanup[i]++
	}
	p.Plan.Blocks = append([]topology.PlanBlock{{GroupID: topology.MetaGroupID,
		SourceVersion: s.indexes.groups[topology.MetaGroupID].SourceVersion, Steps: []int{1}}}, p.Plan.Blocks...)
	p.Scopes = s.capturePlanScopes(topology.AffectedGroups(s.Target, s.Latest))
	applyScopedApprovalFixture(t, &s, p)
	op := s.Operations[len(s.Operations)-1]
	if op.Status.State.Terminal() || op.Status.Blocks[0].State.Terminal() ||
		op.Status.Blocks[1].State != topology.OperationSucceeded || s.indexes.groups["shard0"].Declaration != nil {
		t.Fatal("deleted block fabricated independent sibling convergence", op)
	}
}

func TestDeletionApprovalRefusesOmittedHostWithoutPartialState(t *testing.T) {
	t.Parallel()
	s, p := deletionApprovalFixture(t)
	p = planDetachedApproval(t, &s, p)
	p.Plan.Steps[0].NodeName = "foreign"
	before, _ := json.Marshal(s)
	if _, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval); err == nil {
		t.Fatal("forged host debt accepted")
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) || slices.ContainsFunc(s.RetiredGroups, func(row retiredGroup) bool {
		return row.ID == "shard0"
	}) {
		t.Fatal("refusal partially published deletion")
	}
}
