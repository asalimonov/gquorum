//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Prepare the future terminal shape directly; new approval selection is enabled
// in the next checkpoint. The source and its issued native effect stay real.
func detachedDeletionFixture(t *testing.T) (scopedState, *scopedState, planningCollector) {
	t.Helper()
	source, approval := deletionApprovalFixture(t)
	scopedPendingTransfer(&source)
	prepareScopedFixture(t, &source)
	next, err := prepareScopedApproval(&source, source.Latest, approval, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	op := &next.Operations[len(next.Operations)-1]
	op.Plan.Blocks[0].Steps, op.Plan.Blocks[0].Cleanup = nil, []int{1, 2}
	op.Status.State, op.Status.Blocks[0].State = topology.OperationSucceeded, topology.OperationSucceeded
	op.Status.Blocks[0].TotalSteps = 0
	next.completeApprovalGroup("shard0", op)
	if err := next.revokeAuthorityLocked(source.AppliedIndex+1, source.supersededBlocks(op), op.Status.ID); err != nil {
		t.Fatal(err)
	}
	next.AppliedIndex = source.AppliedIndex + 1
	prepareScopedFixture(t, next)
	collector := source.planningCollector()
	if _, err := collector.collect(source.planningDeclarations(), next.Target); err != nil {
		t.Fatal(err)
	}
	collector.markConvergedOwners(source.planningDeclarations().Groups)
	return source, next, collector
}

func TestDetachedDeletionCoverageAndCleanupRemainIndependent(t *testing.T) {
	t.Parallel()
	source, next, collector := detachedDeletionFixture(t)
	op := next.Operations[len(next.Operations)-1]
	if err := (deletionPreparation{&source, next, collector}).validateDetachedCoverage(op.Plan.Blocks); err != nil {
		t.Fatal("unacknowledged but exact cleanup does not cover deletion", err)
	}
	native := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "shard0"}
	if next.completedDeletions()["shard0"] || len(next.indexes.obligations[native]) == 0 {
		t.Fatal("logical completion settled unproven native authority")
	}
	raw, err := json.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal("terminal cleanup state did not restore", err)
	}
	work, err := queryReadyCleanup(&restored, storage.Identity{}, 10)
	if err != nil || len(work) != 1 || work[0].Action.Kind != topology.StepStopReplica {
		t.Fatal("terminal operation lost detached stop", work, err)
	}
	debt := restored.Cleanup[0].clone()
	if _, err := restored.applyBeginCleanupEffect(restored.AppliedIndex+1,
		BeginCleanupEffectProposal{Identity: debt.Identity, Action: *debt.RetireAction}); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("retirement bypassed unproven stop", err)
	}
	issued := issueScopedCleanup(t, &restored, BeginCleanupEffectProposal{Identity: work[0].Identity, Action: work[0].Action})
	grant := *issued.result.Grant
	if len(grant.Targets) != 1 || grant.Targets[0].MemberID != debt.MemberID || restored.indexes.grants[grant.Key].Class != "cleanup" {
		t.Fatal("stop issuance changed exact cleanup authority", grant)
	}
	receipt := fixtureEffectReceipts(grant)[0]
	recordScopedEffectFixture(t, &restored, RecordEffectProposal{Receipt: receipt, At: op.UpdatedAt})
	if !restored.completedDeletions()["shard0"] || len(restored.indexes.obligations[native]) != 0 {
		t.Fatal("actual stop did not settle deleted native resource")
	}
	got := restored.indexes.cleanup[debt.Identity]
	if got.Stopped == nil || got.Retired != nil || got.Collection != nil || got.Removed != nil {
		t.Fatal("stop manufactured a later milestone", got)
	}
	work, err = queryReadyCleanup(&restored, storage.Identity{}, 10)
	if err != nil || len(work) != 1 || work[0].Action.Kind != topology.StepDeleteGroupState {
		t.Fatal("stopped owner lost its retirement", work, err)
	}
}

func TestDetachedDeletionCoverageRejectsMissingOrContradictoryOwners(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*scopedState){
		"missing":    func(s *scopedState) { s.Cleanup = nil },
		"member":     func(s *scopedState) { s.Cleanup[0].MemberID++ },
		"replica":    func(s *scopedState) { s.Cleanup[0].Identity.ReplicaID++ },
		"storage":    func(s *scopedState) { s.Cleanup[0].Storage = topology.StorageMemory },
		"stop":       func(s *scopedState) { s.Cleanup[0].StopAction = nil },
		"retirement": func(s *scopedState) { s.Cleanup[0].RetireAction = nil },
		"origin":     func(s *scopedState) { s.Cleanup[0].OperationID = "missing" },
		"action":     func(s *scopedState) { s.Cleanup[0].StopAction.ReplicaID++ },
		"timestamp":  func(s *scopedState) { s.Cleanup[0].StoppedAt = &s.Cleanup[0].CreatedAt },
		"retired ID": func(s *scopedState) { s.RetiredGroups = nil },
		"required stop": func(s *scopedState) {
			op := &s.Operations[len(s.Operations)-1]
			op.Plan.Blocks[0].Steps, op.Plan.Blocks[0].Cleanup = []int{1}, []int{2}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source, next, collector := detachedDeletionFixture(t)
			mutate(next)
			type wire scopedState
			before, err := json.Marshal((*wire)(next))
			if err != nil {
				t.Fatal(err)
			}
			sourceBefore, _ := json.Marshal(source)
			op := next.Operations[len(next.Operations)-1]
			if err := (deletionPreparation{&source, next, collector}).validateDetachedCoverage(op.Plan.Blocks); err == nil {
				t.Fatal("contradictory deletion coverage accepted")
			}
			after, _ := json.Marshal((*wire)(next))
			sourceAfter, _ := json.Marshal(source)
			if !bytes.Equal(before, after) || !bytes.Equal(sourceBefore, sourceAfter) {
				t.Fatal("coverage refusal mutated candidate or source")
			}
		})
	}
}

func TestDetachedDeletionRestoreDoesNotHideMissingDebt(t *testing.T) {
	t.Parallel()
	_, next, _ := detachedDeletionFixture(t)
	raw, _ := json.Marshal(next)
	var restored scopedState
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	next.Cleanup = nil
	type wire scopedState
	bad, _ := json.Marshal((*wire)(next))
	if err := json.Unmarshal(bad, &restored); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("terminal checkpoint concealed lost owner debt", err)
	}
	after, _ := json.Marshal(restored)
	if !bytes.Equal(raw, after) {
		t.Fatal("failed restore modified prior state")
	}
}

func TestDetachedDeletionCoverageRejectsPartialReadmissionWithoutDebt(t *testing.T) {
	t.Parallel()
	source, approval := readmissionDeletionFixture(t)
	for i := range source.AuthorityRecords {
		record := &source.AuthorityRecords[i]
		if record.Grant.Purpose == topology.AuthorityReadmit && record.Grant.Key.GroupID == "shard0" {
			record.Effects = fixtureEffectReceipts(record.Grant)
		}
	}
	prepareScopedFixture(t, &source)
	next, err := prepareScopedApproval(&source, source.Latest, approval, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	collector := source.planningCollector()
	if _, err := collector.collect(source.planningDeclarations(), next.Target); err != nil {
		t.Fatal(err)
	}
	collector.markConvergedOwners(source.planningDeclarations().Groups)
	if err := (deletionPreparation{&source, next, collector}).validateDetachedCoverage(approval.Plan.Blocks); err == nil {
		t.Fatal("partial readmission created independent deletion coverage")
	}
}

func TestDetachedDeletionCleanupDefersHistoricalMemberToReadmission(t *testing.T) {
	t.Parallel()
	_, s, _ := detachedDeletionFixture(t)
	debt := s.Cleanup[0].clone()
	// Exercise selection/authorization under a later approved member hint.
	// Whole topology/readmission graph validation remains independently required.
	for i := range s.Target.MetaGroup().Replicas {
		if s.Target.MetaGroup().Replicas[i].NodeName == debt.Identity.NodeName {
			s.Target.MetaGroup().Replicas[i].ReplicaID = debt.MemberID + 1
		}
	}
	before, _ := json.Marshal(s)
	work, err := queryReadyCleanup(s, storage.Identity{}, 10)
	if err != nil || len(work) != 0 {
		t.Fatal("old member stop dispatched toward its replacement", work, err)
	}
	if _, err := s.applyBeginCleanupEffect(s.AppliedIndex+1,
		BeginCleanupEffectProposal{Identity: debt.Identity, Action: *debt.StopAction}); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("ordinary cleanup stole pending readmission authority", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("deferred cleanup mutated evidence")
	}
}
