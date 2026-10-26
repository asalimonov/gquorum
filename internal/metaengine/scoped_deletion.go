package metaengine

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type deletionPreparation struct {
	source, next *scopedState
	collector    planningCollector
	semantics    DeletionSemantics
}

// Prepare new debt without changing existing proof or inventing native removal.
// The caller has already validated scopes and prepared required-stop handover.
// TODO(not planned): retire unconfirmed cleanup through an explicit permanent-host declaration.
func prepareDeletionDebts(source, next *scopedState, collector planningCollector,
	semantics DeletionSemantics) error {
	operation := &next.Operations[len(next.Operations)-1]
	deleting, retired := next.deletionScopes(operation)
	debts := slices.Clone(next.Cleanup)
	existing := make(map[storage.Identity]*cleanupRecord, len(debts))
	for i := range debts {
		existing[debts[i].Identity] = &debts[i]
	}
	preparation := deletionPreparation{source: source, next: next, collector: collector, semantics: semantics}
	var pending error
	for _, key := range collector.sortedOwners() {
		owner := collector.owners[key]
		block, deleted := deleting[key.group]
		id := storage.Identity{ClusterID: next.Target.ClusterID, GroupID: key.group,
			NodeName: owner.target.NodeName, ReplicaID: key.id}
		if !deleted || owner.progress == "" {
			continue
		}
		debt, err := preparation.deletionDebt(id, block, existing[id])
		if errors.Is(err, fault.ErrUnavailable) {
			pending = err
			continue
		}
		if err != nil {
			return err
		}
		if debt.StopAction != nil || semantics == DeletionSemanticsDetached && debt.Identity != (storage.Identity{}) {
			debts = append(debts, debt)
		}
	}
	if pending != nil {
		return pending
	}
	slices.SortFunc(debts, func(a, b cleanupRecord) int { return compareReplicaIdentity(a.Identity, b.Identity) })
	slices.SortFunc(retired, func(a, b retiredGroup) int { return cmp.Compare(a.ID, b.ID) })
	next.Cleanup, next.RetiredGroups = debts, retired
	return nil
}

func (s *scopedState) deletionScopes(operation *topology.Operation) (map[string]topology.PlanBlock, []retiredGroup) {
	deleting := make(map[string]topology.PlanBlock)
	retired := slices.Clone(s.RetiredGroups)
	for _, block := range operation.Plan.Blocks {
		if s.Target.Group(block.GroupID) != nil {
			continue
		}
		deleting[block.GroupID] = block
		if !slices.ContainsFunc(retired, func(row retiredGroup) bool { return row.ID == block.GroupID }) {
			retired = append(retired, retiredGroup{ID: block.GroupID, Version: operation.Status.Version})
		}
	}
	return deleting, retired
}

func (p deletionPreparation) ordinaryDebt(id storage.Identity, block topology.PlanBlock) (cleanupRecord, error) {
	operation := &p.next.Operations[len(p.next.Operations)-1]
	owner := p.collector.owners[planningReplicaKey{id.GroupID, id.ReplicaID}]
	debt := cleanupRecord{Identity: id, MemberID: owner.target.MemberID, OperationID: operation.Status.ID,
		CreatedAt: operation.UpdatedAt, UpdatedAt: operation.UpdatedAt}
	var err error
	debt.Storage, err = p.source.formerReplicaStorage(id, debt.MemberID)
	if err != nil {
		return cleanupRecord{}, err
	}
	stopSteps := block.Steps
	if p.semantics == DeletionSemanticsDetached {
		if owner.progress == topology.RetainedStopped {
			debt.OperationID = "" // The consumed batch owns actionless retirement provenance.
			return p.readmissionDebt(debt, operation.UpdatedAt)
		}
		stopSteps = block.Cleanup
	}
	for _, seq := range stopSteps {
		step := operation.Plan.Steps[seq-1]
		if step.Kind == topology.StepStopReplica && step.NodeName == id.NodeName && step.ReplicaID == id.ReplicaID {
			action := scopedPlanAction(operation, step)
			debt.StopAction = &action
		}
	}
	for _, seq := range block.Cleanup {
		step := operation.Plan.Steps[seq-1]
		if step.Kind == topology.StepDeleteGroupState {
			action := scopedPlanAction(operation, step)
			debt.RetireAction = &action
		}
	}
	if debt.StopAction == nil {
		if p.semantics == DeletionSemanticsDetached {
			return cleanupRecord{}, fault.ErrOperationAuthorization
		}
		// The batch remains the obligation; actionless debt waits for completion.
		_, err := p.collector.deletionReadmission(id.GroupID, owner.target, p.next.Target)
		return cleanupRecord{}, err
	}
	if debt.RetireAction == nil {
		return cleanupRecord{}, fault.ErrOperationAuthorization
	}
	return debt, nil
}

func scopedPlanAction(operation *topology.Operation, step topology.Step) topology.AuthorizedStep {
	return topology.AuthorizedStep{OperationID: topology.OperationID(operation.Status.ID),
		Version: operation.Status.Version, Attempt: operation.Status.Attempt, StepSeq: step.Seq,
		Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID,
		TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID,
		ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
}

// Storage and historical member identity come from retained approved assignments,
// never a staged edit or the replacement process now using the same node name.
func (s *scopedState) formerReplicaStorage(id storage.Identity, member uint64) (topology.StorageClass, error) {
	var found bool
	var class topology.StorageClass
	for _, doc := range s.indexes.documents {
		if !scopedApprovedVersion(s, doc.Version) || doc.MemberID(id.NodeName) != member {
			continue
		}
		replica := documentReplica(doc, id.GroupID, id.ReplicaID)
		if replica == nil || replica.NodeName != id.NodeName {
			continue
		}
		storageClass := doc.Group(id.GroupID).Storage
		if found && class != storageClass {
			return "", fault.ErrIdentityMismatch
		}
		found, class = true, storageClass
	}
	if !found {
		return "", fault.ErrIdentityMismatch
	}
	return class, nil
}

// Match the former identity; a later replacement's complete batch is still proof.
func (c planningCollector) deletionReadmission(group string, target topology.AuthorityTarget,
	desired topology.Document) (*authorityRecord, error) {
	var selected *authorityRecord
	var issued, fenced bool
	admissions := c.readmissionMembers(target.NodeName)
	for _, record := range c.evidence.authority.records {
		grant := record.Grant
		member := admissions[grant.AdmissionID]
		if grant.Purpose != topology.AuthorityReadmit || member == 0 {
			continue
		}
		issued = issued || member == desired.MemberID(target.NodeName)
		if grant.Key.GroupID != group || !slices.Contains(grant.Targets, target) {
			continue
		}
		if !c.evidence.authority.readmissionUsable(grant.AdmissionID) {
			fenced = true
			continue
		}
		if c.evidence.authority.preferReadmission(record, selected) {
			selected = record
		}
	}
	switch {
	case selected != nil:
		return selected, nil
	case fenced:
		return nil, fault.ErrAuthorityFenced
	case !issued && desired.MemberID(target.NodeName) > target.MemberID:
		message := fmt.Sprintf("node %s former member %d: readmission authority not yet issued",
			target.NodeName, target.MemberID)
		return nil, fault.New(fault.Unavailable, message, nil)
	default:
		return nil, fault.ErrOperationAuthorization
	}
}

func validateScopedDeletion(state *scopedState, index *scopedIndexes) error {
	source := *state
	source.indexes = index
	collector, err := deletionCollector(&source, index)
	if err != nil {
		return err
	}
	var completed []topology.PlanBlock
	for _, row := range state.Groups {
		if !row.Deleting && deletionRow(&row) {
			completed = append(completed, topology.PlanBlock{GroupID: row.GroupID})
		}
	}
	preparation := deletionPreparation{source: &source, next: &source, collector: collector}
	if err := preparation.validateDetachedCoverage(completed); err != nil {
		return invalidScopedState("completed deletion lacks exact retained cleanup coverage")
	}
	for _, key := range collector.sortedOwners() {
		owner := collector.owners[key]
		row := index.groups[key.group]
		if owner.progress == "" || row == nil || !row.Deleting {
			continue
		}
		id := storage.Identity{ClusterID: source.Target.ClusterID, GroupID: key.group,
			NodeName: owner.target.NodeName, ReplicaID: key.id}
		if debt := index.cleanup[id]; debt != nil && (debt.Stopped != nil || state.deletionStop(id)) {
			continue
		}
		if _, err := collector.deletionReadmission(key.group, owner.target, source.Target); err != nil {
			return invalidScopedState("pending deletion lacks exact debt or usable readmission coverage")
		}
	}
	return nil
}

func deletionCollector(source *scopedState, index *scopedIndexes) (planningCollector, error) {
	collector := source.planningCollector()
	input := source.planningDeclarations()
	for i, doc := range collector.evidence.documents {
		doc.Groups = slices.DeleteFunc(slices.Clone(doc.Groups), func(g topology.Group) bool {
			return g.ID != topology.MetaGroupID && !deletionRow(index.groups[g.ID])
		})
		collector.evidence.documents[i] = doc
	}
	if _, err := collector.collect(input, topology.Document{}); err != nil {
		return planningCollector{}, invalidScopedState("deletion ownership contradicts retained identity")
	}
	collector.markConvergedOwners(input.Groups)
	collector.markDeletionSources(source, index)
	return collector, nil
}

// A nil checkpoint no longer carries its former declaration. Retained plan
// sources still prove those owners existed, even if a corrupt snapshot loses
// the corresponding cleanup row. Staged and never-converged declarations do not.
func (c planningCollector) markDeletionSources(state *scopedState, index *scopedIndexes) {
	for _, operation := range state.Operations {
		for _, block := range operation.Plan.Blocks {
			if block.SourceVersion != 0 && deletionRow(index.groups[block.GroupID]) {
				doc := index.documents[block.SourceVersion]
				c.markDeletionGroup(doc.Group(block.GroupID))
			}
		}
	}
}

func (c planningCollector) markDeletionGroup(group *topology.Group) {
	if group == nil {
		return
	}
	for _, replica := range group.Replicas {
		if owner := c.owners[planningReplicaKey{group.ID, replica.ReplicaID}]; owner != nil {
			owner.hosted()
		}
	}
}

func (c planningCollector) readmissionMembers(node string) map[string]uint64 {
	members := make(map[string]uint64)
	for _, a := range c.evidence.admissions {
		if a.NodeName == node {
			members[ReadmissionID(a)] = a.MemberID
		}
	}
	return members
}

// Consumption outranks pending proof; the key makes equal candidates deterministic.
func (a authorityCatalog) preferReadmission(candidate, previous *authorityRecord) bool {
	if previous == nil {
		return true
	}
	consumed := a.readmissionBatchConsumed(candidate.Grant.AdmissionID)
	prior := a.readmissionBatchConsumed(previous.Grant.AdmissionID)
	if consumed != prior {
		return consumed
	}
	return compareAuthorityKey(candidate.Grant.Key, previous.Grant.Key) < 0
}

func deletionRow(row *groupConvergence) bool {
	return row != nil && (row.Deleting || row.SourceVersion != 0 && row.Declaration == nil)
}
