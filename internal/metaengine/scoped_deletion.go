package metaengine

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type deletionPreparation struct {
	source, next *scopedState
	collector    planningCollector
}

// Prepare new debt without changing existing proof or inventing native removal.
// The caller has already validated scopes and prepared required-stop handover.
func prepareDeletionDebts(source, next *scopedState, collector planningCollector) error {
	operation := &next.Operations[len(next.Operations)-1]
	deleting := make(map[string]topology.PlanBlock)
	retired := slices.Clone(next.RetiredGroups)
	for _, block := range operation.Plan.Blocks {
		if next.Target.Group(block.GroupID) != nil {
			continue
		}
		deleting[block.GroupID] = block
		if !slices.ContainsFunc(retired, func(row retiredGroup) bool { return row.ID == block.GroupID }) {
			retired = append(retired, retiredGroup{ID: block.GroupID, Version: operation.Status.Version})
		}
	}
	debts := slices.Clone(next.Cleanup)
	existing := make(map[storage.Identity]bool, len(debts))
	for _, debt := range debts {
		existing[debt.Identity] = true
	}
	preparation := deletionPreparation{source, next, collector}
	for key, owner := range collector.owners {
		block, deleted := deleting[key.group]
		id := storage.Identity{ClusterID: next.Target.ClusterID, GroupID: key.group,
			NodeName: owner.target.NodeName, ReplicaID: key.id}
		if !deleted || owner.progress == "" || existing[id] {
			continue
		}
		debt, err := preparation.ordinaryDebt(id, block)
		if err != nil {
			return err
		}
		if debt.StopAction != nil {
			debts = append(debts, debt)
		}
	}
	slices.SortFunc(debts, func(a, b cleanupRecord) int { return compareReplicaIdentity(a.Identity, b.Identity) })
	slices.SortFunc(retired, func(a, b retiredGroup) int { return cmp.Compare(a.ID, b.ID) })
	next.Cleanup, next.RetiredGroups = debts, retired
	return nil
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
	for _, seq := range block.Steps {
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
	return topology.AuthorizedStep{OperatorID: topology.OperatorID(operation.Status.ID),
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
	collector := source.planningCollector()
	input := source.planningDeclarations()
	for i, doc := range collector.evidence.documents {
		doc.Groups = slices.DeleteFunc(slices.Clone(doc.Groups), func(g topology.Group) bool {
			row := index.groups[g.ID]
			return g.ID != topology.MetaGroupID && !deletionRow(row)
		})
		collector.evidence.documents[i] = doc
	}
	if _, err := collector.collect(input, topology.Document{}); err != nil {
		return invalidScopedState("deletion ownership contradicts retained identity")
	}
	collector.markConvergedOwners(input.Groups)
	for key, owner := range collector.owners {
		row := index.groups[key.group]
		id := storage.Identity{ClusterID: source.Target.ClusterID, GroupID: key.group,
			NodeName: owner.target.NodeName, ReplicaID: key.id}
		if owner.progress == "" || !deletionRow(row) {
			continue
		}
		if debt := index.cleanup[id]; debt != nil && (row.Deleting || debt.Stopped != nil) {
			continue
		}
		if !row.Deleting {
			return invalidScopedState("completed deletion lacks exact retained host-stop debt")
		}
		if _, err := collector.deletionReadmission(key.group, owner.target, source.Target); err != nil {
			return invalidScopedState("pending deletion lacks exact debt or usable readmission coverage")
		}
	}
	return nil
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
