package metaengine

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type planningReplicaKey struct {
	group string
	id    uint64
}

type planningReplica struct {
	target     topology.AuthorityTarget
	progress   topology.RetainedProgress
	removed    uint64
	readmitted bool
}

type planningCollector struct {
	evidence planningEvidence
	owners   map[planningReplicaKey]*planningReplica
	founding map[string]bool
}

func collectPlanningEvidence(input topology.PlanInput, desired topology.Document,
	evidence planningEvidence) (topology.PlanInput, error) {
	c := planningCollector{evidence: evidence, owners: make(map[planningReplicaKey]*planningReplica),
		founding: make(map[string]bool)}
	if err := c.declarations(); err != nil {
		return topology.PlanInput{}, err
	}
	if err := c.grants(); err != nil {
		return topology.PlanInput{}, err
	}
	if err := c.debts(); err != nil {
		return topology.PlanInput{}, err
	}
	if err := c.native(input.Native, desired); err != nil {
		return topology.PlanInput{}, err
	}
	if err := c.desiredBindings(desired); err != nil {
		return topology.PlanInput{}, err
	}
	input.Retained = c.result(desired)
	return input, nil
}

func (c *planningCollector) remember(group string, target topology.AuthorityTarget) (*planningReplica, error) {
	if target.NodeName == "" || target.MemberID == 0 || target.ReplicaID == 0 ||
		target.ReplicaID >= c.evidence.allocations[group] {
		return nil, fault.ErrIdentityMismatch
	}
	key := planningReplicaKey{group, target.ReplicaID}
	if old := c.owners[key]; old != nil {
		if old.target != target {
			return nil, fault.ErrIdentityMismatch
		}
		return old, nil
	}
	owner := &planningReplica{target: target}
	c.owners[key] = owner
	return owner, nil
}

func (c *planningCollector) declarations() error {
	for _, doc := range c.evidence.documents {
		for _, group := range doc.Groups {
			for _, replica := range group.Replicas {
				_, err := c.remember(group.ID, topology.AuthorityTarget{NodeName: replica.NodeName,
					MemberID: doc.MemberID(replica.NodeName), ReplicaID: replica.ReplicaID})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *planningCollector) grants() error {
	for _, record := range c.evidence.authority.records {
		grant := record.Grant
		for _, target := range grant.Targets {
			owner, err := c.remember(grant.Key.GroupID, target)
			if err != nil {
				return err
			}
			if grant.Action != nil && (grant.Action.Kind == topology.StepPrepareReplica ||
				grant.Action.Kind == topology.StepCreateFoundingGroup) {
				owner.hosted()
				c.founding[grant.Key.GroupID] = c.founding[grant.Key.GroupID] ||
					grant.Action.Kind == topology.StepCreateFoundingGroup
			}
		}
	}
	for _, record := range c.evidence.authority.records {
		c.receipts(record)
	}
	return nil
}

func (owner *planningReplica) hosted() {
	if owner.progress == "" {
		owner.progress = topology.RetainedHosted
	}
}

func (c *planningCollector) receipts(record *authorityRecord) {
	grant := record.Grant
	readmission := grant.Purpose == topology.AuthorityReadmit && c.completeReadmission(grant.AdmissionID)
	for _, receipt := range record.Effects {
		if receipt.Outcome == topology.EffectTransferStarted {
			continue
		}
		owner := c.owners[planningReplicaKey{grant.Key.GroupID, receipt.Target.ReplicaID}]
		if owner == nil || owner.target != receipt.Target {
			continue // Only exact validated grant targets can establish progress.
		}
		if readmission {
			owner.readmitted, owner.progress = true, topology.RetainedStopped
		} else {
			owner.receipt(grant.Action, receipt)
		}
	}
}

func (owner *planningReplica) receipt(action *topology.AuthorizedStep, receipt topology.EffectReceipt) {
	if action == nil {
		return
	}
	switch action.Kind {
	case topology.StepRemoveReplica:
		owner.removed = max(owner.removed, receipt.AppliedIndex)
		if owner.progress != topology.RetainedStopped {
			owner.progress = topology.RetainedCleanup
		}
	case topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepDeleteGroupState:
		owner.progress = topology.RetainedStopped
	default: // Native/preparation acknowledgements alone do not stop an owner.
	}
}

func (c *planningCollector) completeReadmission(id string) bool {
	return slices.ContainsFunc(c.evidence.admissions, func(admission AdmissionRecord) bool {
		return ReadmissionID(admission) == id && c.evidence.authority.readmissionBatchConsumed(id)
	})
}

func (c *planningCollector) debts() error {
	for _, debt := range c.evidence.cleanup {
		owner, err := c.remember(debt.Identity.GroupID, topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
			MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID})
		if err != nil {
			return err
		}
		if debt.Removed != nil {
			owner.removed = max(owner.removed, debt.Removed.AppliedIndex)
		}
		if debt.Stopped != nil {
			owner.progress = topology.RetainedStopped
		} else if owner.progress != topology.RetainedStopped {
			owner.progress = topology.RetainedCleanup
		}
	}
	return nil
}

func (c *planningCollector) native(states map[string]topology.NativeState, desired topology.Document) error {
	for group, state := range states {
		if state.GroupID != group || state.ReplicaID == 0 || state.Applied == 0 {
			return fault.ErrUnavailable
		}
		if _, err := c.nativeIdentity(state.GroupID, desired, state.ReplicaID); err != nil {
			return err
		}
		for _, ids := range [][]uint64{state.ConfState.Voters, state.ConfState.Learners,
			state.ConfState.VotersOutgoing, state.ConfState.LearnersNext} {
			for _, id := range ids {
				if err := c.nativeOwner(state, desired, id); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *planningCollector) nativeIdentity(group string, desired topology.Document,
	id uint64) (*planningReplica, error) {
	if owner := c.owners[planningReplicaKey{group, id}]; owner != nil {
		return owner, nil
	}
	// Founding metadata is native before approval. Desired may name observed
	// membership; without a native read it creates no possible host state.
	replica := documentReplica(desired, group, id)
	if replica == nil {
		return nil, fault.ErrIdentityMismatch
	}
	return c.remember(group, topology.AuthorityTarget{NodeName: replica.NodeName,
		MemberID: desired.MemberID(replica.NodeName), ReplicaID: id})
}

func (c *planningCollector) nativeOwner(state topology.NativeState, desired topology.Document, id uint64) error {
	owner, err := c.nativeIdentity(state.GroupID, desired, id)
	if err != nil {
		return err
	}
	if owner.removed != 0 {
		if state.Applied < owner.removed {
			return fault.ErrUnavailable
		}
		return fault.ErrUnsafeMembership
	}
	if owner.progress == topology.RetainedCleanup || owner.progress == topology.RetainedStopped && !owner.readmitted {
		return fault.ErrUnsafeMembership
	}
	owner.hosted()
	return nil
}

func (c *planningCollector) desiredBindings(desired topology.Document) error {
	for key, owner := range c.owners {
		if replica := documentReplica(desired, key.group, key.id); replica != nil &&
			(replica.NodeName != owner.target.NodeName || desired.MemberID(replica.NodeName) != owner.target.MemberID) {
			return fault.ErrIdentityMismatch
		}
	}
	return nil
}

func (c *planningCollector) result(desired topology.Document) map[string]topology.RetainedGroup {
	result := make(map[string]topology.RetainedGroup)
	for group, issued := range c.founding {
		result[group] = topology.RetainedGroup{FoundingIssued: issued}
	}
	for key, owner := range c.owners {
		if owner.progress == "" || documentReplica(desired, key.group, key.id) != nil {
			continue
		}
		group := result[key.group]
		group.Replicas = append(group.Replicas, topology.RetainedReplica{
			ReplicaKey: topology.ReplicaKey{NodeName: owner.target.NodeName, ReplicaID: key.id},
			Progress:   owner.progress})
		result[key.group] = group
	}
	for _, group := range result {
		slices.SortFunc(group.Replicas, func(a, b topology.RetainedReplica) int {
			return cmp.Compare(a.ReplicaID, b.ReplicaID)
		})
	}
	return result
}
