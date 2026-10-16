package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func scopedVoterVersion(state *scopedState, version uint64) bool {
	for _, group := range state.VoterSwitches {
		if slices.ContainsFunc(group.Replicas, func(r topology.RoleChangeRecord) bool { return r.Version == version }) {
			return true
		}
	}
	return false
}

func validateScopedVoterSwitches(state *scopedState, index *scopedIndexes, catalog authorityCatalog) error {
	if len(state.VoterSwitches) == 0 {
		return nil
	}
	// Reuse the unchanged shape checks, with no legacy operators or Active.
	check := newVoterSwitchValidation(smState{Latest: state.Latest, Target: state.Target,
		History: state.History, Allocations: state.Allocations, AppliedIndex: state.AppliedIndex})
	if err := check.rememberCurrent(); err != nil {
		return err
	}
	if err := check.rememberHistory(); err != nil {
		return err
	}
	if !check.rememberScopedDeclarations(state) || !check.rememberScopedEffects(state) {
		return invalidVoterSwitch()
	}
	// One retry Apply may acknowledge one skipped role effect in each failed group.
	for _, group := range state.VoterSwitches {
		check.seenIndexes = map[uint64]bool{}
		if err := check.groupState(group); err != nil {
			return err
		}
		for _, record := range group.Replicas {
			if !scopedRoleEffect(record, index, catalog) {
				return invalidScopedState("voter switch lacks its exact completed role effect")
			}
		}
	}
	return nil
}

func (check *voterSwitchValidation) rememberScopedDeclarations(state *scopedState) bool {
	for _, row := range state.Groups {
		if row.Declaration == nil {
			continue
		}
		for _, replica := range row.Declaration.Replicas {
			if !check.remember(row.GroupID, replica.NodeName, replica.ReplicaID, retainSwitchIdentity) {
				return false
			}
		}
	}
	for _, row := range state.Nodes {
		if !check.remember(topology.MetaGroupID, row.NodeName, row.MemberID, retainSwitchIdentity) {
			return false
		}
	}
	for _, admission := range state.Admissions {
		if !check.remember(topology.MetaGroupID, admission.NodeName, admission.MemberID, retainSwitchIdentity) {
			return false
		}
	}
	return true
}

func (check *voterSwitchValidation) rememberScopedEffects(state *scopedState) bool {
	for _, op := range state.Operations {
		for _, step := range op.Plan.Steps {
			if !check.remember(step.GroupID, step.NodeName, step.ReplicaID, retainSwitchIdentity) ||
				!check.remember(step.GroupID, step.TransferNodeName, step.TransferReplicaID, retainSwitchIdentity) {
				return false
			}
		}
	}
	for _, debt := range state.Cleanup {
		id := debt.Identity
		if !check.remember(id.GroupID, id.NodeName, id.ReplicaID, retainSwitchIdentity) {
			return false
		}
	}
	return check.rememberScopedGrants(state.AuthorityRecords)
}

func (check *voterSwitchValidation) rememberScopedGrants(records []scopedAuthorityRecord) bool {
	for _, record := range records {
		for _, target := range record.Grant.Targets {
			if !check.remember(record.Grant.Key.GroupID, target.NodeName, target.ReplicaID, retainSwitchIdentity) ||
				!check.remember(topology.MetaGroupID, target.NodeName, target.MemberID, retainSwitchIdentity) {
				return false
			}
		}
	}
	return true
}

func scopedRoleEffect(record topology.RoleChangeRecord, index *scopedIndexes, catalog authorityCatalog) bool {
	op := index.operations[topology.OperationID(record.OperatorID)]
	if op == nil {
		// Cooldown anchors outlive an entire pruned attempt chain, not one missing attempt.
		return index.first[record.Version] == ""
	}
	if record.StepSeq > len(op.Plan.Steps) {
		return false
	}
	step := op.Plan.Steps[record.StepSeq-1]
	kind := topology.StepPromoteVoter
	if record.To == topology.ReplicaLearner {
		kind = topology.StepDemoteVoter
	}
	if step.Kind != kind || step.GroupID != record.GroupID || step.NodeName != record.NodeName ||
		step.ReplicaID != record.ReplicaID || !catalog.planStepConsumed(op.Status, step, retainedCompletion) {
		return false
	}
	if record.Index <= catalog.planStepAuthority(op.Status, step).Grant.Key.Revision {
		return false // The meta completion application must follow grant issuance.
	}
	// The receipt keeps its original attempt. A retry may be the first to record
	// completion after that receipt was consumed; it must not reset cooldown by
	// relabeling the role effect to a new attempt with no own consumption.
	for current := op; current != nil; current = index.operations[index.retries[current.Status.ID]] {
		for _, block := range current.Status.Blocks {
			if block.GroupID == record.GroupID && scopedStepCompleted(block, record.StepSeq) {
				return true
			}
		}
	}
	return false
}
