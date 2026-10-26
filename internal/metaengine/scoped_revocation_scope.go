package metaengine

import "github.com/asalimonov/gquorum/topology"

func (s *scopedState) supersededBlocks(successor *topology.Operation) []blockRef {
	groups := make(map[string]bool, len(successor.Plan.Blocks))
	for _, block := range successor.Plan.Blocks {
		groups[block.GroupID] = true
	}
	var blocks []blockRef
	for _, operation := range s.Operations {
		if operation.Status.ID == successor.Status.ID {
			continue
		}
		for _, block := range operation.Status.Blocks {
			if groups[block.GroupID] && !block.State.Terminal() {
				blocks = append(blocks, blockRef{operation.Status.ID, block.GroupID})
			}
		}
	}
	return blocks
}

// Only host actions conflict with detached host cleanup. A native membership
// change on another host in the same group cannot fence that cleanup away.
func (s *scopedState) successorResources(operation *topology.Operation,
	semantics DeletionSemantics) map[topology.AuthorityResource]bool {
	resources := make(map[topology.AuthorityResource]bool)
	if operation == nil {
		return resources
	}
	for _, step := range s.successorSteps(operation) {
		if semantics == DeletionSemanticsDetached && s.Target.Group(step.GroupID) == nil {
			s.deletionSuccessorResources(operation, step, resources)
			continue
		}
		resource := topology.AuthorityResource{GroupID: step.GroupID, NodeName: step.NodeName}
		switch step.Kind {
		case topology.StepPrepareReplica, topology.StepStopReplica, topology.StepCleanupReplicaState:
			resource.Kind = topology.AuthorityHost
			resources[resource] = true
		case topology.StepCreateFoundingGroup:
			for _, replica := range s.Target.VotersOf(step.GroupID) {
				resources[topology.AuthorityResource{Kind: topology.AuthorityHost,
					GroupID: step.GroupID, NodeName: replica.NodeName}] = true
			}
			fallthrough
		case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter,
			topology.StepRemoveReplica, topology.StepTransferLeader:
			resource.Kind, resource.NodeName = topology.AuthorityGroup, ""
			resources[resource] = true
		case topology.StepDeleteGroupState:
			s.deletionHostResources(step.GroupID, resources)
		default: // An observational wait issues no authority.
		}
	}
	return resources
}

// Logical deletion inherits existing cleanup rather than claiming every former
// host anew. In particular, an unused conservative Stop cannot fence a still-
// bound readmission batch which owns that host's real retirement.
func (s *scopedState) deletionSuccessorResources(operation *topology.Operation, step topology.Step,
	resources map[topology.AuthorityResource]bool) {
	action := scopedPlanAction(operation, step)
	for _, debt := range s.Cleanup {
		if exactCleanupBinding(&debt, action) {
			resources[topology.AuthorityResource{Kind: topology.AuthorityHost,
				GroupID: debt.Identity.GroupID, NodeName: debt.Identity.NodeName}] = true
		}
	}
}

func (s *scopedState) deletionHostResources(group string, resources map[topology.AuthorityResource]bool) {
	for _, debt := range s.Cleanup {
		if debt.Identity.GroupID == group {
			resources[topology.AuthorityResource{Kind: topology.AuthorityHost,
				GroupID: group, NodeName: debt.Identity.NodeName}] = true
		}
	}
}
