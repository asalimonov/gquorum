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
func (s *scopedState) successorResources(operation *topology.Operation) map[topology.AuthorityResource]bool {
	resources := make(map[topology.AuthorityResource]bool)
	if operation == nil {
		return resources
	}
	for _, step := range operation.Plan.Steps {
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
			for _, debt := range s.Cleanup {
				if debt.Identity.GroupID == step.GroupID {
					resources[topology.AuthorityResource{Kind: topology.AuthorityHost,
						GroupID: step.GroupID, NodeName: debt.Identity.NodeName}] = true
				}
			}
		default: // An observational wait issues no authority.
		}
	}
	return resources
}
