//go:build functional

package operator

import "github.com/asalimonov/gquorum/topology"

func controllerReceipts(grant topology.AuthorityGrant) []topology.EffectReceipt {
	var result []topology.EffectReceipt
	for _, resource := range grant.Resources {
		if resource.Kind == topology.AuthorityHost {
			for _, target := range grant.Targets {
				if target.NodeName == resource.NodeName {
					result = append(result, topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Target: target, Outcome: topology.EffectApplied, LocalSequence: grant.Key.Revision})
				}
			}
			continue
		}
		receipt := topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Outcome: topology.EffectApplied, AppliedIndex: grant.Key.Revision + 1, AppliedTerm: 1}
		if grant.Action.Kind == StepTransferLeader {
			receipt.Outcome = topology.EffectTransferStarted
		}
		for _, target := range grant.Targets {
			if target.NodeName == grant.Action.NodeName && target.ReplicaID == grant.Action.ReplicaID {
				receipt.Target = target
			}
		}
		result = append(result, receipt)
	}
	return result
}
