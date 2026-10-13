package metaengine

import "github.com/asalimonov/gquorum/topology"

func fixtureEffectReceipts(grant topology.AuthorityGrant) []topology.EffectReceipt {
	var receipts []topology.EffectReceipt
	for _, resource := range grant.Resources {
		if resource.Kind == topology.AuthorityHost {
			for _, target := range grant.Targets {
				if target.NodeName == resource.NodeName {
					receipts = append(receipts, topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, AdmissionID: grant.AdmissionID, Target: target, Outcome: topology.EffectApplied, LocalSequence: grant.Key.Revision*10 + 1})
				}
			}
			continue
		}
		receipt := topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Outcome: topology.EffectApplied, AppliedIndex: grant.Key.Revision*10 + 1, AppliedTerm: 1}
		if grant.Action.Kind == topology.StepTransferLeader {
			receipt.Outcome = topology.EffectTransferStarted
		}
		for _, target := range grant.Targets {
			if target.NodeName == grant.Action.NodeName && target.ReplicaID == grant.Action.ReplicaID {
				receipt.Target = target
			}
		}
		receipts = append(receipts, receipt)
	}
	return receipts
}
