//go:build !functional

package metaengine

import (
	"github.com/asalimonov/gquorum/topology"
)

func groupFenceProof(grant topology.AuthorityGrant, index uint64) topology.FenceEvidence {
	return topology.FenceEvidence{Key: grant.Key, Resource: grant.Resources[0], Receiver: topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: "n1", MemberID: 1, InstanceID: "process"}, ReplicaID: 1, AppliedIndex: index, AppliedTerm: 2}
}
func groupEffectProof(grant topology.AuthorityGrant, index uint64) topology.EffectReceipt {
	outcome := topology.EffectApplied
	if grant.Action.Kind == topology.StepTransferLeader {
		outcome = topology.EffectTransferStarted
	}
	return topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0], Action: grant.Action, Target: grant.Targets[0], Outcome: outcome, AppliedIndex: index, AppliedTerm: 2}
}
