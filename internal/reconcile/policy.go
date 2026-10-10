package reconcile

import (
	"github.com/asalimonov/gquorum/topology"
	"time"
)

// RestoreDegradedPolicy is deliberately narrow: it may propose only a fresh-ID
// replacement for a required Active voter whose runtime identity is absent.
// Approved Target execution is resumed by Reconciler itself; operator staging,
// placement, drain, role, shard, and healthy scale-out intent never reaches the
// policy.
type RestoreDegradedPolicy struct {
	stability time.Duration
}

var _ ReconcilePolicy = (*RestoreDegradedPolicy)(nil)

func NewRestoreDegradedPolicy(stability time.Duration) *RestoreDegradedPolicy {
	if stability <= 0 {
		stability = defaultStabilityWindow
	}
	return &RestoreDegradedPolicy{stability: stability}
}

func (p *RestoreDegradedPolicy) Name() string { return "restore-required-replica" }

// Evaluate returns the first exact repair candidate as an action, or, when no
// candidate is immediately repairable, a bounded reason that distinguishes a
// quorum-lost safety block from a stability wait from genuine convergence.
// Quorum-lost outranks a stability wait because it is the more severe blocker,
// and both outrank converged. A directly repairable candidate always wins: a
// blocked candidate in one group never suppresses a valid repair in another.
func (p *RestoreDegradedPolicy) Evaluate(in ReconcileInput) ReconcileDecision {
	sawQuorumLost := false
	sawStabilityWait := false
	for _, group := range in.Active.Groups {
		if group.ID == topology.MetaGroupID {
			// Meta membership has a dedicated admission/readmission path whose
			// quorum rules differ from data-shard replica replacement.
			continue
		}
		status, ok := in.Status[group.ID]
		if !ok || !status.Found {
			continue
		}
		present := make(map[uint64]struct{}, len(status.ConfState.Voters)+len(status.ConfState.Learners))
		for _, id := range status.ConfState.Voters {
			present[id] = struct{}{}
		}
		for _, id := range status.ConfState.Learners {
			present[id] = struct{}{}
		}
		quorumLost := false
		if health, exists := in.Health[group.ID]; exists && health.ObservedHealth.State.QuorumLost() {
			quorumLost = true
		}
		for _, replica := range group.Replicas {
			if replica.Role != topology.ReplicaVoter {
				continue // intentional learners are never auto-promoted
			}
			node := in.Active.Node(replica.NodeName)
			if node == nil || node.State != topology.NodeActive {
				continue
			}
			if _, exists := present[replica.ReplicaID]; exists {
				continue
			}
			// An exact missing Active voter. Check the more severe quorum-lost
			// safety block before the stability window.
			if quorumLost {
				sawQuorumLost = true
				continue
			}
			if in.Alive[replica.NodeName] < p.stability {
				sawStabilityWait = true
				continue
			}
			return ReconcileDecision{
				Action: &ReconcileAction{
					Kind: ActionRepairReplica, GroupID: group.ID,
					NodeName: replica.NodeName, ReplicaID: replica.ReplicaID,
					Reason: ReconcileReasonMissingReplica,
				},
				Reason: ReconcileReasonMissingReplica,
			}
		}
	}
	switch {
	case sawQuorumLost:
		return ReconcileDecision{Reason: ReconcileReasonQuorumLost}
	case sawStabilityWait:
		return ReconcileDecision{Reason: ReconcileReasonStabilityWindow}
	default:
		return ReconcileDecision{Reason: ReconcileReasonConverged}
	}
}
