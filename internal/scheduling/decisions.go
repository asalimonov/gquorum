package scheduling

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/votercheck"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

func (r *Scheduler) callPolicy(ctx context.Context,
	in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
	callbackCtx, cancel := context.WithTimeout(ctx, r.callbackTimeout)
	r.statusMu.Lock()
	r.callback, r.callbackStarted = callbackCtx, in.Now
	r.statusMu.Unlock()
	decision, err := r.policy.Schedule(callbackCtx, policyInput(in))
	r.statusMu.Lock()
	r.callback = nil
	r.statusMu.Unlock()
	if err == nil {
		err = callbackCtx.Err()
	}
	cancel()
	return decision, err
}

func (r *Scheduler) reportIdleDecision(reason string) {
	s := r.newStatus(true, topology.SchedulingIdle, reason)
	switch reason {
	case ReconcileReasonCooldown, ReconcileReasonStabilityWindow, "local_stop", "unknown_observation",
		"stale_observation", "failure_window", "standby_window",
		"replica_cooldown", "group_cooldown", "pinned_learner",
		"target_shortfall", "insufficient_observation_frequency", "placement_unsatisfied",
		"voter_domain_conflict", "voter_domain_missing", "node_learner_only",
		"node_reserved", "selector_key_missing", "selector_value_mismatch":
		s = r.newStatus(true, topology.SchedulingWaiting, reason)
	case ReconcileReasonQuorumLost:
		s = r.newStatus(true, topology.SchedulingBlocked, ReconcileReasonQuorumLost)
	default: // Keep the initial status for other decision reasons.
	}
	r.transition(s, "")
}

func (r *Scheduler) validateReplicaRepair(active topology.Document,
	action schedulingapi.ScheduleAction, status map[string]topology.GroupStatus) error {
	identity := action.Identity
	group, node := active.Group(identity.GroupID), active.Node(identity.NodeName)
	observed, ok := status[identity.GroupID]
	if action.Kind != schedulingapi.RestoreMissingReplica || identity.ClusterID != active.ClusterID || group == nil ||
		group.VoterConfig != nil || identity.GroupID == topology.MetaGroupID || node == nil ||
		node.State != topology.NodeActive || !ok || !observed.Found || observed.GroupID != identity.GroupID ||
		topology.NodeReserved(*node) || slices.Contains(r.heldIdentities(), identity) {
		return fault.ErrOperationAuthorization
	}
	for _, id := range append(append([]uint64(nil), observed.ConfState.Voters...), observed.ConfState.Learners...) {
		if id == identity.ReplicaID {
			return fault.New(fault.OperationAuthorization, "repair identity is already a member", nil)
		}
	}
	return nil
}

func (r *Scheduler) refusePolicyScope(in schedulingapi.ScheduleInput, group string) bool {
	_, reason := votercheck.ReadyGroup(votercheck.Input{Target: in.Target, Groups: in.Groups}, group)
	if reason == "" {
		return false
	}
	status := r.newStatus(true, topology.SchedulingWaiting, reason)
	status.CurrentAction = &topology.SchedulingActionStatus{GroupID: group}
	r.transition(status, string(in.Groups[group].OperationID))
	return true
}
