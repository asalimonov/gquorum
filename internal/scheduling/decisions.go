package scheduling

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/votercheck"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type schedulingApproval struct {
	views    topology.ReadView
	operator topology.OperatorLookup
	leader   bool
}

func (r *Scheduler) reconcileApproval(ctx context.Context, input schedulingApproval) bool {
	latest, target, active := input.views.Latest, input.views.Target, input.views.Active
	operator, leader := input.operator, input.leader
	if !r.enabled.Load() {
		r.resetAlive()
		if leader && resumable(operator.Operator, operator.Found, active, target) {
			r.host.WakeOperator()
		}
		r.transition(r.newStatus(leader, topology.SchedulingDisabled, ReconcileReasonConfigDisabled), "")
		return true
	}
	if resumable(operator.Operator, operator.Found, active, target) {
		if leader {
			r.host.WakeOperator()
		}
		r.transition(r.runningOperatorStatus(leader, operator.Operator.Status.ID), "")
		return true
	}
	if operator.Found && operator.Operator.Status.State == topology.OperatorFailed &&
		operator.Operator.Status.Version == latest.Version {
		s := r.newStatus(leader, topology.SchedulingBlocked, ReconcileReasonFailedOperator)
		s.BlockingVersion, s.CurrentOperatorID = &operator.Operator.Status.Version, &operator.Operator.Status.ID
		r.transition(s, "")
		return true
	}
	if !reflect.DeepEqual(latest, target) || !reflect.DeepEqual(latest, active) {
		if leader && !hasVoterConfigs(active) && r.healStagedInternal(ctx, latest, active) {
			return true
		}
		r.transition(r.blockedStagedStatus(leader, latest.Version), "")
		return true
	}
	if !leader {
		r.transition(r.newStatus(false, topology.SchedulingStandby, ReconcileReasonNotLeader), "")
		return true
	}
	return false
}

func (r *Scheduler) evaluatePolicy(ctx context.Context, in schedulingapi.ScheduleInput, views topology.ReadView) {
	decision, err := r.callPolicy(ctx, in)
	if err != nil {
		r.transition(r.newStatus(true, topology.SchedulingError, ReconcileReasonPolicyRejected), err.Error())
		return
	}
	if decision.Action == nil {
		r.reportIdleDecision(decision.Reason)
		return
	}
	action := *decision.Action
	if action.Kind == schedulingapi.ReconfigureVoters {
		r.applyVoterDecision(ctx, in, views, action)
		return
	}
	r.applyReplicaDecision(ctx, in, views, action)
}

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

func (r *Scheduler) applyVoterDecision(ctx context.Context,
	in schedulingapi.ScheduleInput, views topology.ReadView, action schedulingapi.ScheduleAction) {
	if action.VoterChange == nil || action.Identity != (storage.Identity{}) {
		r.transition(r.newStatus(true, topology.SchedulingError,
			ReconcileReasonPolicyRejected), "invalid voter action union")
		return
	}
	if r.refusePolicyScope(in, action.VoterChange.GroupID) {
		return
	}
	change := action.VoterChange.Clone()
	id, err := r.commitVoterChange(ctx, in, views, change)
	if err != nil {
		reason := ReconcileReasonPolicyRejected
		if errors.Is(err, fault.ErrVersionConflict) {
			reason = ReconcileReasonCASConflict
		}
		r.transition(r.newStatus(true, topology.SchedulingWaiting, reason), err.Error())
		return
	}
	r.transition(r.runningOperatorStatus(true, id), "")
}

func (r *Scheduler) applyReplicaDecision(ctx context.Context,
	in schedulingapi.ScheduleInput, views topology.ReadView, action schedulingapi.ScheduleAction) {
	if action.VoterChange != nil {
		r.transition(r.newStatus(true, topology.SchedulingError,
			ReconcileReasonPolicyRejected), "mixed action variants")
		return
	}
	if !r.lastRun.IsZero() && in.Now.Sub(r.lastRun) < r.settings.Cooldown {
		r.transition(r.newStatus(true, topology.SchedulingWaiting, ReconcileReasonCooldown), "")
		return
	}
	if action.Kind != schedulingapi.RestoreMissingReplica {
		r.transition(r.newStatus(true, topology.SchedulingError, ReconcileReasonPolicyRejected),
			fmt.Sprintf("forbidden action %q", action.Kind))
		return
	}
	if r.refusePolicyScope(in, action.Identity.GroupID) {
		return
	}
	s := r.newStatus(true, topology.SchedulingRunning, ReconcileReasonMissingReplica)
	s.CurrentAction = &topology.SchedulingActionStatus{
		Kind:      string(action.Kind),
		GroupID:   action.Identity.GroupID,
		NodeName:  action.Identity.NodeName,
		ReplicaID: action.Identity.ReplicaID,
	}
	r.transition(s, "")
	id, err := r.commitReplicaRepair(ctx, replicaRepair{Latest: views.Latest, Active: views.Active,
		Action: action, At: in.Now})
	r.lastRun = in.Now
	if err != nil {
		reason := ReconcileReasonPolicyRejected
		if errors.Is(err, fault.ErrVersionConflict) {
			reason = ReconcileReasonCASConflict
		}
		r.transition(r.newStatus(true, topology.SchedulingError, reason), err.Error())
		return
	}
	r.transition(r.runningOperatorStatus(true, id), "")
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
		return fault.ErrOperatorAuthorization
	}
	for _, id := range append(append([]uint64(nil), observed.ConfState.Voters...), observed.ConfState.Learners...) {
		if id == identity.ReplicaID {
			return fault.New(fault.OperatorAuthorization, "repair identity is already a member", nil)
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
