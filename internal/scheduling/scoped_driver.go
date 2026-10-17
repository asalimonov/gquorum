package scheduling

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/votercheck"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type repairAttempt struct {
	identity storage.Identity
	source   uint64
}

func (r *Scheduler) reconcileOnce(ctx context.Context) {
	if r.scopedInactive() {
		return
	}
	read, err := r.host.ReadRepair(ctx, "")
	if err != nil {
		r.reportRepairError(err, nil)
		return
	}
	if read.Target.Version == 0 {
		r.transition(r.newStatus(true, topology.SchedulingWaiting, ReconcileReasonStarting), "")
		return
	}
	in, err := r.scopedInput(ctx, read)
	if err != nil {
		r.reportRepairError(err, nil)
		return
	}
	r.pruneRepairAttempts(read)
	decision, err := r.callPolicy(ctx, in)
	if err != nil {
		r.reportRepairError(err, nil)
		return
	}
	if decision.Action == nil {
		r.reportIdleDecision(decision.Reason)
		return
	}
	action := *decision.Action
	group, err := repairActionGroup(action)
	if err != nil {
		r.reportRepairError(err, nil)
		return
	}
	if r.refusePolicyScope(in, group) {
		return
	}
	key := repairAttempt{identity: action.Identity, source: in.Groups[group].ConvergedVersion}
	if action.Kind == schedulingapi.RestoreMissingReplica &&
		!r.repairAttempts[key].IsZero() && in.Now.Sub(r.repairAttempts[key]) < r.settings.Cooldown {
		r.reportIdleDecision(ReconcileReasonCooldown)
		return
	}
	id, err := r.commitScopedRepair(ctx, in, action)
	if err != nil {
		r.reportRepairError(err, &action)
		return
	}
	r.transition(r.runningOperationStatus(true, id), "")
}

func (r *Scheduler) scopedInactive() bool {
	if !r.enabled.Load() {
		r.resetAlive()
		if r.host.IsLeader() {
			r.host.WakeOperations()
		}
		r.transition(r.newStatus(r.host.IsLeader(), topology.SchedulingDisabled, ReconcileReasonConfigDisabled), "")
		return true
	}
	if !r.host.IsLeader() {
		r.resetAlive()
		r.transition(r.newStatus(false, topology.SchedulingStandby, ReconcileReasonNotLeader), "")
		return true
	}
	return false
}

func (r *Scheduler) scopedInput(ctx context.Context, read metaengine.PlanRead) (schedulingapi.ScheduleInput, error) {
	health, err := r.host.Health(ctx)
	if err != nil {
		return schedulingapi.ScheduleInput{}, err
	}
	now := r.now()
	r.sampleAlive(read.Target, health, now)
	in := schedulingapi.ScheduleInput{Now: now, Target: read.Target, Groups: read.Input.Groups,
		Status: make(map[string]topology.GroupStatus), Health: make(map[string]topology.GroupHealthEvaluation),
		Alive: r.aliveDurations(now), Held: r.heldIdentities(),
		Metrics: r.host.Observations(), Switches: r.host.VoterSwitches()}
	// An unreadable group is an absent observation, not a global scheduling gate.
	for _, scope := range read.Scopes {
		id := scope.GroupID
		in.Health[id] = topology.EvaluateGroupHealth(read.Target, id, health)
		if _, why := votercheck.ReadyGroup(votercheck.Input{Target: in.Target, Groups: in.Groups}, id); why != "" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, r.settings.TickTimeout/time.Duration(len(read.Scopes)+2))
		fresh, err := r.host.ReadRepair(probeCtx, id)
		cancel()
		if err != nil || sameRepairScope(in, fresh, id) != nil {
			continue
		}
		if status := fresh.Input.Groups[id].Observation; status != nil {
			in.Status[id] = status.Clone()
		}
	}
	if err := ctx.Err(); err != nil {
		return schedulingapi.ScheduleInput{}, err
	}
	if !r.host.IsLeader() {
		return schedulingapi.ScheduleInput{}, fault.ErrNotLeader
	}
	return in, nil
}

func repairActionGroup(action schedulingapi.ScheduleAction) (string, error) {
	switch action.Kind {
	case schedulingapi.RestoreMissingReplica:
		if action.VoterChange == nil && action.Identity.ClusterID != "" && action.Identity.ReplicaID != 0 &&
			action.Identity.NodeName != "" && topology.ValidateLoadID(action.Identity.GroupID) == nil {
			return action.Identity.GroupID, nil
		}
	case schedulingapi.ReconfigureVoters:
		if action.VoterChange != nil && action.Identity == (storage.Identity{}) {
			return action.VoterChange.GroupID, nil
		}
	default: // Unknown action variants cannot originate repair.
	}
	return "", fault.ErrOperationAuthorization
}

func sameRepairScope(in schedulingapi.ScheduleInput, fresh metaengine.PlanRead, group string) error {
	before, after := in.Groups[group], fresh.Input.Groups[group]
	if before.ConvergedVersion != after.ConvergedVersion || before.OperationID != after.OperationID ||
		slices.Contains(topology.AffectedGroups(in.Target, fresh.Input.Approved), group) {
		return fault.ErrVersionConflict
	}
	_, why := votercheck.ReadyGroup(votercheck.Input{Target: fresh.Input.Approved, Groups: fresh.Input.Groups}, group)
	if why != "" {
		return fault.New(fault.AuthorityPending, why+": "+string(after.OperationID), nil)
	}
	return nil
}

func (r *Scheduler) pruneRepairAttempts(read metaengine.PlanRead) {
	for key := range r.repairAttempts {
		if read.Input.Groups[key.identity.GroupID].ConvergedVersion != key.source {
			delete(r.repairAttempts, key)
		}
	}
}

func (r *Scheduler) reportRepairError(err error, action *schedulingapi.ScheduleAction) {
	reason := ReconcileReasonPolicyRejected
	switch {
	case errors.Is(err, errStagedRepair):
		reason = "staged_conflict"
	case errors.Is(err, fault.ErrVersionConflict):
		reason = ReconcileReasonCASConflict
	case errors.Is(err, fault.ErrAuthorityPending):
		reason = "group_busy"
	case errors.Is(err, fault.ErrNotLeader):
		reason = ReconcileReasonNotLeader
	default: // Other validation errors retain the policy-refusal diagnostic.
	}
	status := r.newStatus(r.host.IsLeader(), topology.SchedulingWaiting, reason)
	if action != nil {
		group, _ := repairActionGroup(*action)
		status.CurrentAction = &topology.SchedulingActionStatus{Kind: string(action.Kind), GroupID: group,
			NodeName: action.Identity.NodeName, ReplicaID: action.Identity.ReplicaID}
	}
	r.transition(status, err.Error())
}
