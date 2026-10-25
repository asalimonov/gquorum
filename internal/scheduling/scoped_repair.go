package scheduling

import (
	"context"
	"errors"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/recoveryplacement"
	"github.com/asalimonov/gquorum/internal/votercheck"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

var errStagedRepair = errors.New("staged_conflict")

// A callback selects an exact action. Unrelated changes may refresh the CAS
// base before planning; changes during planning refuse and retry on a later tick.
func (r *Scheduler) commitScopedRepair(ctx context.Context, in schedulingapi.ScheduleInput,
	action schedulingapi.ScheduleAction) (topology.OperationID, error) {
	fresh, err := r.recheckScopedRepair(ctx, in, action)
	if err != nil {
		return "", err
	}
	doc, err := scopedRepairDocument(fresh, action, r.now().UTC())
	if err != nil {
		return "", err
	}
	if err := validateRepairRecovery(fresh, doc); err != nil {
		return "", err
	}
	if err := metaengine.ValidateRepairIndependence(fresh.Target, fresh.Latest, doc); err != nil {
		return "", errors.Join(errStagedRepair, err)
	}
	plan, err := r.host.PlanRepair(ctx, fresh, doc)
	if err != nil {
		return "", err
	}
	if len(plan.Steps) == 0 {
		return "", fault.ErrOperationAuthorization
	}
	after, err := r.recheckScopedRepair(ctx, in, action)
	if err != nil {
		return "", err
	}
	if after.Latest.Version != fresh.Latest.Version || after.Target.Version != fresh.Target.Version ||
		!sameRepairMembership(after, fresh) {
		return "", fault.ErrVersionConflict
	}
	if err := validateRepairRecovery(after, doc); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !r.host.IsLeader() || !r.enabled.Load() {
		return "", fault.ErrNotLeader
	}
	commit := RepairCommit{Document: doc, ExpectedVersion: fresh.Latest.Version,
		ExpectedTarget: fresh.Target.Version,
		Scopes:         slices.Clone(fresh.Scopes), Plan: plan}
	var id topology.OperationID
	if action.Kind == schedulingapi.ReconfigureVoters {
		id, err = r.host.CommitVoterChange(ctx, action.VoterChange.Clone(), commit)
	} else {
		key := repairAttempt{identity: action.Identity,
			source: fresh.Input.Groups[action.Identity.GroupID].ConvergedVersion}
		r.repairAttempts[key] = r.now()
		id, err = r.host.CommitRepair(ctx, commit)
	}
	if err == nil {
		r.host.WakeOperations()
	}
	return id, err
}

func validateRepairRecovery(read metaengine.PlanRead, doc topology.Document) error {
	ctx, err := recoveryplacement.FromImport(read.Recovery, read.Target)
	if err != nil {
		return err
	}
	return recoveryplacement.ValidateEdit(ctx, read.Target, doc)
}

func (r *Scheduler) recheckScopedRepair(ctx context.Context, in schedulingapi.ScheduleInput,
	action schedulingapi.ScheduleAction) (metaengine.PlanRead, error) {
	group, err := repairActionGroup(action)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	if err := ctx.Err(); err != nil {
		return metaengine.PlanRead{}, err
	}
	if !r.host.IsLeader() || !r.enabled.Load() {
		return metaengine.PlanRead{}, fault.ErrNotLeader
	}
	if err := r.host.RefreshObservations(ctx); err != nil {
		return metaengine.PlanRead{}, err
	}
	fresh, err := r.host.ReadRepair(ctx, group)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	if err := sameRepairScope(in, fresh, group); err != nil {
		return metaengine.PlanRead{}, err
	}
	health, err := r.host.Health(ctx)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	r.sampleAlive(fresh.Target, health, r.now())
	if err := r.checkRepairEvidence(in, fresh, action); err != nil {
		return metaengine.PlanRead{}, err
	}
	evaluation := topology.EvaluateGroupHealth(fresh.Target, group, health)
	if evaluation.ObservedHealth.State == topology.GroupUnknown {
		return metaengine.PlanRead{}, fault.ErrUnavailable
	}
	if evaluation.ObservedHealth.State.QuorumLost() {
		return metaengine.PlanRead{}, fault.ErrUnsafeMembership
	}
	return fresh, nil
}

func (r *Scheduler) checkRepairEvidence(in schedulingapi.ScheduleInput, fresh metaengine.PlanRead,
	action schedulingapi.ScheduleAction) error {
	observed := r.host.Observations()
	if !observed.IsCollectorLeader || observed.CollectorRunID == "" || observed.ObserverInstanceID == "" ||
		observed.CollectorRunID != in.Metrics.CollectorRunID ||
		observed.ObserverInstanceID != in.Metrics.ObserverInstanceID {
		return fault.ErrNotLeader
	}
	group, _ := repairActionGroup(action)
	status := fresh.Input.Groups[group].Observation
	native := fresh.Input.Native[group]
	if status == nil || !status.Found || status.SourceVersion != fresh.Input.Groups[group].ConvergedVersion ||
		!status.HealthKnown || status.LeaderID == 0 || native.Applied == 0 || native.GroupID != group ||
		!reflect.DeepEqual(status.ConfState, native.ConfState) {
		return fault.ErrUnavailable
	}
	if action.Kind == schedulingapi.ReconfigureVoters {
		why := votercheck.Check(votercheck.Input{Target: fresh.Target, Groups: fresh.Input.Groups,
			Metrics: observed,
			Status:  map[string]topology.GroupStatus{group: *status}, Held: r.heldIdentities()}, *action.VoterChange)
		if why != "" {
			return fault.New(fault.OperationAuthorization, why, nil)
		}
		return nil
	}
	return r.checkMissingReplica(fresh, action, *status)
}

func (r *Scheduler) checkMissingReplica(fresh metaengine.PlanRead, action schedulingapi.ScheduleAction,
	status topology.GroupStatus) error {
	cs := status.ConfState
	if !status.LiveVoters[status.LeaderID] || cs.AutoLeave ||
		len(cs.VotersOutgoing) != 0 || len(cs.LearnersNext) != 0 || !slices.Contains(cs.Voters, status.LeaderID) {
		return fault.ErrUnsafeMembership
	}
	live := 0
	for _, id := range cs.Voters {
		if status.LiveVoters[id] {
			live++
		}
	}
	if live < topology.QuorumOf(len(cs.Voters)) {
		return fault.ErrUnsafeMembership
	}
	if r.aliveDurations(r.now())[action.Identity.NodeName] < r.settings.StabilityWindow {
		return fault.New(fault.Unavailable, ReconcileReasonStabilityWindow, nil)
	}
	return r.validateReplicaRepair(fresh.Target, action,
		map[string]topology.GroupStatus{action.Identity.GroupID: status})
}

// Applied positions may advance during planning. Membership and the observed
// leader epoch must remain the ones used to construct the immutable plan.
func sameRepairMembership(a, b metaengine.PlanRead) bool {
	for _, scope := range a.Scopes {
		id := scope.GroupID
		left, right := a.Input.Groups[id].Observation, b.Input.Groups[id].Observation
		if left == nil || right == nil || left.LeaderID != right.LeaderID || left.Term != right.Term ||
			!reflect.DeepEqual(a.Input.Native[id].ConfState, b.Input.Native[id].ConfState) {
			return false
		}
	}
	return true
}
