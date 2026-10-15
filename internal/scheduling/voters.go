package scheduling

import (
	"context"
	"reflect"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/votercheck"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

// commitVoterChange rechecks after the callback and after planning. Neither a
// cached policy decision nor a successful preview authorizes membership
// effects.
func (r *Scheduler) commitVoterChange(ctx context.Context, in schedulingapi.ScheduleInput,
	change topology.VoterChange) (topology.OperatorID, error) {
	fresh, err := r.recheckVoterChange(ctx, in, change)
	if err != nil {
		return "", err
	}
	doc, err := metaengine.VoterChangeDocument(fresh.Active, change, r.now().UTC(), "auto-voter-config")
	if err != nil {
		return "", err
	}
	plan, err := r.host.PlanFor(ctx, doc)
	if err != nil {
		return "", err
	}
	if len(plan.Steps) == 0 {
		return "", fault.ErrOperatorAuthorization
	}
	if _, err := r.recheckVoterChange(ctx, in, change); err != nil {
		return "", err
	}
	if !r.host.IsLeader() {
		return "", fault.ErrNotLeader
	}
	id, err := r.host.CommitVoterChange(ctx, change, RepairCommit{Document: doc,
		ExpectedVersion: fresh.Active.Version, Plan: plan})
	if err == nil {
		r.host.WakeOperator()
		return id, nil
	}
	return r.resolveVoterApproval(ctx, doc, plan, err)
}

func (r *Scheduler) recheckVoterChange(ctx context.Context,
	in schedulingapi.ScheduleInput, change topology.VoterChange) (votercheck.Input, error) {
	if err := ctx.Err(); err != nil {
		return votercheck.Input{}, err
	}
	views, err := r.host.Views(ctx)
	latest, target, active := views.Latest, views.Target, views.Active
	if err != nil {
		return votercheck.Input{}, err
	}
	if !reflect.DeepEqual(latest, in.Latest) || !reflect.DeepEqual(active, in.Active) ||
		!reflect.DeepEqual(target, active) ||
		!reflect.DeepEqual(latest, active) {
		return votercheck.Input{}, fault.ErrVersionConflict
	}
	if !r.host.IsLeader() || !r.enabled.Load() {
		return votercheck.Input{}, fault.ErrNotLeader
	}
	op, err := r.host.CurrentOperator(ctx)
	if err != nil {
		return votercheck.Input{}, err
	}
	if op.Found && (op.Operator.Status.State == topology.OperatorFailed &&
		op.Operator.Status.Version == latest.Version ||
		!op.Operator.Status.State.Terminal()) {
		return votercheck.Input{}, fault.ErrOperatorAuthorization
	}
	if err := r.host.RefreshObservations(ctx); err != nil {
		return votercheck.Input{}, err
	}
	status, err := r.host.GlobalStatus(ctx, active)
	if err != nil {
		return votercheck.Input{}, err
	}
	fresh := votercheck.Input{Active: active, Metrics: r.host.Observations(), Status: status,
		Held: r.heldIdentities()}
	if why := votercheck.Check(fresh, change); why != "" {
		return votercheck.Input{}, fault.New(fault.OperatorAuthorization, why, nil)
	}
	// A collector leadership change invalidates the callback's source epoch
	// even
	// if the same node regained leadership before the callback returned.
	if fresh.Metrics.CollectorRunID != in.Metrics.CollectorRunID ||
		fresh.Metrics.ObserverInstanceID != in.Metrics.ObserverInstanceID {
		return votercheck.Input{}, fault.ErrNotLeader
	}
	return fresh, nil
}

func (r *Scheduler) resolveVoterApproval(ctx context.Context, doc topology.Document,
	plan topology.Plan, cause error) (topology.OperatorID, error) {
	// Resolve a committed approval whose response was lost. Never retry a role
	// mutation by approving arbitrary Latest, or substitute another candidate.
	views, readErr := r.host.Views(ctx)
	latest, target := views.Latest, views.Target
	if readErr == nil && reflect.DeepEqual(latest, doc) && reflect.DeepEqual(target, doc) {
		op, opErr := r.host.CurrentOperator(ctx)
		if opErr == nil && op.Found && op.Operator.Status.Version == doc.Version &&
			reflect.DeepEqual(op.Operator.Plan, plan) {
			r.host.WakeOperator()
			return op.Operator.Status.ID, nil
		}
	}
	return "", cause
}
