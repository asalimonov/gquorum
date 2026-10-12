package scheduling

import (
	"context"
	"reflect"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/votercheck"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/topology"
)

// commitVoterChange rechecks after the callback and after planning. Neither a
// cached policy decision nor a successful preview authorizes membership effects.
func (r *Driver) commitVoterChange(ctx context.Context, in policy.ScheduleInput, change topology.VoterChange) (topology.OperatorID, error) {
	recheck := func() (votercheck.Input, error) {
		if err := ctx.Err(); err != nil {
			return votercheck.Input{}, err
		}
		latest, target, active, err := r.host.Views(ctx)
		if err != nil {
			return votercheck.Input{}, err
		}
		if !reflect.DeepEqual(latest, in.Latest) || !reflect.DeepEqual(active, in.Active) || !reflect.DeepEqual(target, active) || !reflect.DeepEqual(latest, active) {
			return votercheck.Input{}, fault.ErrVersionConflict
		}
		if !r.host.IsLeader() || !r.enabled.Load() {
			return votercheck.Input{}, fault.ErrNotLeader
		}
		op, exists, err := r.host.CurrentOperator(ctx)
		if err != nil {
			return votercheck.Input{}, err
		}
		if exists && (!op.Status.State.Terminal() || op.Status.State == topology.OperatorFailed && op.Status.Version == latest.Version) {
			return votercheck.Input{}, fault.ErrOperatorAuthorization
		}
		if err := r.host.RefreshObservations(ctx); err != nil {
			return votercheck.Input{}, err
		}
		status, err := r.host.GlobalStatus(ctx, active)
		if err != nil {
			return votercheck.Input{}, err
		}
		fresh := votercheck.Input{Active: active, Metrics: r.host.Observations(), Status: status, Held: r.heldIdentities()}
		if why := votercheck.Check(fresh, change); why != "" {
			return votercheck.Input{}, fault.New(fault.OperatorAuthorization, why, nil)
		}
		// A collector leadership change invalidates the callback's source epoch even
		// if the same node regained leadership before the callback returned.
		if fresh.Metrics.CollectorRunID != in.Metrics.CollectorRunID || fresh.Metrics.ObserverInstanceID != in.Metrics.ObserverInstanceID {
			return votercheck.Input{}, fault.ErrNotLeader
		}
		return fresh, nil
	}
	fresh, err := recheck()
	if err != nil {
		return "", err
	}
	doc, err := metaengine.VoterChangeDocument(fresh.Active, change, r.now().UTC(), "auto-voter-policy")
	if err != nil {
		return "", err
	}
	plan, err := r.host.PlanFor(ctx, doc)
	if err != nil {
		return "", err
	}
	if plan.IsEmpty() {
		return "", fault.ErrOperatorAuthorization
	}
	if _, err := recheck(); err != nil {
		return "", err
	}
	if !r.host.IsLeader() {
		return "", fault.ErrNotLeader
	}
	id, err := r.host.CommitVoterChange(ctx, change, doc, fresh.Active.Version, plan)
	if err == nil {
		r.host.WakeOperator()
		return id, nil
	}
	// Resolve a committed approval whose response was lost. Never retry a role
	// mutation by approving arbitrary Latest, or substitute another candidate.
	latest, target, _, readErr := r.host.Views(ctx)
	if readErr == nil && reflect.DeepEqual(latest, doc) && reflect.DeepEqual(target, doc) {
		op, exists, opErr := r.host.CurrentOperator(ctx)
		if opErr == nil && exists && op.Status.Version == doc.Version && reflect.DeepEqual(op.Plan, plan) {
			r.host.WakeOperator()
			return op.Status.ID, nil
		}
	}
	return "", err
}
