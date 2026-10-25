package placement

import (
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// Prepare uses owned count-normalized analysis and restores original intent.
//
//revive:disable-next-line:argument-limit Target preparation adds one private scope parameter.
func Prepare(ctx Context, view topology.ReadView, edited topology.Document, rules topology.Rules,
	at time.Time) (topology.PreparedPlacement, error) {
	allowed, err := allowances(ctx, view.Target, view.Latest, edited)
	if err != nil {
		return topology.PreparedPlacement{}, err
	}
	if len(allowed) == 0 {
		return topology.PreparePlacement(view, edited, rules, at)
	}
	normalizedView := view
	normalizedView.Latest = normalized(view.Latest, allowed)
	normalizedView.Target = normalized(view.Target, allowed)
	result, err := topology.PreparePlacement(normalizedView, normalized(edited, allowed), rules, at)
	if err != nil {
		restoreReport(&result.Report, edited)
		return result, err
	}
	restorePolicies(&result.Document, edited)
	restoreReport(&result.Report, edited)
	if err := Validate(ctx, view.Target, view.Latest, result.Document, rules); err != nil {
		return topology.PreparedPlacement{}, err
	}
	return result, nil
}

// Validate preserves identity/placement checks while admitting only scoped deficits.
//
//revive:disable-next-line:argument-limit The target binds approved, base and desired declarations explicitly.
func Validate(ctx Context, approved, base, desired topology.Document, rules topology.Rules) error {
	allowed, err := allowances(ctx, approved, base, desired)
	if err != nil {
		return err
	}
	old, next := normalized(base, allowed), normalized(desired, allowed)
	if old.ClusterID == "" && old.Version == 0 {
		old.ClusterID = next.ClusterID
	}
	reference := normalized(approved, allowed)
	if reference.ClusterID == "" {
		reference.ClusterID = next.ClusterID
	}
	if err := topology.ValidateHostingCapacityFor(reference, next, rules); err != nil {
		return err
	}
	return topology.ValidatePlacementTransition(reference, old, next)
}

// BuildPlan retains original membership evidence and all ordinary native guards.
func BuildPlan(ctx Context, input topology.PlanInput, desired topology.Document,
	options topology.PlanOptions) (topology.Plan, error) {
	allowed, err := allowances(ctx, input.Approved, input.Approved, desired)
	if err != nil {
		return topology.Plan{}, err
	}
	if len(allowed) == 0 {
		return topology.BuildPlanPreferred(input, desired, options)
	}
	return topology.BuildPlanPreferred(normalizedInput(input, allowed), normalized(desired, allowed), options)
}

// ValidatePlan repeats exact scoped normalization before normal immutable-plan checks.
func ValidatePlan(ctx Context, input topology.PlanInput, desired topology.Document, plan topology.Plan) error {
	allowed, err := allowances(ctx, input.Approved, input.Approved, desired)
	if err != nil {
		return err
	}
	if len(allowed) == 0 {
		return topology.ValidatePlanShape(input, desired, plan)
	}
	return topology.ValidatePlanShape(normalizedInput(input, allowed), normalized(desired, allowed), plan)
}

func normalizedInput(input topology.PlanInput, allowed map[string]allowance) topology.PlanInput {
	result := input
	result.Approved = normalized(input.Approved, allowed)
	result.Groups = make(map[string]topology.GroupProgress, len(input.Groups))
	for id, row := range input.Groups {
		owned := row.Clone()
		for _, group := range []*topology.Group{owned.Desired, owned.Approved, owned.Converged} {
			if group == nil {
				continue
			}
			doc := topology.Document{Placement: input.Approved.Placement, Groups: []topology.Group{*group}}
			normal := normalized(doc, allowed)
			*group = normal.Groups[0]
		}
		result.Groups[id] = owned
	}
	return result
}

// CanRelease requires proven original-policy satisfaction or completed retirement.
// The FSM owns durable release; completion of data import alone is insufficient.
func CanRelease(ctx Context, progress topology.GroupProgress, target topology.Document) bool {
	policy, found := ctx.policies[progress.GroupID]
	if !found || policy.PolicyReleased || ctx.cluster != target.ClusterID {
		return false
	}
	group := target.Group(progress.GroupID)
	if group == nil {
		return progress.ConvergedVersion > 0 && progress.Converged == nil
	}
	if progress.Converged == nil || progress.ConvergedVersion == 0 || !reflect.DeepEqual(progress.Converged,
		group) {
		return false
	}
	minimum, _ := target.GroupBounds(group.ID)
	if group.ID == topology.MetaGroupID {
		minimum = 1
	}
	if group.VoterConfig != nil {
		minimum = max(minimum, group.VoterConfig.TargetVoters)
	}
	if len(target.VotersOf(group.ID)) < minimum {
		return false
	}
	eligible, err := eligibleCount(target, group.ID)
	if err != nil || eligible < minimum {
		return false
	}
	voters := make([]topology.ReplicaKey, 0, len(group.Replicas))
	for _, replica := range group.Replicas {
		if replica.Role == topology.ReplicaVoter {
			voters = append(voters, topology.ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID})
		}
	}
	return topology.ValidateVoterPlacement(target, group.ID, voters) == nil
}
