package metaservice

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// Requested selectors only. Resolved successors, counters and generated clocks
// are added to the accepted record/document after replay lookup, never here.
type mutationInput struct {
	GroupID    string `json:"group_id,omitempty"`
	NodeName   string `json:"node_name,omitempty"`
	PreviousID uint64 `json:"previous_id,omitempty"`
	Version    uint64 `json:"version,omitempty"`
	Value      any    `json:"value,omitempty"`
}

type mutationFingerprint struct {
	Actor   string          `json:"actor"`
	Comment string          `json:"comment"`
	Origin  topology.Origin `json:"origin"`
	Input   mutationInput   `json:"input"`
}

func (g *Group) mutationIntent(o MutationOptions, policy mutationPolicy) (*metaengine.Intent, error) {
	if policy.Kind == "" {
		return nil, nil // Direct computeMutation fixtures; ordinary methods supply a kind.
	}
	input, err := normalizeMutationInput(policy.Input, g.rules)
	if err != nil {
		return nil, err
	}
	actor, comment := o.Actor, o.Comment
	if actor == "" {
		actor = g.currentIdentity().NodeName
	}
	if comment == "" {
		comment = policy.Comment
	}
	intent, err := metaengine.NewIntent(policy.Kind, mutationFingerprint{
		Actor: actor, Comment: comment, Origin: topology.OriginOperator, Input: input})
	if err != nil {
		return nil, err
	}
	intent.GroupID, intent.NodeName, intent.PreviousID = input.GroupID, input.NodeName, input.PreviousID
	return &intent, nil
}

func normalizeMutationInput(input mutationInput, rules topology.Rules) (mutationInput, error) {
	if input.GroupID != "" && topology.ValidateID(input.GroupID) != nil ||
		strings.ContainsRune(input.NodeName, 0) {
		return mutationInput{}, fault.ErrInvalid
	}
	owned := input
	value, err := normalizeMutationValue(input.Value, rules)
	owned.Value = value
	return owned, err
}

func normalizeMutationValue(value any, rules topology.Rules) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case topology.Node:
		if strings.TrimSpace(v.Name) == "" || strings.ContainsRune(v.Name, 0) ||
			strings.TrimSpace(string(v.Type)) == "" {
			return nil, fault.ErrInvalid
		}
		v.State, v.CreatedAt, v.UpdatedAt = topology.NodeActive, time.Time{}, time.Time{}
		v.Labels = maps.Clone(v.Labels)
		return v, topology.ValidateLabels(v.Labels, topology.NodeLabels)
	case topology.PlacementRule:
		return normalizedIntentPlacement(v, rules)
	case GroupSpec:
		return normalizedIntentGroup(v, rules)
	case topology.HostingRule:
		v.Nodes = slices.Sorted(slices.Values(v.Nodes))
		return v, v.Validate()
	case topology.VoterConfig:
		return v, v.Validate(1, max(rules.Limits.MaxNodes, rules.Limits.MaxReplicas))
	case topology.LearnerPin:
		if v.Replica.ReplicaID == 0 || strings.TrimSpace(v.Replica.NodeName) == "" {
			return nil, fault.ErrInvalid
		}
		return v, nil
	case topology.ReplicaRoleChange:
		if v.Role != topology.ReplicaVoter && v.Role != topology.ReplicaLearner || strings.TrimSpace(v.NodeName) == "" {
			return nil, fault.ErrInvalid
		}
		return v, nil
	case topology.ChangeAudit:
		v.StagedAt = time.Time{}
		if strings.TrimSpace(v.Reason) == "" || strings.TrimSpace(v.StagedBy) == "" {
			return nil, fault.ErrInvalid
		}
		v.Reason = strings.TrimSpace(v.Reason)
		return v, nil
	default:
		return nil, fault.New(fault.Invalid, "unsupported typed mutation arguments", nil)
	}
}

func normalizedIntentPlacement(rule topology.PlacementRule, rules topology.Rules) (topology.PlacementRule, error) {
	owned := rule
	owned.AllowedNodeTypes = slices.Sorted(slices.Values(rule.AllowedNodeTypes))
	owned.NodeSelector = maps.Clone(rule.NodeSelector)
	if owned.MinReplicas < 1 || owned.MaxReplicas < owned.MinReplicas || owned.MaxReplicas > rules.Limits.MaxReplicas {
		return topology.PlacementRule{}, fault.ErrInvalid
	}
	for i, value := range owned.AllowedNodeTypes {
		if i > 0 && value == owned.AllowedNodeTypes[i-1] || strings.TrimSpace(string(value)) == "" {
			return topology.PlacementRule{}, fault.ErrInvalid
		}
	}
	return owned, topology.ValidateSelector(owned.NodeSelector)
}

func normalizedIntentGroup(spec GroupSpec, rules topology.Rules) (GroupSpec, error) {
	if spec.Nodes != nil && spec.Hosting != nil || topology.ValidateLoadID(spec.ID) != nil {
		return GroupSpec{}, fault.ErrInvalid
	}
	owned := spec
	owned.Labels, owned.Nodes = maps.Clone(spec.Labels), nil
	if err := topology.ValidateLabels(owned.Labels, topology.GroupLabels); err != nil {
		return GroupSpec{}, err
	}
	if err := topology.ValidateStorageClass(owned.Storage); err != nil {
		return GroupSpec{}, err
	}
	policy := topology.DefaultVoterConfig()
	if spec.VoterConfig != nil {
		policy = *spec.VoterConfig
	}
	owned.VoterConfig = &policy
	hosting := topology.HostingRule{Mode: topology.HostingAllEligible}
	if spec.Hosting != nil {
		hosting = *spec.Hosting
	}
	if len(spec.Nodes) != 0 {
		hosting = topology.HostingRule{Mode: topology.HostingExplicit, Nodes: spec.Nodes}
	}
	hosting.Nodes = slices.Sorted(slices.Values(hosting.Nodes))
	owned.Hosting = &hosting
	if err := hosting.Validate(); err != nil {
		return GroupSpec{}, err
	}
	lo, hi := 1, rules.Limits.MaxReplicas
	if spec.Placement != nil {
		placement, err := normalizedIntentPlacement(*spec.Placement, rules)
		if err != nil {
			return GroupSpec{}, err
		}
		owned.Placement = &placement
		lo, hi = placement.MinReplicas, placement.MaxReplicas
	}
	return owned, policy.Validate(lo, hi)
}

// Replay carries the retained acceptance, without recomputing its placement or
// automatically approving an old version after a lost response.
func mutationReplayResult(result metaengine.ApplyResult) MutationResult {
	return MutationResult{Document: result.Doc, OperatorID: result.OperatorID,
		OperationID: topology.OperationID(result.OperatorID),
		RebasedTo:   result.RebasedTo, Placement: result.Placement}
}
