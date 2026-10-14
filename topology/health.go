package topology

import (
	"maps"
	"time"

	"github.com/asalimonov/gquorum/internal/collection"
)

// ObservedGroupHealth evaluates the declared Active voters against one node
// health snapshot. Unknown observations remain nullable rather than measured
// zero.
type ObservedGroupHealth struct {
	State            GroupHealth `json:"state"`
	Source           string      `json:"source"`
	HealthyVoters    *int        `json:"healthy_voters"`
	FailureTolerance *int        `json:"failure_tolerance"`
	BelowMinReplicas *bool       `json:"below_min_replicas"`
}

// GroupHealthEvaluation compares declared voter count and quorum with observed
// node health.
type GroupHealthEvaluation struct {
	GroupID        string              `json:"group_id"`
	Voters         int                 `json:"voters"`
	Quorum         int                 `json:"quorum"`
	ObservedHealth ObservedGroupHealth `json:"observed_health"`
}

// Clone copies optional health measurements into owned values.
func (e GroupHealthEvaluation) Clone() GroupHealthEvaluation {
	if e.ObservedHealth.HealthyVoters != nil {
		e.ObservedHealth.HealthyVoters = valuePtr(*e.ObservedHealth.HealthyVoters)
	}
	if e.ObservedHealth.FailureTolerance != nil {
		e.ObservedHealth.FailureTolerance = valuePtr(*e.ObservedHealth.FailureTolerance)
	}
	if e.ObservedHealth.BelowMinReplicas != nil {
		e.ObservedHealth.BelowMinReplicas = valuePtr(*e.ObservedHealth.BelowMinReplicas)
	}
	return e
}

// HealthObservation is an observer-local snapshot of one Active document and
// one node-health sample. At and Version expose its age and document scope.
// It describes declared voter availability, not a linearizable Raft read.
type HealthObservation struct {
	Version uint64                  `json:"version"`
	At      time.Time               `json:"at"`
	Nodes   map[string]Health       `json:"nodes"`
	Groups  []GroupHealthEvaluation `json:"groups"`
}

// Clone copies the node health map and all nested group measurements.
func (s HealthObservation) Clone() HealthObservation {
	s.Nodes = maps.Clone(s.Nodes)
	if s.Groups != nil {
		owned := make([]GroupHealthEvaluation, len(s.Groups))
		for i, g := range s.Groups {
			owned[i] = g.Clone()
		}
		s.Groups = owned
	}
	return s
}

// QuorumLost reports an observed loss of voter majority.
func (h GroupHealth) QuorumLost() bool { return h == GroupQuorumLost }
func valuePtr[T any](v T) *T           { return &v }

// EvaluateGroupHealth evaluates declared voters against a node-health snapshot,
// preserving unknowns.
func EvaluateGroupHealth(doc Document, id string, health map[string]Health) GroupHealthEvaluation {
	voters := doc.VotersOf(id)
	out := GroupHealthEvaluation{
		GroupID:        id,
		Voters:         len(voters),
		Quorum:         QuorumOf(len(voters)),
		ObservedHealth: ObservedGroupHealth{State: GroupUnknown, Source: "none"},
	}
	if health == nil {
		return out
	}
	out.ObservedHealth.Source = "observed"
	alive := 0
	for _, r := range voters {
		observed, exists := health[r.NodeName]
		if !exists || observed != HealthAlive && observed != HealthDead {
			return out
		}
		if observed == HealthAlive {
			alive++
		}
	}
	out.ObservedHealth.HealthyVoters = valuePtr(alive)
	out.ObservedHealth.FailureTolerance = valuePtr(max(alive-out.Quorum, 0))
	switch {
	case alive < out.Quorum:
		out.ObservedHealth.State = GroupQuorumLost
	case alive < out.Voters:
		out.ObservedHealth.State = GroupDegraded
	default:
		out.ObservedHealth.State = GroupHealthy
	}
	if id != MetaGroupID {
		lo, _ := doc.GroupBounds(id)
		out.ObservedHealth.BelowMinReplicas = valuePtr(alive < lo)
	}
	return out
}

// EvaluateDataGroupHealth evaluates all declared application groups in document
// order.
func EvaluateDataGroupHealth(doc Document, health map[string]Health) []GroupHealthEvaluation {
	groups := doc.LoadGroups()
	out := collection.Slice[GroupHealthEvaluation](len(groups))
	for _, group := range groups {
		out = append(out, EvaluateGroupHealth(doc, group.ID, health))
	}
	return out
}

// EvaluateAllGroupHealth evaluates meta, when present, followed by all
// application groups.
func EvaluateAllGroupHealth(doc Document, health map[string]Health) []GroupHealthEvaluation {
	var out []GroupHealthEvaluation
	if doc.MetaGroup() != nil {
		out = append(out, EvaluateGroupHealth(doc, MetaGroupID, health))
	}
	return append(out, EvaluateDataGroupHealth(doc, health)...)
}
