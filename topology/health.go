package topology

// ObservedGroupHealth evaluates the declared Active voters against one node
// health snapshot. Unknown observations remain nullable rather than measured zero.
type ObservedGroupHealth struct {
	State            GroupHealth `json:"state"`
	Source           string      `json:"source"`
	HealthyVoters    *int        `json:"healthy_voters"`
	FailureTolerance *int        `json:"failure_tolerance"`
	BelowMinReplicas *bool       `json:"below_min_replicas"`
}
type GroupHealthEvaluation struct {
	GroupID        string              `json:"group_id"`
	Voters         int                 `json:"voters"`
	Quorum         int                 `json:"quorum"`
	ObservedHealth ObservedGroupHealth `json:"observed_health"`
}

func (h GroupHealth) QuorumLost() bool { return h == GroupQuorumLost }
func valuePtr[T any](v T) *T           { return &v }
func EvaluateGroupHealth(doc Document, id string, health map[string]Health) GroupHealthEvaluation {
	voters := doc.VotersOf(id)
	out := GroupHealthEvaluation{GroupID: id, Voters: len(voters), Quorum: QuorumOf(len(voters)), ObservedHealth: ObservedGroupHealth{State: GroupUnknown, Source: "none"}}
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
func EvaluateDataGroupHealth(doc Document, health map[string]Health) []GroupHealthEvaluation {
	var out []GroupHealthEvaluation
	for _, group := range doc.LoadGroups() {
		out = append(out, EvaluateGroupHealth(doc, group.ID, health))
	}
	return out
}
func EvaluateAllGroupHealth(doc Document, health map[string]Health) []GroupHealthEvaluation {
	var out []GroupHealthEvaluation
	if doc.MetaGroup() != nil {
		out = append(out, EvaluateGroupHealth(doc, MetaGroupID, health))
	}
	return append(out, EvaluateDataGroupHealth(doc, health)...)
}
