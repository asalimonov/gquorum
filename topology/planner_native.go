package topology

// Membership comes only from Native. A cached observation may describe another
// applied position; preserve it solely for liveness/leadership planning.
func plannerObservations(input PlanInput, desired Document, groups []string) (map[string]GroupStatus, error) {
	observed := make(map[string]GroupStatus)
	for _, id := range groups {
		status := GroupStatus{GroupID: id}
		if sample := input.Groups[id].Observation; sample != nil {
			status = sample.Clone()
			if status.GroupID != "" && status.GroupID != id {
				return nil, planUnsafef("observation for group %q identifies %q", id, status.GroupID)
			}
		}
		status.GroupID = id
		state, exists := input.Native[id]
		if exists {
			if state.GroupID != id || state.ReplicaID == 0 || state.Applied == 0 {
				return nil, planUnsafef("group %q native read has no exact applied reporter", id)
			}
			if _, known := input.planReplicaKey(id, desired.Group(id), state.ReplicaID); !known {
				return nil, planUnsafef("group %q native reporter %d has no retained identity", id, state.ReplicaID)
			}
		}
		status.Found, status.ConfState = exists, state.Clone().ConfState
		observed[id] = status
	}
	return observed, nil
}
