package topology

// replacementOrderSafe simulates every intermediate voter set. Live observations
// select a feasible plan; receivers independently recheck safety before effects.
func replacementOrderSafe(status GroupStatus, additions, demotions, removals []Replica, shrinkFirst bool, maximum int) bool {
	voters := idSet(status.ConfState.Voters)
	quorum := func() bool {
		alive := 0
		for id := range voters {
			if status.LiveVoters[id] {
				alive++
			}
		}
		return len(voters) > 0 && alive >= QuorumOf(len(voters))
	}
	if !quorum() {
		return false
	}
	shrink := func() bool {
		for _, list := range [][]Replica{demotions, removals} {
			for _, r := range list {
				if _, voter := voters[r.ReplicaID]; !voter {
					continue
				}
				if validateDestructiveBoundary(status.GroupID, len(voters)) != nil {
					return false
				}
				delete(voters, r.ReplicaID)
				if !quorum() {
					return false
				}
			}
		}
		return true
	}
	grow := func() bool {
		for _, r := range additions {
			if r.Role != ReplicaVoter {
				continue
			}
			voters[r.ReplicaID] = struct{}{}
			if len(voters) > maximum || !quorum() {
				return false
			}
		}
		return true
	}
	if shrinkFirst {
		return shrink() && grow()
	}
	return grow() && shrink()
}

func liveTransferVoters(voters map[uint64]struct{}, status GroupStatus) map[uint64]struct{} {
	if !status.HealthKnown {
		return voters
	}
	result := make(map[uint64]struct{}, len(voters))
	for id := range voters {
		if status.LiveVoters[id] {
			result[id] = struct{}{}
		}
	}
	return result
}

// bindDemotionGuards includes an exact destination even when the victim is not
// currently leader: it can win an election between approval and demotion.
func bindDemotionGuards(plan *Plan, desired Document, observed map[string]GroupStatus, preferredMeta uint64) error {
	voters := make(map[string]map[uint64]struct{}, len(observed))
	for id, st := range observed {
		voters[id] = idSet(st.ConfState.Voters)
	}
	for i := range plan.Steps {
		s := &plan.Steps[i]
		members := voters[s.GroupID]
		if members == nil {
			members = make(map[uint64]struct{})
			voters[s.GroupID] = members
		}
		switch s.Kind {
		case StepCreateFoundingGroup:
			for _, r := range desired.VotersOf(s.GroupID) {
				members[r.ReplicaID] = struct{}{}
			}
		case StepPromoteVoter:
			members[s.ReplicaID] = struct{}{}
		case StepDemoteVoter:
			preferred := uint64(0)
			if s.GroupID == MetaGroupID {
				preferred = preferredMeta
			}
			r, ok := transferTarget(desired.Group(s.GroupID), liveTransferVoters(members, observed[s.GroupID]), s.ReplicaID, preferred)
			if !ok {
				return planUnsafef("group %q has no retained voter for the demotion guard", s.GroupID)
			}
			s.TransferNodeName, s.TransferReplicaID = r.NodeName, r.ReplicaID
			delete(members, s.ReplicaID)
		case StepRemoveReplica:
			delete(members, s.ReplicaID)
		}
	}
	return nil
}
