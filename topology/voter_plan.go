package topology

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
			r, ok := transferTarget(desired.Group(s.GroupID), liveTransferVoters(members, observed[s.GroupID]),
				s.ReplicaID, preferred)
			if !ok {
				return planUnsafef("group %q has no retained voter for the demotion guard", s.GroupID)
			}
			s.TransferNodeName, s.TransferReplicaID = r.NodeName, r.ReplicaID
			delete(members, s.ReplicaID)
		case StepRemoveReplica:
			delete(members, s.ReplicaID)
		default: // Other steps leave voters and transfer guards unchanged.
		}
	}
	return nil
}
