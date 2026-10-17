package topology

func (s *voterPlanState) replicaCapacity() int {
	if s.groupID == MetaGroupID {
		return s.maximum + s.metaDecommissioning()
	}
	return s.maximum
}

// FI_META_SURGE: final documents and voter counts keep their hard limits. Only
// exact still-native decommissioned identities provide temporary learner seats.
func (s *voterPlanState) metaDecommissioning() int {
	count := 0
	for _, members := range []map[uint64]struct{}{s.voters, s.learners} {
		for id := range members {
			old, known := s.base.planReplicaKey(MetaGroupID, s.desired.MetaGroup(), id)
			// classifyMoves always removes this exact native identity: no
			// desired member on its node can retain or demote it instead.
			if known && s.desired.MemberID(old.NodeName) == 0 {
				count++
			}
		}
	}
	return count
}

func (s *voterPlanState) prepareBeforeDecommission(group *Group) error {
	if s.groupID != MetaGroupID || s.metaDecommissioning() == 0 {
		return nil
	}
	hosts := make(map[string]bool)
	for _, members := range []map[uint64]struct{}{s.voters, s.learners} {
		for id := range members {
			old, known := s.base.planReplicaKey(MetaGroupID, group, id)
			if known {
				hosts[old.NodeName] = true
			}
		}
	}
	// Admission supplies evacuation destinations before removal waits on loads.
	// Same-node readmission still retires its old owner through its own protocol.
	for _, replica := range sortedReplicas(group.Replicas) {
		if !hosts[replica.NodeName] {
			if err := s.ensureLearner(replica); err != nil {
				return err
			}
		}
	}
	return nil
}
