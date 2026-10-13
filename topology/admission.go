package topology

// ValidAdmissionSequence recognizes exactly the role-specific learner-first
// sequence for one meta identity. Preparation alone never authorizes admission.
func ValidAdmissionSequence(steps []Step, nodeName string, replicaID uint64, role ReplicaRole) bool {
	if nodeName == "" || replicaID == 0 || role != ReplicaVoter && role != ReplicaLearner {
		return false
	}
	want := []StepKind{StepPrepareReplica, StepAddLearner}
	if role == ReplicaVoter {
		want = append(want, StepWaitCaughtUp, StepPromoteVoter)
	}
	for i := 0; i+len(want) <= len(steps); i++ {
		match := true
		for j, kind := range want {
			s := steps[i+j]
			match = match && s.Kind == kind && s.GroupID == MetaGroupID && s.NodeName == nodeName && s.ReplicaID == replicaID
		}
		if match {
			// A learner admission must not hide later promotion or removal of
			// that identity. Full plan validation enforces other shape rules.
			for j, s := range steps {
				if (j < i || j >= i+len(want)) && s.GroupID == MetaGroupID && s.NodeName == nodeName && s.ReplicaID == replicaID {
					if role == ReplicaVoter && s.Kind == StepTransferLeader {
						continue
					}
					return false
				}
			}
			return true
		}
	}
	return false
}
