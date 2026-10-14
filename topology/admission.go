package topology

const admissionPromotionOffset = 3

// ValidAdmissionSequence recognizes exactly the role-specific learner-first
// sequence for one meta identity. Preparation alone never authorizes admission.
func ValidAdmissionSequence(steps []Step, nodeName string, replicaID uint64, role ReplicaRole) bool {
	if nodeName == "" || replicaID == 0 || role != ReplicaVoter && role != ReplicaLearner {
		return false
	}
	sequence := admissionSequence{steps: steps, nodeName: nodeName, replicaID: replicaID}
	for i := 0; i+1 < len(steps); i++ {
		if !sequence.match(i, StepPrepareReplica) || !sequence.match(i+1, StepAddLearner) {
			continue
		}
		allowed := sequence.allowed(i, role)
		if allowed == nil {
			continue
		}
		return sequence.onlyAllowed(allowed, role)
	}
	return false
}

type admissionSequence struct {
	steps     []Step
	nodeName  string
	replicaID uint64
}

func (s admissionSequence) match(i int, kind StepKind) bool {
	if i < 0 || i >= len(s.steps) {
		return false
	}
	step := s.steps[i]
	return step.Kind == kind && step.GroupID == MetaGroupID &&
		step.NodeName == s.nodeName && step.ReplicaID == s.replicaID
}

func (s admissionSequence) allowed(i int, role ReplicaRole) map[int]bool {
	allowed := map[int]bool{i: true, i + 1: true}
	if role != ReplicaVoter {
		return allowed
	}
	if !s.match(i+2, StepWaitCaughtUp) {
		return nil
	}
	allowed[i+2] = true
	if s.match(i+admissionPromotionOffset, StepPromoteVoter) {
		allowed[i+admissionPromotionOffset] = true
		return allowed
	}
	if !preparatoryWaitPair(s.steps, i+2, s.steps[i+2]) {
		return nil
	}
	for j := i + admissionPromotionOffset; j+1 < len(s.steps); j++ {
		if s.match(j, StepWaitCaughtUp) && s.match(j+1, StepPromoteVoter) {
			allowed[j], allowed[j+1] = true, true
			break
		}
	}
	return allowed
}

func (s admissionSequence) onlyAllowed(allowed map[int]bool, role ReplicaRole) bool {
	for j, step := range s.steps {
		if step.GroupID == MetaGroupID && step.NodeName == s.nodeName && step.ReplicaID == s.replicaID && !allowed[j] &&
			!(role == ReplicaVoter && step.Kind == StepTransferLeader) {
			return false
		}
	}
	return true
}
