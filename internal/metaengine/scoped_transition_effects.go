package metaengine

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) recordScopedTransitionEffect(index uint64, op *topology.Operation,
	seq int, at time.Time) error {
	step := op.Plan.Steps[seq-1]
	s.recordScopedAdmissionStep(op.Status.ID, step)
	if step.Kind != topology.StepPromoteVoter && step.Kind != topology.StepDemoteVoter {
		return nil
	}
	record := scopedConsumedAuthority(op, step, s.indexes, s.authorityCatalog())
	if record == nil || index <= record.Grant.Key.Revision {
		return fault.ErrAuthorityPending
	}
	action := record.Grant.Action
	from, to := topology.ReplicaLearner, topology.ReplicaVoter
	if action.Kind == topology.StepDemoteVoter {
		from, to = to, from
	}
	change := topology.RoleChangeRecord{OperationID: action.OperationID, Version: action.Version,
		Attempt: action.Attempt, StepSeq: action.StepSeq, GroupID: action.GroupID, NodeName: action.NodeName,
		ReplicaID: action.ReplicaID, From: from, To: to, At: at, Index: index}
	position := slices.IndexFunc(s.VoterSwitches, func(row topology.VoterSwitchState) bool {
		return row.GroupID == step.GroupID
	})
	if position < 0 {
		s.VoterSwitches = append(s.VoterSwitches, topology.VoterSwitchState{GroupID: step.GroupID})
		position = len(s.VoterSwitches) - 1
	}
	s.VoterSwitches[position] = recordRoleChange(s.VoterSwitches[position].Clone(), change)
	return nil
}

func (s *scopedState) recordScopedAdmissionStep(id topology.OperationID, step topology.Step) {
	if step.GroupID != topology.MetaGroupID {
		return
	}
	for i := range s.Admissions {
		a := &s.Admissions[i]
		if topology.OperationID(a.OperationID) != id || !admissionStep(*a, step) || a.State.terminal() {
			continue
		}
		switch step.Kind {
		case topology.StepAddLearner:
			a.State = AdmissionLearner
		case topology.StepPromoteVoter:
			a.State = AdmissionPromoted
		default: // Readiness is authenticated separately; demotion does not erase its consumed promotion.
		}
	}
}
