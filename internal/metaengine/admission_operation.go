package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// HasCompletedAdmission observes completion for an exact durable member. A
// retained restart has a fresh process ID; process exclusivity remains the
// session lock's responsibility. This observation cannot authorize disk loss.
func (m *Machine) HasCompletedAdmission(name string, member uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.ContainsFunc(m.scoped.Admissions, func(a AdmissionRecord) bool {
		return a.NodeName == name && a.MemberID == member && a.State == AdmissionCompleted
	})
}

// AdmissionOperation identifies the metadata work that can bind a fresh process.
// Its pinned approval, rather than the latest whole document, defines the plan.
// The replicated binding rechecks this observation before granting any identity.
func (m *Machine) AdmissionOperation(name string, member uint64) (topology.OperationID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	op := m.scoped.admissionOperation(name, member)
	if op == nil || !hasAdmissionSequence(op.Plan.Steps,
		m.scoped.indexes.documents[op.Status.Version], name, member) {
		return "", false
	}
	return op.Status.ID, true
}

func (s *scopedState) admissionOperation(name string, member uint64) *topology.Operation {
	meta := s.indexes.groups[topology.MetaGroupID]
	if meta == nil || member == 0 || s.Target.MemberID(name) != member {
		return nil
	}
	op := s.indexes.operations[meta.Owner]
	if op == nil {
		return nil
	}
	approved := s.indexes.documents[op.Status.Version]
	if approved.MemberID(name) != member ||
		!slices.ContainsFunc(op.Status.Blocks, func(block topology.BlockStatus) bool {
			return block.GroupID == topology.MetaGroupID && !block.State.Terminal()
		}) {
		return nil
	}
	return op
}
