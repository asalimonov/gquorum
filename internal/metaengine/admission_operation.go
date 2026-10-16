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
	// Task 78 reads the same fields from scopedState.Admissions.
	for _, admission := range m.admissions {
		if admission.NodeName == name && admission.MemberID == member && admission.State == AdmissionCompleted {
			return true
		}
	}
	return false
}

// AdmissionOperation identifies the metadata work that can bind a fresh process.
// Its pinned approval, rather than the latest whole document, defines the plan.
// The replicated binding rechecks this observation before granting any identity.
func (m *Machine) AdmissionOperation(name string, member uint64) (topology.OperationID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Task 78: replace this schema-3 adapter with scopedState.admissionOperation.
	// Schema 3 has a single current execution and cannot retain scoped owners.
	if member == 0 || m.target.MemberID(name) != member {
		return "", false
	}
	for _, op := range m.operators {
		approved, ok := m.documentLocked(op.Version)
		if ok && op.Version == m.target.Version && !op.State.Terminal() &&
			hasAdmissionSequence(op.Steps, approved, name, member) {
			return topology.OperationID(op.ID), true
		}
	}
	return "", false
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
