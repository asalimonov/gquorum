package metaengine

import "github.com/asalimonov/gquorum/topology"

// AdmissionBasis checks exact proven metadata membership and current approved
// placement. The returned operation, when nonempty, is the retained founding
// proof; success alone does not bind a process or grant a session.
func (m *Machine) AdmissionBasis(identity topology.Identity) (topology.OperationID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Task 78: replace this schema-3 adapter with scopedState.admissionBasis.
	if !admissionMemberMatches(m.target, m.active.MetaGroup(), identity) {
		return "", false
	}
	id := topology.MakeOperatorID(1, 1)
	if m.foundingMemberLocked(identity.NodeName, identity.MemberID) {
		return topology.OperationID(id), true
	}
	return "", true
}

func (sm *topologySM) foundingMemberLocked(name string, member uint64) bool {
	op := sm.operators[topology.MakeOperatorID(1, 1)]
	doc, ok := sm.documentLocked(1)
	return ok && op != nil && op.State == topology.OperatorSucceeded && doc.Origin == topology.OriginFounding &&
		doc.MemberID(name) == member
}

func (sm *topologySM) validFoundingBinding(record AdmissionRecord) bool {
	identity := topology.Identity{ClusterID: sm.target.ClusterID, NodeName: record.NodeName, MemberID: record.MemberID}
	if record.OperatorID != topology.MakeOperatorID(1, 1) ||
		!sm.foundingMemberLocked(record.NodeName, record.MemberID) ||
		!admissionMemberMatches(sm.target, sm.active.MetaGroup(), identity) {
		return false
	}
	for _, prior := range sm.admissions {
		if prior.NodeName == record.NodeName && prior.MemberID == record.MemberID {
			return false // A retained restart uses completed membership proof, not another seed binding.
		}
	}
	return true
}

func admissionMemberMatches(target topology.Document, proven *topology.Group, identity topology.Identity) bool {
	node := target.Node(identity.NodeName)
	return identity.MemberID != 0 && target.ClusterID == identity.ClusterID &&
		node != nil && node.State.Places() && target.MemberID(identity.NodeName) == identity.MemberID &&
		groupHasMember(proven, identity.NodeName, identity.MemberID)
}

func (s *scopedState) admissionBasis(identity topology.Identity) (topology.OperationID, bool) {
	row := s.indexes.groups[topology.MetaGroupID]
	if row == nil || row.SourceVersion == 0 || !admissionMemberMatches(s.Target, row.Declaration, identity) {
		return "", false
	}
	id := s.indexes.first[1]
	if s.foundingMember(id, identity.NodeName, identity.MemberID) {
		return id, true
	}
	return "", true
}

func (s *scopedState) foundingMember(id topology.OperationID, name string, member uint64) bool {
	op := s.indexes.operations[id]
	if op == nil || op.Status.Version != 1 {
		return false
	}
	doc := s.indexes.documents[1]
	if member == 0 || doc.Origin != topology.OriginFounding || doc.MemberID(name) != member {
		return false
	}
	for _, block := range op.Status.Blocks {
		if block.GroupID == topology.MetaGroupID {
			return block.State == topology.OperationSucceeded
		}
	}
	return false
}
