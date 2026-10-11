package metaengine

import "github.com/asalimonov/gquorum/topology"

// ProcessIdentity resolves one currently admitted process at a single FSM
// boundary. Pending admission takes precedence over a former member's session.
// An activated member's node lock binds its current process after a restart.
func (m *Machine) ProcessIdentity(name string) (topology.Identity, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	member := m.target.MemberID(name)
	if member == 0 {
		member = m.active.MemberID(name)
	}
	id := topology.Identity{NodeName: name, ClusterID: m.target.ClusterID, MemberID: member}
	if member == 0 {
		return id, false
	}
	if admission := m.liveAdmissionForNodeLocked(name, ""); admission != nil && admission.MemberID == member {
		id.InstanceID = admission.InstanceID
		return id, id.InstanceID != ""
	}
	if m.active.MemberID(name) == member {
		if session := m.sessions[m.locks["node/"+name]]; session != nil && session.NodeName == name {
			id.InstanceID = session.InstanceID
		}
	}
	return id, id.InstanceID != ""
}
