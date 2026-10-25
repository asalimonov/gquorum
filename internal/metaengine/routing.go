package metaengine

import (
	"strings"

	"github.com/asalimonov/gquorum/topology"
)

// ProcessIdentity resolves one currently admitted process at a single FSM
// boundary. Pending admission takes precedence over a former member's session.
// An activated member's node lock binds its current process after a restart.
func (m *Machine) ProcessIdentity(name string) (topology.Identity, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, found := m.processIdentities[name]
	if !found {
		return topology.Identity{NodeName: name, ClusterID: m.scoped.Target.ClusterID}, false
	}
	return id, id.InstanceID != ""
}

// Process routes are rebuilt at applied graph/session/lock boundaries, never
// persisted. Chunk authorization reads two maps rather than scanning topology,
// admissions, locks and sessions for every fragment.
func indexProcessIdentities(s *scopedState) map[string]topology.Identity {
	routes := processMemberRoutes(s)
	admissions := make(map[string]AdmissionRecord)
	for _, a := range s.Admissions {
		if a.State.terminal() {
			continue
		}
		if _, exists := admissions[a.NodeName]; !exists {
			admissions[a.NodeName] = a
		}
	}
	declared := make(map[string]uint64)
	if meta := s.indexes.groups[topology.MetaGroupID]; meta != nil && meta.Declaration != nil {
		for _, r := range meta.Declaration.Replicas {
			declared[r.NodeName] = r.ReplicaID
		}
	}
	locked := processSessionRoutes(s)
	for name, id := range routes {
		if id.MemberID == 0 {
			continue
		}
		if a, found := admissions[name]; found && a.MemberID == id.MemberID {
			id.InstanceID = a.InstanceID
		} else if declared[name] == id.MemberID {
			id.InstanceID = locked[name]
		}
		routes[name] = id
	}
	return routes
}
func processMemberRoutes(s *scopedState) map[string]topology.Identity {
	routes := make(map[string]topology.Identity)
	if meta := s.Target.MetaGroup(); meta != nil {
		for _, r := range meta.Replicas {
			routes[r.NodeName] = topology.Identity{
				NodeName: r.NodeName, ClusterID: s.Target.ClusterID, MemberID: r.ReplicaID}
		}
	}
	for _, row := range s.Nodes {
		if _, exists := routes[row.NodeName]; !exists {
			routes[row.NodeName] = topology.Identity{
				NodeName: row.NodeName, ClusterID: s.Target.ClusterID, MemberID: row.MemberID}
		}
	}
	return routes
}
func processSessionRoutes(s *scopedState) map[string]string {
	sessions := make(map[string]sessionState, len(s.Sessions))
	for _, session := range s.Sessions {
		sessions[session.SessionID] = session
	}
	locked := make(map[string]string)
	for _, lock := range s.Locks {
		if !strings.HasPrefix(lock.Key, "node/") {
			continue
		}
		name := strings.TrimPrefix(lock.Key, "node/")
		if session, found := sessions[lock.SessionID]; found && session.NodeName == name {
			locked[name] = session.InstanceID
		}
	}
	return locked
}
