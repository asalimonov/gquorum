package metaengine

import "github.com/asalimonov/gquorum/topology"

// ProcessIdentity resolves one currently admitted process at a single FSM
// boundary. Pending admission takes precedence over a former member's session.
// An activated member's node lock binds its current process after a restart.
func (m *Machine) ProcessIdentity(name string) (topology.Identity, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return scopedProcessIdentity(m.scoped, name)
}
