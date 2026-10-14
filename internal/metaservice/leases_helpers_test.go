package metaservice

import "time"

// refresh sets (or extends) a session's deadline to now+ttl.
func (m *sessionManager) refresh(sessionID string, now time.Time) {
	m.refreshTTL(sessionID, now, m.ttl)
}

// resetFrom rebuilds the table from the given live session IDs with fresh
// now+ttl deadlines, dropping any deadline not in the set. Called on
// leadership acquisition so this leader does not inherit stale deadlines.
func (m *sessionManager) resetFrom(sessionIDs []string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fresh := make(map[string]time.Time, len(sessionIDs))
	for _, id := range sessionIDs {
		fresh[id] = now.Add(m.ttl)
	}
	m.deadlines = fresh
}

// seedMissing gives a now+ttl deadline to every live session that has none,
// leaving existing deadlines untouched.
//
// This closes the grant→expiry gap: GrantSession applies in the replicated SM,
// but deadlines live only in the LEADER's memory, so a session granted under
// this leader has no deadline until its first keepalive arrives. A node that
// dies inside that window would otherwise hold its name lock forever — and,
// unlike a data-lost node (which readmitNode can probe-and-revoke), a
// data-intact node skips JoinCluster entirely and fails boot on ErrNameActive
// with no escape. Reconciling against the SM each scan is self-healing whatever
// route a session arrived by.
func (m *sessionManager) seedMissing(sessionIDs []string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range sessionIDs {
		if _, ok := m.deadlines[id]; !ok {
			m.deadlines[id] = now.Add(m.ttl)
		}
	}
}
