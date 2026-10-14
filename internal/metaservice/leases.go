package metaservice

import (
	"slices"
	"sync"
	"time"
)

type sessionManager struct {
	ttl time.Duration

	mu        sync.Mutex
	deadlines map[string]time.Time // sessionID → expiry
}

func newSessionManager(ttl time.Duration) *sessionManager {
	return &sessionManager{ttl: ttl, deadlines: make(map[string]time.Time)}
}

func (m *sessionManager) refreshTTL(sessionID string, now time.Time, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadlines[sessionID] = now.Add(ttl)
}

// remove drops a session's deadline (after revoke).
func (m *sessionManager) remove(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.deadlines, sessionID)
}

// expired returns the session IDs whose deadline is at or before now.
func (m *sessionManager) expired(now time.Time) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for id, deadline := range m.deadlines {
		if !deadline.After(now) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// remaining returns the seconds left on a session's lease (>=0), or 0 when
// absent.
func (m *sessionManager) remaining(sessionID string, now time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.deadlines[sessionID]
	if !ok {
		return 0
	}
	if secs := int(d.Sub(now).Seconds()); secs > 0 {
		return secs
	}
	return 0
}

func (m *sessionManager) isExpired(id string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.deadlines[id]
	return ok && !d.After(now)
}
