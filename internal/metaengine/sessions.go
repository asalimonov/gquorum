package metaengine

import (
	"fmt"
	"slices"
)

type sessionState struct {
	SessionID  string `json:"session_id"`
	NodeName   string `json:"node_name"`
	InstanceID string `json:"instance_id"`
	TTLSeconds int    `json:"ttl_seconds"`
}

func (sm *topologySM) applyGrantSession(p GrantSession) (ApplyResult, bool) {
	if p.SessionID == "" || p.NodeName == "" || p.InstanceID == "" || p.TTLSeconds <= 0 {
		return rejectProposal(p.ProposalID, proposalGuardrail,
			"session requires exact identity and positive TTL"), false
	}
	if existing, exists := sm.sessions[p.SessionID]; exists {
		if existing.NodeName != p.NodeName || existing.InstanceID != p.InstanceID ||
			existing.TTLSeconds != p.TTLSeconds {
			return rejectProposal(p.ProposalID, proposalAuthorization, "session identity differs"), false
		}
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, false
	}
	sm.sessions[p.SessionID] = &sessionState{
		SessionID:  p.SessionID,
		NodeName:   p.NodeName,
		InstanceID: p.InstanceID,
		TTLSeconds: p.TTLSeconds,
	}
	if p.NodeName != "" {
		sm.activated[p.NodeName] = struct{}{}
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, true
}

func (sm *topologySM) applyRevokeSession(p RevokeSession) (ApplyResult, bool) {
	if _, exists := sm.sessions[p.SessionID]; !exists {
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, false
	}
	delete(sm.sessions, p.SessionID)
	changed := true
	for key, holder := range sm.locks {
		if holder == p.SessionID {
			delete(sm.locks, key)
		}
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, changed
}

func (sm *topologySM) applyAcquireLock(p AcquireLock) (ApplyResult, bool) {
	if p.Key == "" {
		return rejectProposal(p.ProposalID, proposalGuardrail, "lock key required"), false
	}
	if _, ok := sm.sessions[p.SessionID]; !ok {
		return ApplyResult{
			ProposalID: p.ProposalID,
			Accepted:   false,
			Reason:     fmt.Sprintf("lock %q: acquiring session %q does not exist", p.Key, p.SessionID),
		}, false
	}
	if holder, held := sm.locks[p.Key]; held {
		if holder == p.SessionID {
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, false
		}
		if _, live := sm.sessions[holder]; live {
			node := ""
			if s := sm.sessions[holder]; s != nil {
				node = s.NodeName
			}
			return ApplyResult{
				ProposalID: p.ProposalID,
				Accepted:   false,
				Reason:     fmt.Sprintf("lock %q already held by a live session of node %q", p.Key, node),
			}, false
		}
	}
	sm.locks[p.Key] = p.SessionID
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true}, true
}

func (sm *topologySM) sessionSnapshotLocked() []sessionState {
	ids := make([]string, 0, len(sm.sessions))
	for id := range sm.sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := make([]sessionState, 0, len(ids))
	for _, id := range ids {
		out = append(out, *sm.sessions[id])
	}
	return out
}

type lockEntry struct {
	Key       string `json:"key"`
	SessionID string `json:"session_id"`
}

func (sm *topologySM) lockSnapshotLocked() []lockEntry {
	keys := make([]string, 0, len(sm.locks))
	for k := range sm.locks {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]lockEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, lockEntry{Key: k, SessionID: sm.locks[k]})
	}
	return out
}

func (sm *topologySM) Sessions() []sessionState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessionSnapshotLocked()
}

func (sm *topologySM) session(sessionID string) (sessionState, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.sessions[sessionID]
	if !ok {
		return sessionState{}, false
	}
	return *s, true
}

func (sm *topologySM) lockHolderSession(key string) (sessionState, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	sid, held := sm.locks[key]
	if !held {
		return sessionState{}, false
	}
	if s, ok := sm.sessions[sid]; ok {
		return *s, true
	}
	return sessionState{}, true
}

func (sm *topologySM) LockHolder(key string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	holder, held := sm.locks[key]
	if !held {
		return "", false
	}
	if s := sm.sessions[holder]; s != nil {
		return s.NodeName, true
	}
	return "", true
}
