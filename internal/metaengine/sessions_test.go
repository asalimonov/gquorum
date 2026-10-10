package metaengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// grant/acquire/revoke through the SM Apply path (using the shared apply helper
// from sm_test.go) exercises the deterministic session+lock rules (N2/N3).

func TestSM_GrantSession_AndIdempotent(t *testing.T) {
	sm := newTopologySM(nil, testRules())
	r := apply(t, sm, 1, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", TTLSeconds: 15, ProposalID: "p1"})
	assert.True(t, r.Accepted)
	require.Len(t, sm.Sessions(), 1)
	assert.Equal(t, "n1", sm.Sessions()[0].NodeName)

	// Re-grant the same session ID is an accepted no-op.
	r = apply(t, sm, 2, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "p2", TTLSeconds: 15})
	assert.True(t, r.Accepted)
	assert.Len(t, sm.Sessions(), 1)
}

func TestSM_AcquireLock_RequiresSession(t *testing.T) {
	sm := newTopologySM(nil, testRules())
	// Acquiring under a non-existent session is rejected.
	r := apply(t, sm, 1, AcquireLock{Key: "node/n1", SessionID: "ghost", ProposalID: "p1"})
	assert.False(t, r.Accepted)
	assert.Contains(t, r.Reason, "does not exist")
}

func TestSM_AcquireLock_Grant_Reacquire_Conflict(t *testing.T) {
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	apply(t, sm, 2, GrantSession{SessionID: "s2", NodeName: "n1", InstanceID: "i2", ProposalID: "g2", TTLSeconds: 15})

	// s1 acquires node/n1.
	r := apply(t, sm, 3, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})
	assert.True(t, r.Accepted)
	holder, held := sm.LockHolder("node/n1")
	assert.True(t, held)
	assert.Equal(t, "n1", holder)

	// s1 re-acquires: accepted no-op.
	r = apply(t, sm, 4, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a2"})
	assert.True(t, r.Accepted)

	// s2 (a second live process claiming the same node name) is rejected.
	r = apply(t, sm, 5, AcquireLock{Key: "node/n1", SessionID: "s2", ProposalID: "a3"})
	assert.False(t, r.Accepted)
	assert.Contains(t, r.Reason, "already held by a live session of node \"n1\"")
}

func TestSM_RevokeSession_ReleasesLocks(t *testing.T) {
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	apply(t, sm, 2, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})
	apply(t, sm, 3, AcquireLock{Key: "other", SessionID: "s1", ProposalID: "a2"})

	// Revoke s1 → both its locks release and the session is gone.
	r := apply(t, sm, 4, RevokeSession{SessionID: "s1", Reason: "expired", ProposalID: "r1"})
	assert.True(t, r.Accepted)
	assert.Empty(t, sm.Sessions())
	_, held := sm.LockHolder("node/n1")
	assert.False(t, held)
	_, held = sm.LockHolder("other")
	assert.False(t, held)

	// Revoking an absent session is an accepted no-op.
	r = apply(t, sm, 5, RevokeSession{SessionID: "s1", ProposalID: "r2"})
	assert.True(t, r.Accepted)
}

func TestSM_AcquireLock_ReclaimsAfterHolderGone(t *testing.T) {
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	apply(t, sm, 2, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})
	apply(t, sm, 3, RevokeSession{SessionID: "s1", ProposalID: "r1"})

	// A fresh session for the same node (restart) may reclaim the freed lock.
	apply(t, sm, 4, GrantSession{SessionID: "s2", NodeName: "n1", InstanceID: "i2", ProposalID: "g2", TTLSeconds: 15})
	r := apply(t, sm, 5, AcquireLock{Key: "node/n1", SessionID: "s2", ProposalID: "a2"})
	assert.True(t, r.Accepted)
	holder, held := sm.LockHolder("node/n1")
	assert.True(t, held)
	assert.Equal(t, "n1", holder)
}

// Sessions/locks survive a snapshot/restore round-trip AND do not pollute the
// document version history.
func TestSM_Sessions_SnapshotRestore_NoVersionBump(t *testing.T) {
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "d1"})
	verBefore := sm.Current().Version
	apply(t, sm, 2, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	apply(t, sm, 3, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})

	// Session/lock activity must not bump the document version.
	assert.Equal(t, verBefore, sm.Current().Version, "sessions/locks must not change the document version")

	data, err := sm.Snapshot()
	require.NoError(t, err)
	restored := newTopologySM(nil, testRules())
	require.NoError(t, restored.Restore(data))

	assert.Equal(t, sm.Current().Version, restored.Current().Version)
	require.Len(t, restored.Sessions(), 1)
	holder, held := restored.LockHolder("node/n1")
	assert.True(t, held)
	assert.Equal(t, "n1", holder)

	// Snapshots are byte-identical (determinism).
	d2, err := restored.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, string(data), string(d2))
}
