//go:build !functional

package metaengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Grant/acquire/revoke exercise the production Apply boundary against an
// approved metadata membership checkpoint.

func TestSM_GrantSession_AndIdempotent(t *testing.T) {
	t.Parallel()
	state, _ := rebaseFixture(t)
	sm := restoreMachineFixture(t, state)
	r := applyMachine(t, sm, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", TTLSeconds: 15, ProposalID: "p1"})
	assert.True(t, r.Accepted)
	require.Len(t, sm.Sessions(), 1)
	assert.Equal(t, "n1", sm.Sessions()[0].NodeName)

	// Re-grant the same session ID is an accepted no-op.
	r = applyMachine(t, sm, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "p2", TTLSeconds: 15})
	assert.True(t, r.Accepted)
	assert.Len(t, sm.Sessions(), 1)
}

func TestSM_AcquireLock_RequiresSession(t *testing.T) {
	t.Parallel()
	state, _ := rebaseFixture(t)
	sm := restoreMachineFixture(t, state)
	// Acquiring under a non-existent session is rejected.
	r := applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "ghost", ProposalID: "p1"})
	assert.False(t, r.Accepted)
	assert.Contains(t, r.Reason, "does not exist")
}

func TestSM_AcquireLock_Grant_Reacquire_Conflict(t *testing.T) {
	t.Parallel()
	state, _ := rebaseFixture(t)
	sm := restoreMachineFixture(t, state)
	applyMachine(t, sm, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	applyMachine(t, sm, GrantSession{SessionID: "s2", NodeName: "n1", InstanceID: "i2", ProposalID: "g2", TTLSeconds: 15})

	// s1 acquires node/n1.
	r := applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})
	assert.True(t, r.Accepted)
	holder, held := sm.LockSession("node/n1")
	assert.True(t, held)
	assert.Equal(t, "n1", holder.NodeName)

	// s1 re-acquires: accepted no-op.
	r = applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a2"})
	assert.True(t, r.Accepted)

	// s2 (a second live process claiming the same node name) is rejected.
	r = applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "s2", ProposalID: "a3"})
	assert.False(t, r.Accepted)
	assert.Contains(t, r.Reason, "already held by a live session of node \"n1\"")
}

func TestSM_RevokeSession_ReleasesLocks(t *testing.T) {
	t.Parallel()
	state, _ := rebaseFixture(t)
	sm := restoreMachineFixture(t, state)
	applyMachine(t, sm, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})
	applyMachine(t, sm, AcquireLock{Key: "other", SessionID: "s1", ProposalID: "a2"})

	// Revoke s1 → both its locks release and the session is gone.
	r := applyMachine(t, sm, RevokeSession{SessionID: "s1", Reason: "expired", ProposalID: "r1"})
	assert.True(t, r.Accepted)
	assert.Empty(t, sm.Sessions())
	_, held := sm.LockSession("node/n1")
	assert.False(t, held)
	_, held = sm.LockSession("other")
	assert.False(t, held)

	// Revoking an absent session is an accepted no-op.
	r = applyMachine(t, sm, RevokeSession{SessionID: "s1", ProposalID: "r2"})
	assert.True(t, r.Accepted)
}

func TestSM_AcquireLock_ReclaimsAfterHolderGone(t *testing.T) {
	t.Parallel()
	state, _ := rebaseFixture(t)
	sm := restoreMachineFixture(t, state)
	applyMachine(t, sm, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})
	applyMachine(t, sm, RevokeSession{SessionID: "s1", ProposalID: "r1"})

	// A fresh session for the same node (restart) may reclaim the freed lock.
	applyMachine(t, sm, GrantSession{SessionID: "s2", NodeName: "n1", InstanceID: "i2", ProposalID: "g2", TTLSeconds: 15})
	r := applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "s2", ProposalID: "a2"})
	assert.True(t, r.Accepted)
	holder, held := sm.LockSession("node/n1")
	assert.True(t, held)
	assert.Equal(t, "n1", holder.NodeName)
}

// Sessions/locks survive a snapshot/restore round-trip AND do not pollute the
// document version history.
func TestSM_Sessions_SnapshotRestore_NoVersionBump(t *testing.T) {
	t.Parallel()
	state, _ := rebaseFixture(t)
	sm := restoreMachineFixture(t, state)
	verBefore := sm.Latest().Version
	applyMachine(t, sm, GrantSession{SessionID: "s1", NodeName: "n1", InstanceID: "i1", ProposalID: "g1", TTLSeconds: 15})
	applyMachine(t, sm, AcquireLock{Key: "node/n1", SessionID: "s1", ProposalID: "a1"})

	// Session/lock activity must not bump the document version.
	assert.Equal(t, verBefore, sm.Latest().Version, "sessions/locks must not change the document version")

	data, err := sm.Snapshot(t.Context())
	require.NoError(t, err)
	restored := mustMachine(t, testRules(), nil)
	require.NoError(t, restored.Restore(t.Context(), data))

	assert.Equal(t, sm.Latest().Version, restored.Latest().Version)
	require.Len(t, restored.Sessions(), 1)
	holder, held := restored.LockSession("node/n1")
	assert.True(t, held)
	assert.Equal(t, "n1", holder.NodeName)

	// Snapshots are byte-identical (determinism).
	d2, err := restored.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, string(data), string(d2))
}
