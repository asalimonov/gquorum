package meta

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// --- pure sessionManager TTL table (deterministic, injected clock) ---

func TestSessionManager_RefreshAndExpiry(t *testing.T) {
	m := newSessionManager(15 * time.Second)
	t0 := time.Unix(1_000_000, 0)
	m.refresh("s1", t0)

	// Not yet expired at t0+10s.
	assert.Empty(t, m.expired(t0.Add(10*time.Second)))
	// Expired at t0+15s (deadline reached).
	assert.Equal(t, []string{"s1"}, m.expired(t0.Add(15*time.Second)))
	// A refresh pushes the deadline out.
	m.refresh("s1", t0.Add(10*time.Second))
	assert.Empty(t, m.expired(t0.Add(20*time.Second)))
}

// TestSessionManager_SeedMissing covers the grant→expiry gap: GrantSession
// applies in the replicated SM, but deadlines live only in the leader's memory
// and are written by keepalive/failover alone. A session granted under THIS
// leader therefore has no deadline until its first keepalive — and a node that
// dies inside that window would hold its name lock forever, failing its own
// restart on ErrNameActive with no escape (unlike a data-lost node, which
// readmitNode can probe-and-revoke).
func TestSessionManager_SeedMissing(t *testing.T) {
	m := newSessionManager(15 * time.Second)
	t0 := time.Unix(1_000_000, 0)

	// A session the leader has never seen a keepalive for has no deadline, so
	// it can never expire — this is the bug.
	require.Empty(t, m.expired(t0.Add(time.Hour)), "precondition: no deadline means no expiry")

	m.seedMissing([]string{"granted"}, t0)

	// It now expires on the normal TTL schedule.
	assert.Empty(t, m.expired(t0.Add(10*time.Second)))
	assert.Equal(t, []string{"granted"}, m.expired(t0.Add(15*time.Second)))
}

func TestSessionManager_SeedMissingPreservesExistingDeadlines(t *testing.T) {
	m := newSessionManager(15 * time.Second)
	t0 := time.Unix(1_000_000, 0)

	// A session already 10s into its lease must not have that lease extended by
	// a seed pass — otherwise the per-scan reconcile would keep a dead node's
	// session alive forever, which is the very thing it exists to end.
	m.refresh("live", t0)
	m.seedMissing([]string{"live", "fresh"}, t0.Add(10*time.Second))

	assert.Equal(t, []string{"live"}, m.expired(t0.Add(15*time.Second)),
		"seedMissing must not extend an existing deadline")
	assert.Equal(t, 15, m.remaining("fresh", t0.Add(10*time.Second)),
		"a newly seeded session gets a full TTL")
}

func TestSessionManager_ResetFrom(t *testing.T) {
	m := newSessionManager(15 * time.Second)
	t0 := time.Unix(1_000_000, 0)
	m.refresh("stale", t0.Add(-time.Hour)) // long expired
	m.resetFrom([]string{"a", "b"}, t0)

	// The stale deadline is dropped; a and b get fresh leases.
	assert.Empty(t, m.expired(t0.Add(10*time.Second)))
	assert.Len(t, m.expired(t0.Add(16*time.Second)), 2)
	assert.Equal(t, 0, m.remaining("stale", t0), "reset drops sessions not in the set")
	assert.Greater(t, m.remaining("a", t0), 0)
}

// --- MetaGroup handlers on a single-node leader ---
