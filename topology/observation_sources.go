package topology

import (
	"context"
)

// AddressResolver resolves a logical node name to its meta transport endpoint.
// Resolve runs on the Raft send path, including before a leader exists. Use
// local routing information; a quorum read here prevents the election it needs.
type AddressResolver interface {
	Resolve(context.Context, string) (string, error)
}

// HealthSource supplies owned node-reachability observations.
type HealthSource interface {
	// Snapshot returns an owned, coherent node observation. Nil means globally
	// unknown; a missing node or HealthUnknown remains unknown, never dead.
	Snapshot(context.Context) (map[string]Health, error)
}
