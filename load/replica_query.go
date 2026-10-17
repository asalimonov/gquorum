package load

import (
	"github.com/asalimonov/gquorum/metrics"

	"github.com/asalimonov/gquorum/topology"
)

// ReplicaQuery requests observations or confirmed native membership from an
// exact peer.
type ReplicaQuery struct {
	From topology.Identity
	To   Peer
	// Native requests quorum-confirmed applied membership, independently of the
	// diagnostic Status below. Transports must preserve this flag and the
	// reply.
	Native bool
}

// ReplicaStatus returns an exact peer's observed state and optional confirmed
// membership.
type ReplicaStatus struct {
	Peer  Peer
	Found bool
	// Native is present only for a successful Native query on a ready replica.
	// Absence (including Found=false) never proves that a group does not exist.
	Native *topology.NativeState `json:"native,omitempty"`
	// Status is a raw replica observation. LeaderNodeName is not authoritative;
	// cluster status and scheduler inputs resolve it through exact approved or
	// converged replica identities.
	Status     topology.GroupStatus
	LocalState string
	Metrics    *metrics.ReplicaSample `json:"metrics,omitempty"`
}
