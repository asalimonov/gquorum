// Package gquorum owns a cluster participant and its managed Raft replicas.
// NewNode constructs a Node from caller-supplied settings; Start opens the
// local topology service and starts admission and recovery. Node.Cluster provides
// topology administration, while load.Group exposes application consensus.
// Applications supply load state machines and transport, and may replace the
// built-in repair decisions through package scheduling and local lifecycle
// decisions through package lifecycle.
//
// NewRecovery inspects and archives one stopped complete survivor after permanent
// majority loss. It requires Node-owned WAL configuration and explicit acceptance
// of retained-commit data loss. RecoveryStartOptions prepares ordinary Node startup
// with a fresh cluster lineage on the same roots. Package recovery supplies plain
// request, acceptance, file-hook and status values; applications own remote jobs.
//
// TODO(not planned): key-range splitting/merging, MVCC, timestamp allocation
// and a separate PD service. Application data semantics stay outside this owner.
package gquorum
