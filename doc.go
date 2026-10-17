// Package gquorum owns a cluster participant and its managed Raft replicas.
// NewNode constructs a Node from caller-supplied settings; Start opens the
// local topology service and starts admission and recovery. Node.Cluster provides
// topology administration, while load.Group exposes application consensus.
// Applications supply load state machines and transport, and may replace the
// built-in repair decisions through package scheduling and local lifecycle
// decisions through package lifecycle.
//
// TODO(not planned): key-range splitting/merging, MVCC, timestamp allocation
// and a separate PD service. Application data semantics stay outside this owner.
package gquorum
