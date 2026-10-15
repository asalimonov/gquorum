package topology

import "slices"

// Clone returns owned rows while preserving absence, provenance and original timestamps.
func (p ProgressPage) Clone() ProgressPage {
	owned := p
	owned.Groups = slices.Clone(p.Groups)
	for i := range owned.Groups {
		owned.Groups[i] = p.Groups[i].Clone()
	}
	owned.Nodes = slices.Clone(p.Nodes)
	for i := range owned.Nodes {
		owned.Nodes[i] = p.Nodes[i].Clone()
	}
	owned.Operations = slices.Clone(p.Operations)
	for i := range owned.Operations {
		owned.Operations[i] = p.Operations[i].Clone()
	}
	owned.Cleanup = slices.Clone(p.Cleanup)
	for i := range owned.Cleanup {
		owned.Cleanup[i] = cloneCleanupProgress(p.Cleanup[i])
	}
	return owned
}

// BlockedDependency and the following codes identify stable progress blockers.
const (
	BlockedDependency           = "dependency"
	BlockedRevocationPending    = "revocation_pending"
	BlockedReplicaUnreachable   = "replica_unreachable"
	BlockedQuorumUnavailable    = "quorum_unavailable"
	BlockedCatchUp              = "catch_up"
	BlockedLeadership           = "leadership"
	BlockedLocalHold            = "local_hold"
	BlockedRetentionDeferred    = "retention_deferred"
	BlockedCleanupFailed        = "cleanup_failed"
	BlockedIdentityMismatch     = "identity_mismatch"
	BlockedStagedConflict       = "staged_conflict"
	BlockedSupersededDependency = "superseded_dependency"
	BlockedUnconfirmed          = "unconfirmed"
)
