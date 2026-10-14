package metastore

import "go.etcd.io/raft/v3/raftpb"

// AppliedMembership distinguishes a persisted ConfState from an absent marker.
type AppliedMembership struct {
	ConfState raftpb.ConfState
	Found     bool
}

// LocalLineage identifies the highest retained topology version and member pin.
type LocalLineage struct {
	Version  uint64
	MemberID uint64
}

// IdentityLookup preserves pin presence separately from validation errors.
type IdentityLookup struct {
	Identity Identity
	Found    bool
}
