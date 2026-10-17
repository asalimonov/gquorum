package raftengine

import "go.etcd.io/raft/v3/raftpb"

// Command contains a committed application payload, request identity and Raft
// position.
type Command struct {
	Index, Term uint64
	RequestID   string
	Data        []byte
	// ConfState is metadata-only owned native membership at Index, including
	// earlier configuration entries in the same Ready batch. Load commands
	// leave it empty.
	// It is runtime context, never supplied by an application proposal.
	ConfState raftpb.ConfState
}

// Result carries application response data and a deterministic business result
// code.
type Result struct {
	Code string
	Data []byte
}

// Proposal carries an application request ID and command payload.
type Proposal struct {
	RequestID string
	Data      []byte
}
