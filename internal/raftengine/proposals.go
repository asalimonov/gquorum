package raftengine

// Command contains a committed application payload, request identity and Raft
// position.
type Command struct {
	Index, Term uint64
	RequestID   string
	Data        []byte
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
