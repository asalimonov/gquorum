// Package load exposes application Raft replicas without leaking the internal
// engine's lifecycle and membership controls. Node manages cluster replicas;
// Standalone owns an explicitly independent engine.
package load

// Command contains a committed application request and its Raft position.
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

// Proposal carries an application request ID and command data for consensus.
type Proposal struct {
	RequestID string
	Data      []byte
}
