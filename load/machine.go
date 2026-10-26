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

// Proposal carries a required caller-owned logical request ID and command data.
// Reuse the exact ID and bytes for the same operation across calls/restarts.
// RequestID is passed unchanged to Apply; the library does not deduplicate it.
type Proposal struct {
	RequestID string
	Data      []byte
}
