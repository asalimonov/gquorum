package metaengine

import "github.com/asalimonov/gquorum/topology"

// ScopeExpectation binds an approval to captured proven state and current ownership.
type ScopeExpectation struct {
	GroupID       string               `json:"group_id"`
	SourceVersion uint64               `json:"source_version"`
	Owner         topology.OperationID `json:"owner,omitempty"`
}

// intentRecord is typed-mutation provenance, not a general request registry.
// The digest is canonical lowercase SHA-256 hex; validation belongs to the FSM.
type intentRecord struct {
	Version         uint64 `json:"version"`
	ExpectedVersion uint64 `json:"expected_version"`
	Kind            string `json:"kind"`
	Digest          string `json:"digest"`
	GroupID         string `json:"group_id,omitempty"`
	NodeName        string `json:"node_name,omitempty"`
	PreviousID      uint64 `json:"previous_id,omitempty"`
	SuccessorID     uint64 `json:"successor_id,omitempty"`
}
