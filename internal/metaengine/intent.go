package metaengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ScopeExpectation binds an approval to captured proven state and current ownership.
type ScopeExpectation struct {
	GroupID       string               `json:"group_id"`
	SourceVersion uint64               `json:"source_version"`
	Owner         topology.OperationID `json:"owner,omitempty"`
}

// Intent is typed-mutation provenance, not a general request registry.
// The digest is canonical lowercase SHA-256 hex; validation belongs to the FSM.
type Intent struct {
	Version         uint64 `json:"version"`
	ExpectedVersion uint64 `json:"expected_version"`
	Kind            string `json:"kind"`
	Digest          string `json:"digest"`
	GroupID         string `json:"group_id,omitempty"`
	NodeName        string `json:"node_name,omitempty"`
	PreviousID      uint64 `json:"previous_id,omitempty"`
	SuccessorID     uint64 `json:"successor_id,omitempty"`
}

// NewIntent fingerprints validated, normalized typed arguments before identity
// allocation. This internal boundary shares the FSM's accepted kind vocabulary;
// it is provenance, never independent authority for a document mutation.
func NewIntent(kind string, arguments any) (Intent, error) {
	if arguments == nil || !knownIntentKind(kind) {
		return Intent{}, fault.ErrInvalid
	}
	data, err := json.Marshal(arguments)
	if err != nil {
		return Intent{}, fault.New(fault.Invalid, "invalid normalized mutation intent", err)
	}
	digest := sha256.Sum256(data)
	return Intent{Kind: kind, Digest: hex.EncodeToString(digest[:])}, nil
}
