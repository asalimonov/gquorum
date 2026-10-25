package recoverymodel

import (
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// AuthorityEvidence contains validated source facts, not new authority in B.
type AuthorityEvidence struct {
	Identity         topology.Identity     `json:"identity"`
	MetadataAcquired bool                  `json:"metadata_acquired"`
	AdmissionID      string                `json:"admission_id,omitempty"`
	PreviousMemberID uint64                `json:"previous_member_id,omitempty"`
	Fences           []AuthorityFence      `json:"fences"`
	Effects          []AuthorityEffect     `json:"effects"`
	Collections      []AuthorityCollection `json:"collections"`
}

// ReadBounds limits encoded WAL framing and materialized group input.
type ReadBounds struct{ MaxRecordBytes, MaxGroupBytes uint64 }

// WALEvidence is immutable validated source input and its retained boundaries.
type WALEvidence struct {
	Identity        storage.Identity
	Recovery        storage.Recovery
	Metadata        []byte
	SnapshotMarkers []Position
	LastLog         Position
	Digest          string
	Bytes           uint64
	TornTail        *TornTail
	Issues          []Issue
}
