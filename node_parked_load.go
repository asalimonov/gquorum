package gquorum

import (
	"time"

	"github.com/asalimonov/gquorum/storage"
)

// ParkReason classifies why local replica startup was refused.
type ParkReason string

// ParkedLoad describes a refused local boot, not a replicated cluster event.
// Entries clear only after a successful start of the approved identity.
type ParkedLoad struct {
	Identity storage.Identity `json:"identity"`
	Reason   ParkReason       `json:"reason"`
	Since    time.Time        `json:"since"`
}

// LoadParkedError reports a refused replica boot while preserving its
// underlying cause.
type LoadParkedError struct {
	ParkedLoad
	Cause error
}
