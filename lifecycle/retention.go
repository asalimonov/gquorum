package lifecycle

import (
	"time"

	"github.com/asalimonov/gquorum/storage"
)

// RetentionInput supplies the current time and an exact quarantined replica.
type RetentionInput struct {
	Now     time.Time
	Retired storage.RetiredReplica
}

// RetentionDecision requests collection or continued retention of quarantined
// state.
type RetentionDecision struct {
	Collect   bool
	NotBefore time.Time
	Reason    string
}
