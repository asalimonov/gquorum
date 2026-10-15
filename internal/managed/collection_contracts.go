package managed

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// CollectionCallbacks connect host-owned collection proof to metadata acceptance
// and diagnosis. They do not authorize retirement or transfer host ownership.
type CollectionCallbacks struct {
	Report func(context.Context, authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error)
	Status func(context.Context, CleanupStatus) error
}

// CleanupStatus retains the historical host/proof binding even after readmission.
// Diagnostic updates never rebind this evidence to the current process identity.
type CleanupStatus struct {
	Identity      storage.Identity
	MemberID      uint64
	RetirementKey topology.AuthorityKey
	NotBefore     *time.Time
	Blocked       *topology.BlockedReason
}
