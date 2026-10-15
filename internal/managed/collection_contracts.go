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
	Status func(context.Context, storage.Identity, *time.Time, *topology.BlockedReason) error
}
