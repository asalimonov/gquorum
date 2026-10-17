package operator

import (
	"context"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// CleanupCoordinator selects retained detached actions; selection is advisory.
// Required deletion stops stay in ReadyBlocks and ordinary execution capacity.
type CleanupCoordinator interface {
	ReadyCleanup(context.Context, int) ([]CleanupWork, error)
	BeginCleanupEffect(context.Context, storage.Identity, topology.AuthorizedStep) (topology.AuthorityGrant, error)
	ReadyFences(context.Context, int) ([]FenceWork, error)
	ResolveFenceTarget(context.Context, FenceWork) (topology.AuthorityTarget, error)
	DispatchFence(context.Context, topology.AuthorityGrant, topology.AuthorityResource, topology.AuthorityTarget) error
}

// CleanupWork carries one exact former identity and its current approved action.
// No receipt or collection token is copied into the dispatch queue.
type CleanupWork struct {
	Identity storage.Identity
	MemberID uint64
	Action   topology.AuthorizedStep
}

// FenceWork carries a resource's current immutable boundary. Recipient lookup
// happens inside a bounded worker and never implies installation evidence.
type FenceWork struct {
	Grant    topology.AuthorityGrant
	Resource topology.AuthorityResource
}
