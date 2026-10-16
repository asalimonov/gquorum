package metaservice

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Operation observes an exact retained attempt locally. A later retry never
// redirects this lookup; ReadProgress supplies confirmed paged observations.
func (g *Group) Operation(ctx context.Context, id topology.OperationID) (topology.OperationStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.OperationStatus{}, err
	}
	return g.state().Operation(id)
}

// WaitOperation waits for the complete aggregate outcome of the named attempt.
// Cancellation only ends this waiter and never cancels approved work.
func (g *Group) WaitOperation(ctx context.Context, id topology.OperationID) (topology.OperationStatus, error) {
	return g.waitOperationResult(ctx, id, g.Operation)
}

func (g *Group) waitOperationResult(ctx context.Context, id topology.OperationID,
	read func(context.Context, topology.OperationID) (topology.OperationStatus, error),
) (topology.OperationStatus, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := read(ctx, id)
		if err == nil && status.State.Terminal() {
			return status, nil
		}
		if err != nil && !errors.Is(err, fault.ErrHistoryUnavailable) {
			return status, err
		}
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-g.stopped:
			return status, fault.ErrStopped
		case <-ticker.C:
		}
	}
}
