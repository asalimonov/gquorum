package recoveryruntime

import (
	"context"
	"errors"
	"math"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Follow retained attempts without issuing work, then create at most one successor
// during this Node start. A failed new factory is never put in an automatic loop.
func (c *Coordinator) resumeExistingOperation(ctx context.Context,
	first topology.OperationID) (topology.OperationID, error) {
	status, err := c.config.Meta.Operation(ctx, first)
	if err != nil {
		return "", err
	}
	for status.Attempt < math.MaxUint32 {
		next, err := c.config.Meta.Operation(ctx, topology.MakeOperationID(status.Version, status.Attempt+1))
		if errors.Is(err, fault.ErrHistoryUnavailable) {
			break
		}
		if err != nil {
			return "", err
		}
		if next.Version != status.Version || next.Attempt != status.Attempt+1 {
			return "", fault.ErrIdentityMismatch
		}
		status = next
	}
	if status.State == topology.OperationFailed {
		return c.config.Meta.RetryOperation(ctx, status.ID)
	}
	if status.State == topology.OperationSuperseded {
		return "", fault.ErrOperationAuthorization
	}
	return status.ID, nil
}
