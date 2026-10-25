//revive:disable:package-naming Recovery scopes this runtime; callers import it as recoveryruntime.
package runtime

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
)

func retryControl(ctx context.Context, call func(context.Context) error) error {
	_, err := fault.Retry(ctx, fault.RetryOptions{}, func(attempt context.Context) (struct{}, error) {
		return struct{}{}, call(attempt)
	})
	return err
}

func (c *Coordinator) readRecovery(ctx context.Context) (*model.ImportState, error) {
	return fault.Retry(ctx, fault.RetryOptions{}, c.config.Meta.ReadRecovery)
}

// Waiting creates no new attempt or factory: only RetryOperation does that.
func (c *Coordinator) waitImportOperation(ctx context.Context, id topology.OperationID) error {
	return retryControl(ctx, func(attempt context.Context) error {
		status, err := c.config.Meta.Operation(attempt, id)
		if err != nil {
			return err
		}
		if status.State == topology.OperationSucceeded {
			return nil
		}
		if status.State.Terminal() {
			return fault.New(fault.OperationAuthorization, "recovery import attempt did not succeed", nil)
		}
		return fault.ErrUnavailable
	})
}
