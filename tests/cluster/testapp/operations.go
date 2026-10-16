package main

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// readOperationStatus bridges schema-3 status until Task78 activates persisted
// scoped operation reads. The fallback identifies only the exact retained ID.
func readOperationStatus(ctx context.Context, cluster interface {
	Operation(context.Context, topology.OperationID) (topology.OperationStatus, error)
	Status(context.Context) (topology.ClusterStatus, error)
}, id topology.OperationID) (topology.OperationStatus, error) {
	status, err := cluster.Operation(ctx, id)
	if err == nil || !errors.Is(err, fault.ErrUnsupported) {
		return status, err
	}
	observed, err := cluster.Status(ctx)
	if err != nil {
		return topology.OperationStatus{}, err
	}
	legacy := observed.Operator
	if legacy == nil || legacy.ID != topology.OperatorID(id) {
		return topology.OperationStatus{}, fault.ErrHistoryUnavailable
	}
	status = topology.OperationStatus{ID: id, Version: legacy.Version, Attempt: legacy.Attempt,
		State: topology.OperationState(legacy.State), ErrorCode: legacy.ErrorCode, Error: legacy.Error}
	if legacy.GroupID != "" {
		block := topology.BlockStatus{GroupID: legacy.GroupID, State: topology.OperationState(legacy.State),
			CurrentStep: legacy.CurrentStep, TotalSteps: legacy.TotalSteps, ErrorCode: legacy.ErrorCode, Error: legacy.Error}
		if legacy.WaitingReason != "" {
			block.Blocked = &topology.BlockedReason{Message: legacy.WaitingReason, NodeName: legacy.NodeName}
		}
		status.Blocks, status.TotalBlocks = []topology.BlockStatus{block}, 1
	}
	return status, nil
}

func waitForOperation(ctx context.Context, cluster interface {
	Operation(context.Context, topology.OperationID) (topology.OperationStatus, error)
	Status(context.Context) (topology.ClusterStatus, error)
}, id topology.OperationID) (topology.OperationStatus, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := readOperationStatus(ctx, cluster, id)
		if err == nil && status.State.Terminal() {
			return status, nil
		}
		if err != nil && !errors.Is(err, fault.ErrHistoryUnavailable) {
			return status, err
		}
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-ticker.C:
		}
	}
}
