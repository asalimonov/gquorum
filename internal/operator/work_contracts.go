package operator

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// Coordinator supplies bounded scoped work and exact replicated transitions.
// A selected item is an observation, never authorization to perform an effect.
type Coordinator interface {
	CleanupCoordinator
	IsMetaLeader() bool
	ReadyBlocks(context.Context, int) ([]WorkItem, error)
	RecordOperationTransition(context.Context, topology.OperationTransition) error
}

// WorkItem owns one operation snapshot and identifies its required group block.
type WorkItem struct {
	Operation topology.Operation
	GroupID   string
	// Approved is the declaration retained with this operation, nil for deletion.
	Approved *topology.Group
	// Consumed describes the current required step, including original retry proof.
	// It is an observation; the replicated transition independently checks proof.
	Consumed bool
}

// ScopedValidator checks one immutable operation scope against current evidence.
type ScopedValidator func(context.Context, topology.Operation, string) error
