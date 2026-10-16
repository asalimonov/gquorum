package operator

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// Coordinator supplies bounded scoped work and exact replicated transitions.
// A selected item is an observation, never authorization to perform an effect.
type Coordinator interface {
	IsMetaLeader() bool
	ReadyBlocks(context.Context, int) ([]WorkItem, error)
	RecordOperationTransition(context.Context, topology.OperationTransition) error
}

// WorkItem owns one operation snapshot and identifies its required group block.
type WorkItem struct {
	Operation topology.Operation
	GroupID   string
}

// ScopedValidator checks one immutable operation scope against current evidence.
type ScopedValidator func(context.Context, topology.Operation, string) error
