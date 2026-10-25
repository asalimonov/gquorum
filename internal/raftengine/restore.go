package raftengine

import (
	"bytes"
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

func restoreMachine(ctx context.Context, machine Machine, data []byte, boundary raftpb.SnapshotMetadata) error {
	if err := machine.Restore(ctx, bytes.Clone(data)); err != nil {
		return err
	}
	if validator, ok := machine.(RestoredValidator); ok {
		if err := validator.ValidateRestoredSnapshot(ctx, boundary.Index, boundary.Term); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// RestoredValidator checks application-specific snapshot boundaries before any
// committed tail is replayed. Metadata implements it; opaque load data does not.
type RestoredValidator interface {
	ValidateRestoredSnapshot(context.Context, uint64, uint64) error
}
