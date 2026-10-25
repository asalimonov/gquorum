package raftengine

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
)

// DiscardMachine validates managed load replay without invoking application code.
// It intentionally does not claim compatibility with an application's snapshot.
type DiscardMachine struct{}

// Restore discards opaque application state after the engine validates its envelope.
func (DiscardMachine) Restore(ctx context.Context, _ []byte) error { return ctx.Err() }

// Apply discards application commands while the shared engine interprets authority.
func (DiscardMachine) Apply(ctx context.Context, _ Command) (Result, error) {
	return Result{}, ctx.Err()
}

// Snapshot refuses reconstruction; inspection never produces an application image.
func (DiscardMachine) Snapshot(context.Context) ([]byte, error) { return nil, fault.ErrUnsupported }
