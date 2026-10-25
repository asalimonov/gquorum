package loadsnapshot

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
)

// Coordinator is the internal managed-owner integration point. The facade is
// introduced before its resource-owning implementation (plan tasks 6–8); these
// unavailable methods cannot accept work and are not attached to a Host yet.
type Coordinator struct{}

// BeginSnapshot refuses admission until coordinator ownership is implemented.
func (*Coordinator) BeginSnapshot(ctx context.Context, _ snapshot.BeginRequest) (snapshot.Ack, error) {
	return snapshot.Ack{}, unavailable(ctx)
}

// WriteSnapshot refuses bytes without an admitted owner.
func (*Coordinator) WriteSnapshot(ctx context.Context, _ snapshot.Chunk) (snapshot.Ack, error) {
	return snapshot.Ack{}, unavailable(ctx)
}

// FinishSnapshot never fabricates success for an unavailable owner.
func (*Coordinator) FinishSnapshot(ctx context.Context, _ snapshot.Session) (snapshot.Ack, error) {
	return snapshot.Ack{}, unavailable(ctx)
}

// DetachSnapshot requires an admitted exact attachment.
func (*Coordinator) DetachSnapshot(ctx context.Context, _ snapshot.Session) error {
	return unavailable(ctx)
}

// AbortSnapshot requires an admitted exact transfer.
func (*Coordinator) AbortSnapshot(ctx context.Context, _ snapshot.Session) error {
	return unavailable(ctx)
}

// SessionDone returns closed for every session before admission is available.
func (*Coordinator) SessionDone(_ snapshot.Session) <-chan struct{} { return closedSessionSignal }

// List returns empty observations before a coordinator can own work.
func (*Coordinator) List(_ snapshot.Query) snapshot.Page { return snapshot.Page{} }

func unavailable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fault.ErrReplicaNotReady
}
