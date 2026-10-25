//revive:disable:package-naming Recovery scopes this runtime; callers import it as recoveryruntime.
package runtime

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
)

type publicationCall struct {
	context context.Context
	release func()
}

func (c *Coordinator) acquirePublication(ctx context.Context, identity topology.Identity) (publicationCall, error) {
	if err := ctx.Err(); err != nil {
		return publicationCall{}, err
	}
	if err := c.checkIdentity(identity); err != nil {
		return publicationCall{}, err
	}
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return publicationCall{}, ctx.Err()
	}
	release := func() { <-c.gate }
	if !c.DataReady() || c.config.Meta.Identity() != identity {
		release()
		return publicationCall{}, fault.ErrReplicaNotReady
	}
	owned, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	return publicationCall{context: owned, release: func() { stop(); cancel(); release() }}, nil
}

// ReadyToPublish exposes recovered data for the existing serialized readiness hook.
func (c *Coordinator) ReadyToPublish(ctx context.Context, identity topology.Identity) error {
	call, err := c.acquirePublication(ctx, identity)
	if err != nil {
		return err
	}
	defer call.release()
	if c.config.Host != nil {
		if err := c.config.Host.PublishRecovery(call.context, identity); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.published = !c.stopped && call.context.Err() == nil
	published := c.published
	c.mu.Unlock()
	if !published {
		return fault.ErrStopped
	}
	return nil
}

// CommitLocalCompletion persists completion before releasing application background work.
func (c *Coordinator) CommitLocalCompletion(ctx context.Context, identity topology.Identity) error {
	call, err := c.acquirePublication(ctx, identity)
	if err != nil {
		return err
	}
	defer call.release()
	c.mu.Lock()
	published := c.published
	c.mu.Unlock()
	if !published {
		return fault.ErrReplicaNotReady
	}
	if c.config.Host != nil {
		if err := c.config.Host.FinishRecovery(call.context, identity); err != nil {
			return err
		}
	}
	if c.config.Journal.View().Phase != model.Complete {
		if err := c.config.Journal.SetPhase(call.context, model.Complete, nil); err != nil {
			return err
		}
	}
	if err := call.context.Err(); err != nil {
		return err
	}
	if c.config.Host != nil {
		return c.config.Host.ReleaseRecoveryRun(call.context, identity)
	}
	return nil
}
