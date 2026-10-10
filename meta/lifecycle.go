package meta

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
)

// joinContext gives each owned call both its caller cancellation and the owning
// worker's cancellation. Cleanup joins the cancellation hook before release.
func joinContext(parent, owner context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	stop := context.AfterFunc(owner, func() { cancel(); close(done) })
	return ctx, func() {
		if !stop() {
			<-done
		}
		cancel()
	}
}
func (g *Group) beginRPC(parent context.Context) (context.Context, func(), error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	g.mu.Lock()
	if !g.started || g.rpcClosed || g.ctx.Err() != nil {
		g.mu.Unlock()
		return nil, nil, fault.ErrStopped
	}
	g.rpcWG.Add(1)
	owner := g.ctx
	g.mu.Unlock()
	ctx, cleanup := joinContext(parent, owner)
	return ctx, func() { cleanup(); g.rpcWG.Done() }, nil
}
func (g *Group) beginEffect(parent context.Context) (context.Context, func(), error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	g.mu.Lock()
	if !g.started || g.quiescing || g.selfRemoval != nil || g.ctx.Err() != nil {
		g.mu.Unlock()
		return nil, nil, fault.ErrStopped
	}
	g.effectWG.Add(1)
	owner := g.workCtx
	g.mu.Unlock()
	ctx, cleanup := joinContext(parent, owner)
	return ctx, func() { cleanup(); g.effectWG.Done() }, nil
}

// Quiesce refuses new mutations, cancels and joins topology/effect workers,
// attempts bounded deactivation, then stops networking and Raft. It keeps the
// meta database open so callers can stop their load runtimes before final Stop.
// Repeated calls wait for the same work. Timeout preserves resource ownership.
func (g *Group) Quiesce(ctx context.Context) error {
	g.requestQuiesce()
	select {
	case <-g.quiesced:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "meta quiescence has not completed", ctx.Err())
	}
}
func (g *Group) requestQuiesce() {
	g.quiesceOnce.Do(func() {
		g.mu.Lock()
		g.quiescing = true
		if g.terminal == nil {
			g.lifecycle = "quiescing"
		}
		if g.workCancel != nil {
			g.workCancel()
		}
		g.mu.Unlock()
		go g.finishQuiesce()
	})
}
func (g *Group) finishQuiesce() {
	// Serialize component startup and shutdown. No Add/Start can occur after any
	// component has been joined, even when Close races with the first Start.
	g.startGate.Lock()
	defer g.startGate.Unlock()
	if g.watches != nil {
		g.watches.close()
	}
	if g.observer != nil {
		_ = g.observer.Stop(context.Background())
	}
	if g.reconciler != nil {
		_ = g.reconciler.Stop(context.Background())
	}
	if g.convergence != nil {
		g.convergence.Stop()
	}
	g.effectWG.Wait()
	g.wg.Wait()
	g.deactivateForShutdown()
	g.mu.Lock()
	g.rpcClosed = true
	if g.cancel != nil {
		g.cancel()
	}
	g.mu.Unlock()
	if g.transport != nil {
		_ = g.transport.Stop(context.Background())
	}
	g.rpcWG.Wait()
	if engine := g.engine.Load(); engine != nil {
		_ = engine.Stop(context.Background())
	}
	g.removalWG.Wait()
	g.mu.Lock()
	if g.terminal == nil {
		g.lifecycle = "quiescent"
	}
	g.mu.Unlock()
	close(g.quiesced)
}
func (g *Group) deactivateForShutdown() {
	g.sessionMu.Lock()
	id := g.ownSession
	if id != "" {
		g.ownSession = ""
		g.lastDeactivated = id
	}
	g.sessionPaused = true
	g.ownDeadline = time.Time{}
	g.sessionMu.Unlock()
	g.mu.RLock()
	running := g.started && g.ctx != nil && g.ctx.Err() == nil
	g.mu.RUnlock()
	if id == "" || !running {
		return
	}
	// Best effort: quorum loss cannot prevent safe local quiescence, and it does
	// not authorize a fabricated revoke. Expiry handles an uncommitted revoke.
	ctx, cancel := context.WithTimeout(g.ctx, 2*time.Second)
	defer cancel()
	if _, err := g.submitProposal(ctx, metaengine.RevokeSession{SessionID: id}, true); err != nil && g.opts.Logger != nil {
		g.opts.Logger.Debug("meta shutdown deactivation was not acknowledged", "error", err)
	}
}
func (g *Group) autoStopNeeded() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.terminal != nil || g.ctx != nil && g.ctx.Err() != nil
}

// RequestStop initiates Stop without waiting. Lifecycle callbacks can schedule
// teardown with this method and return, allowing Stop to join those callbacks.
func (g *Group) RequestStop() {
	g.stopOnce.Do(func() {
		g.requestQuiesce()
		go func() {
			<-g.quiesced
			g.mu.Lock()
			if g.store != nil {
				if err := g.store.Close(); err != nil && g.terminal == nil {
					g.terminal = err
				}
			}
			if g.terminal == nil {
				g.lifecycle = "stopped"
			}
			g.mu.Unlock()
			close(g.stopped)
		}()
	})
}

// Stop completes quiescence and closes the owned meta database. Caller
// controllers, resolver, bus and shared load storage remain caller-owned.
func (g *Group) Stop(ctx context.Context) error {
	g.RequestStop()
	select {
	case <-g.stopped:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "meta workers have not stopped", ctx.Err())
	}
}

func (g *Group) startFailed(err error) error {
	g.mu.RLock()
	stopping := g.quiescing || g.ctx != nil && g.ctx.Err() != nil
	g.mu.RUnlock()
	if stopping {
		g.RequestStop()
		return fault.ErrStopped
	}
	g.fail(err)
	g.RequestStop()
	return err
}
