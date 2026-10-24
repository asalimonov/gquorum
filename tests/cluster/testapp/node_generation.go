package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/util"
)

// A generation remains installed through request cancellation, callback joining
// and Node teardown. A later generation never receives an old generation's work.
type nodeGeneration struct {
	node               gquorum.Node
	listener           net.Listener
	ctx                context.Context
	cancel             context.CancelFunc
	accepting, stopped bool
	effects, workers   sync.WaitGroup
	joinOnce           sync.Once
	joined             chan struct{}
}

func (a *application) currentNode() gquorum.Node {
	a.generationMu.RLock()
	defer a.generationMu.RUnlock()
	if a.generation != nil {
		return a.generation.node
	}
	return a.node // compatibility with focused fixtures that supply an inert Node
}
func (a *application) currentCluster() gquorum.Cluster {
	if n := a.currentNode(); n != nil {
		return n.Cluster()
	}
	return a.cluster
}
func (a *application) makeNodeGeneration(n gquorum.Node, listener net.Listener) *nodeGeneration {
	ctx, cancel := context.WithCancel(a.workCtx)
	return &nodeGeneration{node: n, listener: listener, ctx: ctx, cancel: cancel, accepting: true, joined: make(chan struct{})}
}
func (a *application) startNodeGeneration() error {
	a.nodeOpMu.Lock()
	defer a.nodeOpMu.Unlock()
	return a.startGeneration(a.generation)
}
func (a *application) startGeneration(g *nodeGeneration) error {
	if err := g.node.Start(a.lifeCtx); err != nil {
		return err
	}
	g.workers.Go(func() { a.markEstablishedFor(g.ctx, g.node) })
	g.workers.Go(func() { a.observeServingContext(g.ctx) })
	if a.cfg.Bootstrap {
		g.workers.Go(func() { a.bootstrapContext(g.ctx, g.node) })
	}
	return nil
}
func (a *application) stopNodeGeneration(ctx context.Context) error {
	a.nodeOpMu.Lock()
	defer a.nodeOpMu.Unlock()
	a.generationMu.Lock()
	g := a.generation
	if g == nil {
		a.generationMu.Unlock()
		if a.node != nil {
			return a.node.Stop(ctx)
		}
		return nil
	}
	g.accepting = false
	g.cancel()
	a.generationMu.Unlock()
	g.joinOnce.Do(func() { go func() { g.effects.Wait(); g.workers.Wait(); close(g.joined) }() })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.joined:
	}
	if err := g.node.Stop(ctx); err != nil {
		return err
	}
	// A failed Start may never have transferred this listener to Node.
	if err := g.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	a.generationMu.Lock()
	g.stopped = true
	a.generationMu.Unlock()
	return nil
}

func (a *application) restartNodeGeneration(ctx context.Context) error {
	a.nodeOpMu.Lock()
	defer a.nodeOpMu.Unlock()
	if !a.cfg.NodeOwnedWAL {
		return fault.ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.generationMu.RLock()
	old := a.generation
	stopped := old != nil && old.stopped
	a.generationMu.RUnlock()
	if !stopped {
		return fault.ErrUnavailable
	}
	select {
	case <-a.closing:
		return fault.ErrStopped
	default:
	}
	opts := a.nodeOptions
	opts.Identity = old.node.Identity()
	opts.Identity.InstanceID = util.NewInstanceID()
	// This is a retained restart. The retained pin, not old founding intent,
	// supplies membership; explicit joining keeps its configured seed addresses.
	opts.Bootstrap.Founders = nil
	address := a.nodeOptions.Meta.Network.Listener.Addr().String()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	opts.Meta.Network.Listener = listener
	n, err := gquorum.NewNode(opts)
	if err != nil {
		_ = listener.Close()
		return err
	}
	next := a.makeNodeGeneration(n, listener)
	a.mu.Lock()
	a.serving = make(map[string]topology.GroupProgress)
	a.startupObserved = false
	a.startupMissing = ""
	a.mu.Unlock()
	a.generationMu.Lock()
	a.generation = next
	a.generationMu.Unlock()
	if a.metaProxy != nil {
		a.metaProxy.internal = listener
	}
	return a.startGeneration(next)
}

// Management routes remain available without an active participant. They are
// process effects, so stopping a Node never waits on its own HTTP stop request.
func managementRoute(path string) bool {
	return path == "/health/live" || strings.HasPrefix(path, "/internal/test/node/") || path == "/internal/test/runtime" || path == "/internal/test/resources"
}
func (a *application) registerNodeGenerationRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("POST /internal/test/node/stop", protect(func(w http.ResponseWriter, r *http.Request) {
		if !a.cfg.NodeOwnedWAL {
			httpError(w, fault.ErrUnsupported)
			return
		}
		if err := a.stopNodeGeneration(r.Context()); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, a.currentNode().Status())
	}))
	mux.HandleFunc("POST /internal/test/node/start", protect(func(w http.ResponseWriter, r *http.Request) {
		if err := a.restartNodeGeneration(r.Context()); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, a.currentNode().Status())
	}))
	mux.HandleFunc("GET /internal/test/node/status", protect(func(w http.ResponseWriter, _ *http.Request) {
		jsonResponse(w, http.StatusOK, a.currentNode().Status())
	}))
}
