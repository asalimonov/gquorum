package metaservice

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type documentWatcher struct {
	view    topology.View
	version uint64
	ch      chan topology.Update
}
type watchRegistry struct {
	mu     sync.Mutex
	limit  int
	closed bool
	done   chan struct{}
	subs   map[*documentWatcher]struct{}
	wg     sync.WaitGroup
}

func newWatchRegistry(limit int) *watchRegistry {
	return &watchRegistry{limit: limit, done: make(chan struct{}), subs: make(map[*documentWatcher]struct{})}
}

// Watch returns an owned initial locally applied view and then monotonically
// increasing versions. Its single-slot buffer coalesces intermediate versions:
// a slow observer never blocks Raft apply. Execution progress is available via
// Execution/WaitExecution and the event bus, independently of document versions.
func (g *Group) Watch(ctx context.Context, view topology.View) (<-chan topology.Update, error) {
	if view != topology.Latest && view != topology.Target && view != topology.Active {
		return nil, fault.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.RLock()
	stopping := g.lifecycle == "stopping" || g.lifecycle == "stopped"
	g.mu.RUnlock()
	if stopping {
		return nil, fault.ErrStopped
	}
	return g.watches.watch(ctx, view, func() topology.Document {
		latest, target, active := g.state().Views()
		switch view {
		case topology.Target:
			return target
		case topology.Active:
			return active
		default:
			return latest
		}
	})
}
func (r *watchRegistry) watch(ctx context.Context, view topology.View, current func() topology.Document) (<-chan topology.Update, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.closed {
		return nil, fault.ErrStopped
	}
	if len(r.subs) >= r.limit {
		return nil, fault.ErrOverloaded
	}
	w := &documentWatcher{view: view, ch: make(chan topology.Update, 1)}
	// Seed and install under the publication lock. A callback that captured an
	// earlier FSM view cannot repeat/regress this seed when it obtains the lock.
	doc := current()
	if doc.Version > 0 {
		w.version = doc.Version
		w.ch <- topology.Update{View: view, Document: doc.Clone()}
	}
	r.subs[w] = struct{}{}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		select {
		case <-ctx.Done():
		case <-r.done:
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if _, exists := r.subs[w]; exists {
			delete(r.subs, w)
			close(w.ch)
		}
	}()
	return w.ch, nil
}
func (r *watchRegistry) publish(latest, target, active topology.Document) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	for w := range r.subs {
		doc := latest
		switch w.view {
		case topology.Target:
			doc = target
		case topology.Active:
			doc = active
		}
		if doc.Version <= w.version {
			continue
		}
		w.version = doc.Version
		update := topology.Update{View: w.view, Document: doc.Clone()}
		select {
		case w.ch <- update:
			continue
		default:
		}
		select {
		case <-w.ch:
		default:
		}
		select {
		case w.ch <- update:
		default:
		}
	}
}
func (r *watchRegistry) close() {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.done)
		for w := range r.subs {
			delete(r.subs, w)
			close(w.ch)
		}
	}
	r.mu.Unlock()
	r.wg.Wait()
}
