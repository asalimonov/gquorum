package operator

import (
	"context"
	"sync"
	"time"
)

type dispatchHost struct{ group, node string }

type dispatchBackoff struct {
	until, updated time.Time
	delay          time.Duration
}

// Each Start owns a new dispatcher. Admission and WaitGroup.Add share mu;
// close seals admission before any waiter can observe a zero worker count.
type controllerDispatch struct {
	mu                sync.Mutex
	closed            bool
	workers           sync.WaitGroup
	groups            map[string]bool
	backoff           map[dispatchHost]dispatchBackoff
	hosts             map[dispatchHost]bool
	requests          map[dispatchRequest]bool
	ordinary, cleanup int
}

func (d *controllerDispatch) close() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
}

func (d *controllerDispatch) available(limit int) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0
	}
	return max(0, limit-len(d.groups))
}

func (e *Controller) dispatchWork(ctx context.Context, d *controllerDispatch, work WorkItem) {
	if _, err := requiredWorkBlock(work); err != nil || !e.ownsGroup(work.GroupID) {
		return
	}
	host := dispatchHost{group: work.GroupID, node: scopedWorkStep(work).NodeName}
	limits := e.rules.Limits.WithDefaults()
	d.mu.Lock()
	if d.closed || d.groups[work.GroupID] || len(d.groups) >= limits.MaxConcurrentGroups ||
		ctx.Err() != nil || !e.scoped.IsMetaLeader() {
		d.mu.Unlock()
		return
	}
	now := e.clock.Now()
	d.pruneBackoff(now, e.maxRetry)
	if now.Before(d.backoff[host].until) {
		d.mu.Unlock()
		return
	}
	if d.groups == nil {
		d.groups = make(map[string]bool)
	}
	d.groups[work.GroupID] = true
	d.workers.Add(1)
	d.mu.Unlock()
	go e.runDispatchWork(ctx, d, work)
}

func (e *Controller) runDispatchWork(ctx context.Context, d *controllerDispatch, work WorkItem) {
	defer d.workers.Done()
	bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	defer cancel()
	outcome := e.runScopedWork(bounded, work)
	if bounded.Err() != nil {
		outcome = controllerWaiting
	}
	host := dispatchHost{group: work.GroupID, node: scopedWorkStep(work).NodeName}
	d.mu.Lock()
	delete(d.groups, work.GroupID)
	if !d.closed && outcome == controllerWaiting {
		e.backoffDispatch(d, host)
	} else {
		delete(d.backoff, host)
	}
	d.mu.Unlock()
	if outcome == controllerProgressed {
		e.Signal()
	}
}

func (e *Controller) backoffDispatch(d *controllerDispatch, host dispatchHost) {
	now := e.clock.Now()
	d.pruneBackoff(now, e.maxRetry)
	previous := d.backoff[host]
	delay := e.interval
	if previous.delay != 0 {
		delay = e.nextDelay(controllerWaiting, previous.delay)
	}
	limit := max(e.rules.Limits.WithDefaults().MaxConcurrentGroups, e.rules.Limits.WithDefaults().MaxConcurrentCleanup)
	if _, exists := d.backoff[host]; !exists && len(d.backoff) >= limit {
		d.evictOldestBackoff()
	}
	if d.backoff == nil {
		d.backoff = make(map[dispatchHost]dispatchBackoff)
	}
	d.backoff[host] = dispatchBackoff{until: now.Add(delay), updated: now, delay: delay}
}

func (d *controllerDispatch) pruneBackoff(now time.Time, idle time.Duration) {
	for key, row := range d.backoff {
		if !now.Before(row.updated.Add(idle)) {
			delete(d.backoff, key)
		}
	}
}

func (d *controllerDispatch) evictOldestBackoff() {
	var oldest dispatchHost
	var at time.Time
	for key, row := range d.backoff {
		if at.IsZero() || row.updated.Before(at) {
			oldest, at = key, row.updated
		}
	}
	delete(d.backoff, oldest)
}
