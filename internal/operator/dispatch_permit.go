package operator

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
)

// A permit owns one bounded worker before grant issuance or target resolution.
// Its optional host and exact key are installed in the shared registry as known.
type dispatchPermit struct {
	run     *controllerRun
	host    dispatchHost
	key     dispatchRequest
	bound   bool
	cleanup bool
}

func (e *Controller) reserveDispatch(ctx context.Context, request dispatchPermit) (*dispatchPermit, error) {
	run, host := request.run, request.host
	if run == nil || run.dispatch == nil {
		return nil, fault.ErrStopped
	}
	d := run.dispatch
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if !e.scoped.IsMetaLeader() {
		return nil, fault.ErrNotLeader
	}
	limits := e.rules.Limits.WithDefaults()
	if request.cleanup && d.cleanup >= limits.MaxConcurrentCleanup ||
		!request.cleanup && d.ordinary >= limits.MaxConcurrentGroups {
		return nil, fault.ErrUnavailable
	}
	now := e.clock.Now()
	d.pruneBackoff(now, e.maxRetry)
	if d.hosts[host] || now.Before(d.backoff[host].until) {
		return nil, fault.ErrUnavailable
	}
	if d.hosts == nil {
		d.hosts = make(map[dispatchHost]bool)
		d.requests = make(map[dispatchRequest]bool)
	}
	d.hosts[host] = true
	if request.cleanup {
		d.cleanup++
	} else {
		d.ordinary++
	}
	d.workers.Add(1)
	return &request, nil
}

func (e *Controller) bindRequest(ctx context.Context, permit *dispatchPermit, request effectDispatch) error {
	d := permit.run.dispatch
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || ctx.Err() != nil {
		return fault.ErrStopped
	}
	if !e.scoped.IsMetaLeader() {
		return fault.ErrNotLeader
	}
	if permit.bound || permit.host.group != request.host().group ||
		permit.host.node != "" && permit.host != request.host() {
		return fault.ErrOperationAuthorization
	}
	if d.requests[request.key()] {
		return fault.ErrUnavailable
	}
	if permit.host.node == "" {
		host := request.host()
		if d.hosts[host] || e.clock.Now().Before(d.backoff[host].until) {
			return fault.ErrUnavailable
		}
		delete(d.hosts, permit.host)
		d.hosts[host], permit.host = true, host
	}
	permit.key, permit.bound = request.key(), true
	d.requests[permit.key] = true
	if permit.cleanup && request.grant.Action != nil {
		e.rememberDispatch(permit, permit.key)
	}
	return nil
}

// Caller holds the dispatch mutex. The existing bounded retry row also retains
// the last admitted target, so a timed-out historical identity cannot monopolize
// another identity on the same gate when the retry deadline expires.
func (e *Controller) rememberDispatch(permit *dispatchPermit, key dispatchRequest) {
	d := permit.run.dispatch
	limit := max(e.rules.Limits.WithDefaults().MaxConcurrentGroups, e.rules.Limits.WithDefaults().MaxConcurrentCleanup)
	if _, exists := d.backoff[permit.host]; !exists && len(d.backoff) >= limit {
		d.evictOldestBackoff()
	}
	if d.backoff == nil {
		d.backoff = make(map[dispatchHost]dispatchBackoff)
	}
	row := d.backoff[permit.host]
	row.last, row.updated = key, e.clock.Now()
	d.backoff[permit.host] = row
}

func (e *Controller) releaseDispatch(permit *dispatchPermit, err error) {
	d := permit.run.dispatch
	d.mu.Lock()
	delete(d.hosts, permit.host)
	if permit.bound {
		delete(d.requests, permit.key)
	}
	if permit.cleanup {
		d.cleanup--
	} else {
		d.ordinary--
	}
	if err != nil && !d.closed {
		e.backoffDispatch(d, permit.host)
	} else {
		delete(d.backoff, permit.host)
	}
	d.mu.Unlock()
	d.workers.Done()
}
