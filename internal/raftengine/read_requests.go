package raftengine

import (
	"bytes"
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type readRequest struct {
	ctx            context.Context
	barrier        bool
	native         bool
	authorityKey   *topology.AuthorityKey
	data           []byte
	reply          chan response
	index          uint64
	maxResultBytes int
}

type readMode uint8

const (
	queryRead readMode = iota
	barrierRead
)

// ReadBarrier confirms a fresh safe index and waits for this exact machine's
// application. It is not a reusable lease or an atomic read-modify-write guard.
func (g *Group) ReadBarrier(ctx context.Context) error {
	_, err := g.submitRead(ctx, nil, barrierRead)
	return err
}

// Read executes the optional read-only Query callback on the serialized machine
// owner after quorum confirmation and local application.
func (g *Group) Read(ctx context.Context, query []byte) (Result, error) {
	return g.submitRead(ctx, query, queryRead)
}

func (g *Group) submitRead(parent context.Context, query []byte, mode readMode) (Result, error) {
	return g.submitQuery(parent, query, mode, g.opts.Reads)
}

// ReadControl uses the same bounded queue, timeout and serialized Query owner,
// with at least the library's default byte limits. Small public response limits
// must not prevent internal topology convergence from reading its own catalog.
func (g *Group) ReadControl(parent context.Context, query []byte) (Result, error) {
	limits := (ReadOptions{}).withDefaults()
	limits.Timeout = g.opts.Reads.Timeout
	limits.MaxRequestBytes = max(limits.MaxRequestBytes, g.opts.Reads.MaxRequestBytes)
	limits.MaxResultBytes = max(limits.MaxResultBytes, g.opts.Reads.MaxResultBytes)
	return g.submitQuery(parent, query, queryRead, limits)
}

func (g *Group) submitQuery(parent context.Context, query []byte, mode readMode, limits ReadOptions) (Result, error) {
	if err := parent.Err(); err != nil {
		return Result{}, err
	}
	if len(query) > limits.MaxRequestBytes {
		return Result{}, fault.ErrOverloaded
	}
	if mode == queryRead {
		machine, ok := g.opts.Machine.(QueryMachine)
		if !ok || !machine.SupportsQuery() {
			return Result{}, fault.ErrUnsupported
		}
	}
	ctx, cancel := context.WithTimeout(parent, g.opts.Reads.Timeout)
	defer cancel()
	req := &readRequest{
		ctx:            ctx,
		barrier:        mode == barrierRead,
		data:           query,
		reply:          make(chan response, 1),
		maxResultBytes: limits.MaxResultBytes,
	}
	return g.waitRead(req)
}

func (g *Group) waitRead(req *readRequest) (Result, error) {
	ctx := req.ctx
	if err := g.enqueueRead(req); err != nil {
		return Result{}, err
	}
	select {
	case res := <-req.reply:
		return res.result, res.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-g.done:
		return Result{}, g.exitError()
	}
}

func (g *Group) enqueueRead(req *readRequest) error {
	// Serialize the admission/ownership handoff against the run-loop exit
	// drain. The bounded queue has a place for every acquired slot; never wait
	// under mu.
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.started || g.stopped {
		if g.err != nil {
			return g.err
		}
		return fault.ErrStopped
	}
	if err := req.ctx.Err(); err != nil {
		return err
	}
	select {
	case <-g.ready:
	default:
		return fault.ErrReplicaNotReady
	}
	select {
	case g.readSlots <- struct{}{}:
	default:
		return fault.ErrOverloaded
	}
	req.data = bytes.Clone(req.data)
	select {
	case g.reads <- req:
		return nil // engine owns slot and request from here
	default:
		<-g.readSlots
		return fault.ErrOverloaded
	}
}

// finishRead is owner-loop-only, including cancellation and shutdown. A caller
// leaving submitRead must not release an executing callback's slot.
func (g *Group) finishRead(req *readRequest, result Result, err error) {
	if req == nil {
		return
	}
	completed, failure := result, err
	if failure == nil {
		failure = req.ctx.Err()
	}
	if failure != nil {
		completed = Result{}
	} else {
		completed.Data = bytes.Clone(completed.Data)
	}
	req.reply <- response{result: completed, err: failure} // private one-result buffer
	req.data = nil
	<-g.readSlots
}

func (g *Group) drainReadQueue(err error) {
	for {
		select {
		case req := <-g.reads:
			g.finishRead(req, Result{}, err)
		default:
			return
		}
	}
}
