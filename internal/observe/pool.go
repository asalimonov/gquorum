package observe

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/fault"
)

// ReadPool retains ownership and its permit until a callback actually returns,
// even if its caller's deadline has elapsed. Keyed reads prevent overlapping
// calls to a stuck exact source across collection rounds.
type ReadPool struct {
	ctx      context.Context
	cancel   context.CancelFunc
	slots    chan struct{}
	mu       sync.Mutex
	inflight map[string]bool
	stopped  bool
	count    int
	joined   chan struct{}
}

// NewReadPool creates a bounded callback pool with exact-source exclusion.
func NewReadPool(limit int) (*ReadPool, error) {
	if limit <= 0 {
		return nil, fault.ErrInvalid
	}
	joined := make(chan struct{})
	close(joined)
	ctx, cancel := context.WithCancel(context.Background())
	return &ReadPool{
		ctx:      ctx,
		cancel:   cancel,
		slots:    make(chan struct{}, limit),
		inflight: map[string]bool{},
		joined:   joined,
	}, nil
}

// Do runs a read under a pool permit that remains owned until the callback
// returns.
func (p *ReadPool) Do(ctx context.Context, read func(context.Context) error) error {
	return p.DoKey(ctx, "", read)
}

// DoKey runs a bounded read and prevents overlap for a nonempty exact-source
// key.
func (p *ReadPool) DoKey(ctx context.Context, key string, read func(context.Context) error) error {
	if read == nil {
		return fault.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return fault.ErrStopped
	}
	p.mu.Lock()
	if p.stopped || key != "" && p.inflight[key] {
		stopped := p.stopped
		p.mu.Unlock()
		<-p.slots
		if stopped {
			return fault.ErrStopped
		}
		return fault.ErrOverloaded
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		<-p.slots
		return err
	}
	if p.count == 0 {
		p.joined = make(chan struct{})
	}
	p.count++
	if key != "" {
		p.inflight[key] = true
	}
	p.mu.Unlock()
	work, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(p.ctx, cancel)
	done := make(chan error, 1)
	go p.runRead(work, key, poolCallback{read: read, cancel: cancel, stopCancel: stopCancel, done: done})
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop cancels callbacks and waits for their actual return.
func (p *ReadPool) Stop(ctx context.Context) error {
	p.mu.Lock()
	p.stopped = true
	p.cancel()
	joined := p.joined
	p.mu.Unlock()
	select {
	case <-joined:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "metrics reads have not returned", ctx.Err())
	}
}

type poolCallback struct {
	read       func(context.Context) error
	cancel     context.CancelFunc
	stopCancel func() bool
	done       chan error
}

func (p *ReadPool) runRead(work context.Context, key string, callback poolCallback) {
	err := callback.read(work)
	callback.stopCancel()
	callback.cancel()
	<-p.slots
	p.mu.Lock()
	delete(p.inflight, key)
	p.count--
	if p.count == 0 {
		close(p.joined)
	}
	p.mu.Unlock()
	// A completed call must release its exact-source exclusion before its
	// caller can begin the next round. Timed-out callbacks still own it.
	callback.done <- err
}
