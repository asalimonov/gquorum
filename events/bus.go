package events

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

const (
	defaultBusBuffer = 1024
)

// BusOptions configures the publication buffer, optional journal and logger.
type BusOptions struct {
	Journal *Journal
	Buffer  int
	Logger  *slog.Logger
}
type subscriber struct {
	filter Filter
	ch     chan Delivery
	lost   uint64
}

// Bus dispatches local events to bounded subscriptions and an optional journal.
type Bus struct {
	mu                 sync.Mutex
	opts               BusOptions
	in                 chan Event
	subs               map[*subscriber]struct{}
	closed             bool
	dropped            atomic.Uint64
	dispatchDone, done chan struct{}
	wg                 sync.WaitGroup
}

// NewBus validates options and starts the event dispatch workers.
func NewBus(opts BusOptions) (*Bus, error) {
	if opts.Buffer < 0 {
		return nil, fault.ErrInvalid
	}
	if opts.Buffer == 0 {
		opts.Buffer = defaultBusBuffer
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	b := &Bus{
		opts:         opts,
		in:           make(chan Event, opts.Buffer),
		subs:         make(map[*subscriber]struct{}),
		dispatchDone: make(chan struct{}),
		done:         make(chan struct{}),
	}
	b.wg.Add(1)
	go b.dispatch()
	go func() { b.wg.Wait(); close(b.done) }()
	return b, nil
}

// Publish queues an owned event copy and reports whether it was accepted.
func (b *Bus) Publish(e Event) bool {
	if b == nil {
		return false
	}
	event := clone(e)
	event.Seq = 0
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	select {
	case b.in <- event:
		return true
	default:
		b.dropped.Add(1)
		for s := range b.subs {
			s.lost++
		}
		return false
	}
}

// Subscribe creates a filtered delivery stream that closes when its context or
// the bus stops.
func (b *Bus) Subscribe(ctx context.Context, f Filter, capacity int) (<-chan Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if capacity <= 0 {
		return nil, fault.ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fault.ErrStopped
	}
	f.Types = slices.Clone(f.Types)
	s := &subscriber{filter: f, ch: make(chan Delivery, capacity)}
	b.subs[s] = struct{}{}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		select {
		case <-ctx.Done():
		case <-b.dispatchDone:
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs, s)
		close(s.ch)
	}()
	return s.ch, nil
}
func (b *Bus) dispatch() {
	defer b.wg.Done()
	defer close(b.dispatchDone)
	var lastWarn time.Time
	for e := range b.in {
		if b.opts.Journal != nil {
			persisted, err := b.opts.Journal.Append(e)
			if err == nil {
				e = persisted
			} else if time.Since(lastWarn) >= time.Minute {
				b.opts.Logger.Warn("event journal append failed", "error", err)
				lastWarn = time.Now()
			}
		}
		b.mu.Lock()
		for s := range b.subs {
			if !s.filter.matches(e) {
				continue
			}
			select {
			case s.ch <- Delivery{Event: clone(e), LostBefore: s.lost}:
				s.lost = 0
			default:
				s.lost++
			}
		}
		b.mu.Unlock()
	}
}

// Dropped returns the number of publications rejected by a full input buffer.
func (b *Bus) Dropped() uint64 {
	if b == nil {
		return 0
	}
	return b.dropped.Load()
}

// Close drains accepted events and joins dispatch/subscription workers. The
// caller owns Journal.Close.
func (b *Bus) Close(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.in)
	}
	b.mu.Unlock()
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "event dispatch has not stopped", ctx.Err())
	}
}
