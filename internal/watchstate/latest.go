// Package watchstate provides bounded, local current-state subscriptions.
package watchstate

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/fault"
)

// Latest owns a current value and one buffered value per subscriber. Publishers
// never wait for consumers. clone must return an owned value and must not call
// p. New starts no workers; Finish closes subscriptions and Join joins their
// workers.
type Latest[T any] struct {
	mu          sync.Mutex
	limit       int
	current     T
	clone       func(T) T
	subscribers map[chan T]struct{}
	finished    bool
	finish      chan struct{}
	joined      chan struct{}
	workers     int
}

// New constructs a bounded coalescing watcher set with the supplied value-copy
// function.
func New[T any](limit int, initial T, clone func(T) T) *Latest[T] {
	return &Latest[T]{limit: limit, current: clone(initial), clone: clone,
		subscribers: make(map[chan T]struct{}), finish: make(chan struct{}), joined: make(chan struct{})}
}

// Subscribe atomically registers and seeds the latest owned value. A terminal
// publisher returns its final value in a closed channel without starting work.
func (p *Latest[T]) Subscribe(ctx context.Context) (<-chan T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !p.finished && p.workers >= p.limit {
		return nil, fault.ErrOverloaded
	}
	ch := make(chan T, 1)
	ch <- p.clone(p.current)
	if p.finished {
		close(ch)
		return ch, nil
	}
	p.subscribers[ch] = struct{}{}
	p.workers++
	go func() {
		select {
		case <-ctx.Done():
		case <-p.finish:
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, exists := p.subscribers[ch]; exists {
			delete(p.subscribers, ch)
			close(ch)
		}
		p.workers--
		if p.finished && p.workers == 0 {
			close(p.joined)
		}
	}()
	return ch, nil
}

// Publish replaces any older unread snapshot. Calls after Finish are ignored.
func (p *Latest[T]) Publish(value T) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.publishLocked(value)
}

func (p *Latest[T]) publishLocked(value T) {
	p.current = p.clone(value)
	for ch := range p.subscribers {
		select {
		case <-ch:
		default:
		}
		ch <- p.clone(value)
	}
}

// Finish retains one final snapshot, closes channels, and wakes owned workers.
func (p *Latest[T]) Finish(value T) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.finished = true
	p.publishLocked(value)
	for ch := range p.subscribers {
		close(ch)
		delete(p.subscribers, ch)
	}
	close(p.finish)
	if p.workers == 0 {
		close(p.joined)
	}
}

// Join waits for Finish and all subscription cancellation workers.
func (p *Latest[T]) Join(ctx context.Context) error {
	select {
	case <-p.joined:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
