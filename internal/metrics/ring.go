package metrics

import (
	"math"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

const (
	defaultHistoryPageSize = 100
)

type bucket[T any] struct {
	at    time.Time
	seq   uint64
	value T
}

// Ring adapts Meridian's bounded history. Append and read copy nested values
// with the supplied clone; reads evict expired entries without adding samples.
type Ring[T any] struct {
	mu         sync.Mutex
	capacity   int
	retention  time.Duration
	clone      func(T) T
	slots      []bucket[T]
	head, size int
	sequence   uint64
}

// NewRing validates bounded history settings and the value-copy function.
func NewRing[T any](capacity int, retention time.Duration, clone func(T) T) (*Ring[T], error) {
	if capacity <= 0 || retention <= 0 || clone == nil {
		return nil, fault.ErrInvalid
	}
	return &Ring[T]{capacity: capacity, retention: retention, clone: clone}, nil
}

func (r *Ring[T]) expire(now time.Time) {
	for r.size > 0 && Age(now, r.slots[r.head].at) >= r.retention {
		r.slots[r.head] = bucket[T]{}
		r.head = (r.head + 1) % len(r.slots)
		r.size--
	}
}

// Roll expires old points and appends an owned value under a fresh history
// cursor.
func (r *Ring[T]) Roll(now time.Time, value T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expire(now)
	// Never reuse a history cursor if its unsigned sequence is exhausted.
	if r.sequence == math.MaxUint64 {
		return
	}
	r.sequence++
	if r.size == len(r.slots) && len(r.slots) < r.capacity {
		n := min(r.capacity, max(1, len(r.slots)*2))
		if n <= len(r.slots) {
			n = r.capacity
		}
		grown := make([]bucket[T], n)
		for i := range r.size {
			grown[i] = r.slots[(r.head+i)%len(r.slots)]
		}
		r.slots, r.head = grown, 0
	}
	if r.size == r.capacity {
		r.slots[r.head] = bucket[T]{now, r.sequence, r.clone(value)}
		r.head = (r.head + 1) % len(r.slots)
		return
	}
	r.slots[(r.head+r.size)%len(r.slots)] = bucket[T]{now, r.sequence, r.clone(value)}
	r.size++
}

// HistoryBatch contains an owned page and its cursor continuity information.
type HistoryBatch[T any] struct {
	Points    []T
	Next      uint64
	Truncated bool
}

// Snapshot copies a bounded page after a cursor and reports whether earlier
// points were lost.
func (r *Ring[T]) Snapshot(now time.Time, after uint64, limit int) HistoryBatch[T] {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expire(now)
	pageSize := limit
	if pageSize <= 0 {
		pageSize = min(defaultHistoryPageSize, r.capacity)
	}
	pageSize = min(pageSize, r.capacity)
	out := make([]T, 0, min(pageSize, r.size))
	next, truncated := after, false
	if r.size == 0 {
		return HistoryBatch[T]{Points: out, Next: max(after, r.sequence), Truncated: after < r.sequence}
	}
	oldest := r.slots[r.head].seq
	truncated = after < oldest-1
	for i := 0; i < r.size && len(out) < pageSize; i++ {
		b := r.slots[(r.head+i)%len(r.slots)]
		if b.seq > after {
			out = append(out, r.clone(b.value))
			next = b.seq
		}
	}
	return HistoryBatch[T]{Points: out, Next: next, Truncated: truncated}
}
