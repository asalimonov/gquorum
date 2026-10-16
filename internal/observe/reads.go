package observe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReadAttempt marks the boundary after address/identity validation and before
// the actual replica read. Waiting for a pool slot or resolving an address is
// not a reachability attempt against that replica.
type ReadAttempt struct{ dispatched atomic.Bool }

// Dispatch records that an exact read attempt reached transport dispatch.
func (a *ReadAttempt) Dispatch() { a.dispatched.Store(true) }

// ReplicaRead binds a metrics read callback to an exact replica and local-hold
// observation.
type ReplicaRead struct {
	Identity metrics.ReplicaIdentity
	Read     func(context.Context, *ReadAttempt) (Probe, error)
	Error    error
}

func (r ReplicaRead) key() string {
	id := r.Identity
	return fmt.Sprintf("%q/%q/%q/%d/%q", id.ClusterID, id.GroupID, id.NodeName, id.ReplicaID, id.InstanceID)
}
func failedReachability(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, fault.ErrIdentityMismatch) ||
		errors.Is(err, fault.ErrInvalid) ||
		errors.Is(err, fault.ErrOperationAuthorization) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) || status.Code(err) == codes.Unavailable ||
		status.Code(err) == codes.DeadlineExceeded
}

// CollectReads rotates across all groups, with only a bounded number of waiting
// dispatchers. The pool continues owning timed-out callbacks until actual
// return.
// Returned outcomes and callbacks share no mutable result buffers.
func CollectReads(ctx context.Context, pool *ReadPool, reads []ReplicaRead, options ReadOptions) ([]Probe, int) {
	start, parallel, maxAge := options.Start, options.Parallel, options.MaxAge
	out := make([]Probe, len(reads))
	for i, r := range reads {
		out[i] = Probe{Identity: r.Identity, Reachability: "unknown", Error: r.Error}
	}
	if len(reads) == 0 || parallel <= 0 {
		return out, 0
	}
	cursor := ((start % len(reads)) + len(reads)) % len(reads)
	round := readRound{ctx: ctx, pool: pool, reads: reads, out: out, cursor: cursor, maxAge: maxAge}
	var wg sync.WaitGroup
	for range min(parallel, len(reads)) {
		wg.Go(round.dispatch)
	}
	wg.Wait()
	// Rotate even when no work could acquire a permit. This prevents a
	// permanently
	// slow first group from always consuming the entire next round budget.
	advance := boundedReadCount(round.next.Load(), len(reads))
	if advance == 0 || advance == len(reads) {
		advance = 1
	}
	return out, (cursor + advance) % len(reads)
}

func boundedReadCount(count uint64, limit int) int {
	//nolint:gosec // limit is a nonnegative slice length.
	if count >= uint64(limit) {
		return limit
	}
	//nolint:gosec // count is checked against the slice length above.
	return int(count)
}

type readRound struct {
	ctx    context.Context
	pool   *ReadPool
	reads  []ReplicaRead
	out    []Probe
	cursor int
	maxAge time.Duration
	next   atomic.Uint64
}

type readDispatch struct {
	read     ReplicaRead
	previous Probe
	maxAge   time.Duration
}

func (round *readRound) dispatch() {
	for {
		if round.ctx.Err() != nil {
			return
		}
		sequence := round.next.Add(1) - 1
		if sequence >= uint64(len(round.reads)) {
			return
		}
		//nolint:gosec // sequence is checked against len(round.reads) above.
		offset := int(sequence)
		index := (round.cursor + offset) % len(round.reads)
		read := round.reads[index]
		if read.Read == nil {
			continue
		}
		round.out[index] = collectReplicaRead(round.ctx, round.pool,
			readDispatch{read: read, previous: round.out[index], maxAge: round.maxAge})
	}
}

func collectReplicaRead(ctx context.Context, pool *ReadPool, input readDispatch) Probe {
	attempt := &ReadAttempt{}
	result := make(chan Probe, 1)
	err := pool.DoKey(ctx, input.read.key(), func(work context.Context) error {
		began := time.Now()
		probe, err := input.read.Read(work, attempt)
		probe.Identity = input.read.Identity
		probe.Observation = internalmetrics.AgeObservation(probe.Observation, time.Since(began), input.maxAge)
		result <- probe
		return err
	})
	probe := input.previous
	if err == nil {
		probe = <-result
	} else {
		probe.Error = err
	}
	probe.Identity = input.read.Identity
	probe.Dispatched = attempt.dispatched.Load()
	if err != nil {
		probe.Reachability = "unknown"
		if probe.Dispatched && failedReachability(err) {
			probe.Reachability = "unreachable"
		}
	}
	if !probe.Dispatched {
		probe.Reachability = "unknown"
	}
	return probe
}
