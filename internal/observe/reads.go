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

func (a *ReadAttempt) Dispatch() { a.dispatched.Store(true) }

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
	if errors.Is(err, context.Canceled) || errors.Is(err, fault.ErrIdentityMismatch) || errors.Is(err, fault.ErrInvalid) || errors.Is(err, fault.ErrOperatorAuthorization) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}

// CollectReads rotates across all groups, with only a bounded number of waiting
// dispatchers. The pool continues owning timed-out callbacks until actual return.
// Returned outcomes and callbacks share no mutable result buffers.
func CollectReads(ctx context.Context, pool *ReadPool, reads []ReplicaRead, start, parallel int, maxAge time.Duration) ([]Probe, int) {
	out := make([]Probe, len(reads))
	for i, r := range reads {
		out[i] = Probe{Identity: r.Identity, Reachability: "unknown", Error: r.Error}
	}
	if len(reads) == 0 || parallel <= 0 {
		return out, 0
	}
	start = ((start % len(reads)) + len(reads)) % len(reads)
	var next atomic.Uint64
	var wg sync.WaitGroup
	for range min(parallel, len(reads)) {
		wg.Go(func() {
			for {
				if ctx.Err() != nil {
					return
				}
				offset := int(next.Add(1) - 1)
				if offset >= len(reads) {
					return
				}
				index := (start + offset) % len(reads)
				read := reads[index]
				if read.Read == nil {
					continue
				}
				attempt := &ReadAttempt{}
				result := make(chan Probe, 1)
				err := pool.DoKey(ctx, read.key(), func(work context.Context) error {
					began := time.Now()
					probe, err := read.Read(work, attempt)
					probe.Identity = read.Identity
					probe.Observation = internalmetrics.AgeObservation(probe.Observation, time.Since(began), maxAge)
					result <- probe
					return err
				})
				probe := out[index]
				if err == nil {
					probe = <-result
				} else {
					probe.Error = err
				}
				probe.Identity = read.Identity
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
				out[index] = probe
			}
		})
	}
	wg.Wait()
	// Rotate even when no work could acquire a permit. This prevents a permanently
	// slow first group from always consuming the entire next round budget.
	advance := min(int(next.Load()), len(reads))
	if advance == 0 || advance == len(reads) {
		advance = 1
	}
	return out, (start + advance) % len(reads)
}
