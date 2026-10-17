package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

const (
	requestTimeout    = 2 * time.Second
	retryWindow       = 500 * time.Millisecond
	requestRetries    = 2
	encodedValueParts = 3
	versionBase       = 10
	versionBits       = 64
	maxFailedVersions = 256
)

// stats are shared by all workers and read by the reporter.
type stats struct {
	writes, writeErrors        atomic.Int64
	hits, misses, uncertain    atomic.Int64
	stale, corrupt, readErrors atomic.Int64
	writeNanos, readNanos      atomic.Int64
}

// keyState tracks one key owned by exactly one worker. A read must never return
// a version older than the last acknowledged write. Versions whose write failed
// may still commit later, so they are "uncertain", never "stale".
type keyState struct {
	name     string
	acked    uint64
	written  uint64
	failures map[uint64]bool
}

// outcome classifies a GET reply for a key.
type outcome int

const (
	hit outcome = iota
	miss
	uncertainHit
	staleRead
	corruptRead
)

func encodeValue(key string, version uint64, payload string) string {
	return fmt.Sprintf("%s|%d|%s", key, version, payload)
}

func classify(k *keyState, reply resp.Reply) outcome {
	if reply.Nil {
		return miss // evicted by the 512-entry bound
	}
	parts := strings.SplitN(reply.Str, "|", encodedValueParts)
	if len(parts) != encodedValueParts || parts[0] != k.name {
		return corruptRead
	}
	version, err := strconv.ParseUint(parts[1], versionBase, versionBits)
	switch {
	case err != nil || version == 0 || version > k.written:
		return corruptRead
	case version >= k.acked:
		return hit
	case k.failures[version]:
		return uncertainHit
	default:
		return staleRead
	}
}

// worker writes a new version of one of its keys, then reads it back through
// the next healthy node selected by the shared round-robin balancer.
type worker struct {
	id       int
	keys     []*keyState
	payload  string
	stats    *stats
	balancer *balancer
	rnd      *rand.Rand
}

func (w *worker) run(ctx context.Context) {
	for contextError(ctx) == nil {
		k := w.keys[w.rnd.IntN(len(w.keys))]
		k.written++
		version := k.written
		started := time.Now()
		write, err := w.balancer.request(ctx, "SET", k.name, encodeValue(k.name, version, w.payload))
		if err != nil {
			if contextError(ctx) != nil {
				return
			}
			w.stats.writeErrors.Add(1)
			k.failures[version] = true
			if len(k.failures) > maxFailedVersions {
				clear(k.failures) // bound memory; older failures are long settled
			}
			pause(ctx, 50*time.Millisecond) // a node may be down; let failover happen
			continue
		}
		k.acked = version
		w.stats.writes.Add(1)
		w.stats.writeNanos.Add(int64(time.Since(started)))

		started = time.Now()
		reply, err := w.balancer.request(ctx, "GET", k.name)
		if err != nil {
			if contextError(ctx) != nil {
				return
			}
			w.stats.readErrors.Add(1)
			continue
		}
		w.stats.readNanos.Add(int64(time.Since(started)))
		switch classify(k, reply.Reply) {
		case hit:
			w.stats.hits.Add(1)
		case miss:
			w.stats.misses.Add(1)
		case uncertainHit:
			w.stats.uncertain.Add(1)
		case staleRead:
			w.stats.stale.Add(1)
			fmt.Printf("STALE worker=%d key=%s writer=%s reader=%s acked=%d got=%q\n",
				w.id, k.name, write.node, reply.node, k.acked, reply.Str)
		case corruptRead:
			w.stats.corrupt.Add(1)
			fmt.Printf("CORRUPT worker=%d key=%s reader=%s got=%q\n", w.id, k.name, reply.node, reply.Str)
		default: // Other read outcomes do not update these counters.
		}
	}
}

// Socket deadlines can fire just before the context's timer marks it done.
// An elapsed workload deadline is normal shutdown in either order.
func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// retryRequest gives the initial attempt two seconds (including dialing) and
// at most two further attempts sharing a total 500ms budget.
func retryRequest(ctx context.Context, call func(context.Context) (resp.Reply, error)) (resp.Reply, error) {
	if err := contextError(ctx); err != nil {
		return resp.Reply{}, err
	}
	initial, cancel := context.WithTimeout(ctx, requestTimeout)
	reply, err := call(initial)
	cancel()
	if stopped := contextError(ctx); stopped != nil {
		return resp.Reply{}, stopped
	}
	retry := retryableReply(reply, err)
	if !retry {
		return reply, requestError(reply, err)
	}
	retries, cancel := context.WithTimeout(ctx, retryWindow)
	defer cancel()
	for attempt := range requestRetries {
		if contextError(retries) != nil {
			break
		}
		deadline, _ := retries.Deadline()
		budget := time.Until(deadline) / time.Duration(requestRetries-attempt)
		next, done := context.WithTimeout(retries, budget)
		reply, err = call(next)
		done()
		if !retryableReply(reply, err) {
			break
		}
	}
	return reply, requestError(reply, err)
}

func requestError(reply resp.Reply, err error) error {
	if err != nil {
		return err
	}
	return reply.Err()
}

func retryableReply(reply resp.Reply, err error) bool {
	if err != nil {
		return !errors.Is(err, resp.ErrProtocol)
	}
	return reply.Kind == '-' && (strings.HasPrefix(reply.Str, "TRYAGAIN ") || strings.HasPrefix(reply.Str, "LOADING "))
}

func pause(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
