package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

const (
	requestTimeout    = 5 * time.Second
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

// worker writes a new version of one of its keys through a random node, then
// reads that key back from a different node.
type worker struct {
	id      int
	nodes   []string
	keys    []*keyState
	payload string
	stats   *stats
	conns   map[string]*resp.Client
	rnd     *rand.Rand
}

func (w *worker) run(ctx context.Context) {
	defer w.closeAll()
	for ctx.Err() == nil {
		k := w.keys[w.rnd.IntN(len(w.keys))]
		writer := w.nodes[w.rnd.IntN(len(w.nodes))]
		k.written++
		version := k.written
		started := time.Now()
		reply, err := w.do(ctx, writer, "SET", k.name, encodeValue(k.name, version, w.payload))
		if err == nil {
			err = reply.Err()
		}
		if err != nil {
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

		reader := w.other(writer)
		started = time.Now()
		reply, err = w.do(ctx, reader, "GET", k.name)
		if err == nil {
			err = reply.Err()
		}
		if err != nil {
			w.stats.readErrors.Add(1)
			continue
		}
		w.stats.readNanos.Add(int64(time.Since(started)))
		switch classify(k, reply) {
		case hit:
			w.stats.hits.Add(1)
		case miss:
			w.stats.misses.Add(1)
		case uncertainHit:
			w.stats.uncertain.Add(1)
		case staleRead:
			w.stats.stale.Add(1)
			fmt.Printf("STALE worker=%d key=%s writer=%s reader=%s acked=%d got=%q\n", w.id, k.name, writer, reader,
				k.acked, reply.Str)
		case corruptRead:
			w.stats.corrupt.Add(1)
			fmt.Printf("CORRUPT worker=%d key=%s reader=%s got=%q\n", w.id, k.name, reader, reply.Str)
		default: // Other read outcomes do not update these counters.
		}
	}
}

// other picks a random node different from the writer when there is one.
func (w *worker) other(writer string) string {
	if len(w.nodes) == 1 {
		return writer
	}
	for {
		if node := w.nodes[w.rnd.IntN(len(w.nodes))]; node != writer {
			return node
		}
	}
}

// do sends one command over this worker's connection to node, reconnecting
// once if the cached connection broke.
func (w *worker) do(ctx context.Context, node string, args ...string) (resp.Reply, error) {
	client := w.conns[node]
	if client == nil {
		dial, cancel := context.WithTimeout(ctx, requestTimeout)
		var err error
		client, err = resp.Dial(dial, node)
		cancel()
		if err != nil {
			return resp.Reply{}, err
		}
		w.conns[node] = client
	}
	reply, err := client.Do(requestTimeout, args...)
	if err != nil {
		_ = client.Close()
		delete(w.conns, node)
	}
	return reply, err
}

func (w *worker) closeAll() {
	for _, c := range w.conns {
		_ = c.Close()
	}
}

func pause(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
