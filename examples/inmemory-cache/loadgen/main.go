// The load generator stresses the replicated cache from several workers. Each
// request is balanced across healthy replicas, with reusable connections and
// independent HTTP health probes under eviction and node failures.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
)

const (
	payloadSeedBytes = 16
	minReportSeconds = 1e-9
	maxWorkers       = 256
)

type config struct {
	Nodes      []string      `env:"CACHE_NODES" required:"true" sep:"," help:"${nodes}"`
	HealthPort int           `env:"CACHE_HEALTH_PORT" default:"8080" help:"HTTP health port on each Redis host."`
	Workers    int           `env:"CACHE_WORKERS" default:"8" help:"Parallel workers, from 1 to 256."`
	Keys       int           `env:"CACHE_KEYS" default:"1024" help:"${keys}"`
	Duration   time.Duration `env:"CACHE_DURATION" default:"0s" help:"Stop after this long; 0 runs until interrupted."`
	Report     time.Duration `env:"CACHE_REPORT" default:"5s"`
}

func (c config) Validate() error {
	if c.Workers < 1 || c.Workers > maxWorkers || c.Keys < c.Workers || len(c.Nodes) == 0 ||
		c.Report <= 0 || c.Duration < 0 {
		return errors.New("need nodes, 1..256 workers, keys >= workers, report > 0 and duration >= 0")
	}
	return c.validateNodes()
}

func main() {
	var cfg config
	parser, err := kong.New(&cfg, cliHelp(), kong.Name("cache-loadgen"),
		kong.Description("Stress the replicated cache from several workers."))
	if err == nil {
		_, err = parser.Parse(os.Args[1:])
	}
	if err == nil {
		err = cfg.Validate()
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}
	s := run(ctx, cfg)
	if s.stale.Load() > 0 || s.corrupt.Load() > 0 || s.hits.Load() == 0 {
		os.Exit(1)
	}
}

// run partitions the key set across workers so each key has one writer, then
// reports progress until ctx ends.
func run(ctx context.Context, cfg config) *stats {
	s := &stats{}
	pool := newRequestPool(cfg.Nodes, cfg.Workers)
	defer pool.close()
	balancer := newBalancer(pool, cfg.Nodes, cfg.HealthPort)
	balancer.start(ctx)
	defer balancer.stop()
	seed := make([]byte, payloadSeedBytes)
	_, _ = rand.Read(seed)
	payload := hex.EncodeToString(seed)
	workers := make([]*worker, cfg.Workers)
	for i := range workers {
		workers[i] = &worker{
			id:       i,
			payload:  payload,
			stats:    s,
			balancer: balancer,
			//nolint:gosec // Synthetic load generation does not use secrets.
			rnd: mathrand.New(mathrand.NewPCG(uint64(time.Now().UnixNano()), uint64(i))),
		}
	}
	for i := range cfg.Keys {
		w := workers[i%cfg.Workers]
		w.keys = append(w.keys, &keyState{name: fmt.Sprintf("key:%04d", i), failures: map[uint64]bool{}})
	}
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Go(func() { w.run(ctx) })
	}
	r := &reporter{stats: s, pool: pool, balancer: balancer, nodes: cfg.Nodes, started: time.Now()}
	r.last.at = r.started
	tick := time.NewTicker(cfg.Report)
	defer tick.Stop()
	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick.C:
		}
		done = done || contextError(ctx) != nil
		if done {
			wg.Wait()
		}
		mode := progressReport
		if done {
			mode = finalReport
		}
		r.report(ctx, mode)
	}
	return s
}

// sample is a point-in-time copy of the counters the report differentiates.
type sample struct {
	at                                time.Time
	writes, writeNanos, reads, readNs int64
}

// reporter prints interval figures (what changed since the previous line), so
// a slowdown is visible immediately instead of being averaged away.
type reporter struct {
	stats    *stats
	pool     *requestPool
	balancer *balancer
	nodes    []string
	started  time.Time
	last     sample
}

type reportMode uint8

const (
	progressReport reportMode = iota
	finalReport
)

func (r *reporter) report(ctx context.Context, mode reportMode) {
	s := r.stats
	now := sample{
		at:         time.Now(),
		writes:     s.writes.Load(),
		writeNanos: s.writeNanos.Load(),
		readNs:     s.readNanos.Load(),
		reads:      s.hits.Load() + s.misses.Load() + s.uncertain.Load() + s.stale.Load() + s.corrupt.Load(),
	}
	avg := func(total, n int64) time.Duration {
		if n == 0 {
			return 0
		}
		return time.Duration(total / n).Round(10 * time.Microsecond)
	}
	label, from := "progress", r.last
	if mode == finalReport {
		label, from = "final", sample{at: r.started} // the final line covers the whole run
	}
	sizeContext := ctx
	if mode == finalReport {
		sizeContext = context.Background()
	}
	seconds := max(now.at.Sub(from.at).Seconds(), minReportSeconds)
	sizes := dbsizes(sizeContext, r.balancer, r.nodes)
	fmt.Printf(
		"%s elapsed=%s rate=%.0f/s write_avg=%s read_avg=%s total_writes=%d total_reads=%d "+
			"hits=%d evicted_misses=%d uncertain=%d stale=%d corrupt=%d write_errors=%d read_errors=%d "+
			"connections=%d dials=%d healthy=%s dbsize=%s\n",
		label, now.at.Sub(r.started).Round(time.Second), float64(now.writes-from.writes)/seconds,
		avg(now.writeNanos-from.writeNanos, now.writes-from.writes), avg(now.readNs-from.readNs, now.reads-from.reads),
		now.writes,
		now.reads, s.hits.Load(), s.misses.Load(), s.uncertain.Load(), s.stale.Load(), s.corrupt.Load(),
		s.writeErrors.Load(),
		s.readErrors.Load(), r.pool.connections.Load(), r.pool.dials.Load(), r.balancer.healthyNodes(), sizes)
	r.last = now
}

// dbsizes reports a key count through each configured endpoint; the bound is 512.
func dbsizes(ctx context.Context, balancer *balancer, nodes []string) string {
	out := ""
	for _, node := range nodes {
		value := "down"
		if balancer.isHealthy(node) {
			if reply, err := balancer.pool.request(ctx, node, "DBSIZE"); err == nil {
				value = fmt.Sprint(reply.Int)
			}
		}
		out += fmt.Sprintf("%s=%s ", node, value)
	}
	return "[" + out[:max(len(out)-1, 0)] + "]"
}

func cliHelp() kong.Vars {
	return kong.Vars{
		"nodes": "Comma-separated host:port Redis endpoints; healthy nodes are selected round-robin.",
		"keys":  "Size of the key set; twice the cache capacity forces eviction.",
	}
}
