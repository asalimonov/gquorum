// The load generator stresses the replicated cache from several workers. Each
// request is balanced across healthy replicas, with reusable connections and
// independent HTTP health probes during node failures.
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
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/asalimonov/gquorum/examples/inmemory-cache/internal/profiling"
)

const (
	payloadSeedBytes = 16
	minReportSeconds = 1e-9
	maxWorkers       = 256
	defaultKeys      = 615
)

type config struct {
	PprofListen string   `env:"CACHE_PPROF_LISTEN" help:"Optional pprof listen address; disabled by default."`
	Nodes       []string `env:"CACHE_NODES" required:"true" sep:"," help:"${nodes}"`
	HealthPort  int      `env:"CACHE_HEALTH_PORT" default:"8080" help:"HTTP health port on each Redis host."`
	Workers     int      `env:"CACHE_WORKERS" default:"8" help:"Parallel workers, from 1 to 256."`
	//revive:disable-next-line:struct-tag // Kong expands this variable to the numeric key-count default.
	Keys     int           `env:"CACHE_KEYS" default:"${key_count}" help:"${keys}"`
	Duration time.Duration `env:"CACHE_DURATION" default:"0s" help:"Stop after this long; 0 runs until interrupted."`
	Report   time.Duration `env:"CACHE_REPORT" default:"5s"`
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
	stopProfile, err := profiling.Listen(cfg.PprofListen)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	s := run(ctx, cfg)
	stopProfile()
	if s.stale.Load() > 0 || s.corrupt.Load() > 0 || s.hits.Load()+s.misses.Load() == 0 {
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
		w.keys = append(w.keys, &keyState{name: fmt.Sprintf("key:%s:%04d", payload, i), failures: map[uint64]bool{}})
	}
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Go(func() { w.run(ctx) })
	}
	r := &reporter{stats: s, pool: pool, balancer: balancer, started: time.Now()}
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
		r.report(mode)
	}
	return s
}

// sample is a point-in-time copy of the counters the report differentiates.
type sample struct {
	hits, misses, uncertain, stale, corrupt int64
	at                                      time.Time
	writes, writeNanos, reads, readNs       int64
}

// reporter prints interval figures (what changed since the previous line), so
// a slowdown is visible immediately instead of being averaged away.
type reporter struct {
	stats    *stats
	pool     *requestPool
	balancer *balancer
	started  time.Time
	last     sample
}

type reportMode uint8

const (
	progressReport reportMode = iota
	finalReport
)

func (r *reporter) report(mode reportMode) {
	s := r.stats
	now := sample{
		at:         time.Now(),
		writes:     s.writes.Load(),
		writeNanos: s.writeNanos.Load(),
		readNs:     s.readNanos.Load(),
		hits:       s.hits.Load(), misses: s.misses.Load(), uncertain: s.uncertain.Load(),
		stale: s.stale.Load(), corrupt: s.corrupt.Load(),
	}
	now.reads = now.hits + now.misses + now.uncertain + now.stale + now.corrupt
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
	seconds := max(now.at.Sub(from.at).Seconds(), minReportSeconds)
	fmt.Printf(
		"%s elapsed=%s write_rate=%.0f/s rate=%.0f/s write_avg=%s read_avg=%s total_writes=%d total_reads=%d "+
			"hits=%d misses=%d uncertain=%d stale=%d corrupt=%d write_errors=%d read_errors=%d "+
			"connections=%d dials=%d healthy=%s\n",
		label, now.at.Sub(r.started).Round(time.Second), float64(now.writes-from.writes)/seconds,
		float64(now.writes-from.writes+now.reads-from.reads)/seconds,
		avg(now.writeNanos-from.writeNanos, now.writes-from.writes), avg(now.readNs-from.readNs, now.reads-from.reads),
		now.writes,
		now.reads, now.hits, now.misses, now.uncertain, now.stale, now.corrupt,
		s.writeErrors.Load(),
		s.readErrors.Load(), r.pool.connections.Load(), r.pool.dials.Load(), r.balancer.healthyNodes())
	r.last = now
}

func cliHelp() kong.Vars {
	return kong.Vars{
		"nodes":     "Comma-separated host:port Redis endpoints; healthy nodes are selected round-robin.",
		"keys":      "Size of the generated key set.",
		"key_count": strconv.Itoa(defaultKeys),
	}
}
