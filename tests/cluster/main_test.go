//go:build clustertest

package cluster

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

var suitePool *Pool

func TestMain(m *testing.M) {
	flag.Parse()
	parallelSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "test.parallel" {
			parallelSet = true
		}
	})
	if !parallelSet {
		if err := flag.Set("test.parallel", "8"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	// Match pool capacity to Go's test slots, including focused raw go test runs.
	parallel, err := strconv.Atoi(flag.Lookup("test.parallel").Value.String())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	image := os.Getenv("GQUORUM_TEST_IMAGE")
	if image == "" {
		image = "gquorum-test:local"
	}
	size := parallel
	if text := os.Getenv("GQUORUM_TEST_POOL_SIZE"); text != "" {
		value, err := strconv.Atoi(text)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		size = value
	}
	cook := size
	if text := os.Getenv("GQUORUM_TEST_COOK_PARALLEL"); text != "" {
		value, err := strconv.Atoi(text)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		cook = value
	}
	fmt.Fprintf(os.Stderr, "Cluster pool: parallel=%d units=%d concurrent-starts=%d\n", parallel, size, cook)
	suitePool, err = NewPool(PoolOptions{Image: image, Size: size, CookParallel: cook})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	start, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = suitePool.Start(start)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start pool:", err)
		cleanup, stop := context.WithTimeout(context.Background(), 45*time.Second)
		_ = suitePool.Close(cleanup)
		stop()
		os.Exit(1)
	}
	result := m.Run()
	cleanup, stop := context.WithTimeout(context.Background(), 45*time.Second)
	err = suitePool.Close(cleanup)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool cleanup:", err)
		result = 1
	}
	os.Exit(result)
}
func acquire(t *testing.T, spec Spec) *Lease {
	t.Helper()
	return acquireFrom(t, suitePool, spec)
}

func acquireFrom(t *testing.T, pool *Pool, spec Spec) *Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()
	lease, err := pool.Acquire(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if t.Failed() {
			for _, node := range lease.Cluster.Nodes() {
				logs, err := node.Logs(cleanup)
				t.Logf("node %s logs(%v): %s", node.Spec.Name, err, logs)
			}
		}
		if err := lease.Release(cleanup); err != nil {
			t.Error("release:", err)
		}
	})
	return lease
}
