//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

var suitePool *Pool

func TestMain(m *testing.M) {
	image := os.Getenv("GQUORUM_TEST_IMAGE")
	if image == "" {
		image = "gquorum-test:local"
	}
	size, cook := 2, 1
	if text := os.Getenv("GQUORUM_TEST_POOL_SIZE"); text != "" {
		value, err := strconv.Atoi(text)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		size = value
	}
	if text := os.Getenv("GQUORUM_TEST_COOK_PARALLEL"); text != "" {
		value, err := strconv.Atoi(text)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		cook = value
	}
	var err error
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
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()
	lease, err := suitePool.Acquire(ctx, spec)
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
