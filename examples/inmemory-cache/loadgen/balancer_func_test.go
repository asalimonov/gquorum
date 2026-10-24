//go:build functional

package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

// These are protocol doubles, with no Raft nodes or application replicas.
func startBalancerProtocol(t *testing.T) (string, *atomic.Bool, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	loading, calls := &atomic.Bool{}, &atomic.Int64{}
	var handlers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			handlers.Go(func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					if _, err := resp.ReadCommand(reader); err != nil {
						return
					}
					calls.Add(1)
					reply := "+PONG\r\n"
					if loading.Load() {
						reply = "-LOADING unavailable\r\n"
					}
					if _, err := io.WriteString(conn, reply); err != nil {
						return
					}
				}
			})
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done; handlers.Wait() })
	return listener.Addr().String(), loading, calls
}

func TestBalancerHealthPollingFailoverAndRecovery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	first, loading, firstCalls := startBalancerProtocol(t)
	second, _, secondCalls := startBalancerProtocol(t)
	var up atomic.Bool
	up.Store(true)
	var probeCount atomic.Int64
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Error("incorrect health path", r.URL.Path)
		}
		probeCount.Add(1)
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer health.Close()
	addresses := []string{first, second}
	pool := newRequestPool(addresses, 2)
	defer pool.close()
	b := newBalancer(pool, addresses, 8080)
	for i := range b.nodes {
		b.nodes[i].healthURL = health.URL + "/health"
	}
	b.start(ctx)
	defer b.stop()
	waitFor := func(condition func() bool) {
		t.Helper()
		for !condition() {
			select {
			case <-ctx.Done():
				t.Fatal("health state did not converge")
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	waitFor(func() bool { return b.healthyNodes() == strings.Join(addresses, ",") })
	for range 6 {
		if _, err := b.request(ctx, "PING"); err != nil {
			t.Fatal(err)
		}
	}
	if firstCalls.Load() != 3 || secondCalls.Load() != 3 || pool.dials.Load() != 2 {
		t.Fatalf("unbalanced or unreused traffic: first=%d second=%d dials=%d",
			firstCalls.Load(), secondCalls.Load(), pool.dials.Load())
	}
	// Retryable replies must select the other node immediately, before a probe.
	loading.Store(true)
	if result, err := b.request(ctx, "PING"); err != nil || result.node != second {
		t.Fatalf("retry did not fail over: selected=%s err=%v", result.node, err)
	}
	loading.Store(false)
	// Probe both nodes as down, then verify the periodic loop restores them.
	up.Store(false)
	waitFor(func() bool { return b.healthyNodes() == "none" })
	up.Store(true)
	waitFor(func() bool { return b.healthyNodes() == strings.Join(addresses, ",") })
	if probeCount.Load() < 6 {
		t.Fatal("periodic health polling did not run")
	}
	if _, err := b.request(ctx, "PING"); err != nil {
		t.Fatal(err)
	}
}

func TestSlowHealthProbeDoesNotBlockOtherNodesAndStopJoins(t *testing.T) {
	t.Parallel()
	var fastCount atomic.Int64
	started := make(chan struct{}, 1)
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { fastCount.Add(1) }))
	defer fast.Close()
	b := newBalancer(nil, []string{"slow:6379", "fast:6379"}, 8080)
	b.nodes[0].healthURL, b.nodes[1].healthURL = slow.URL+"/health", fast.URL+"/health"
	b.start(t.Context())
	defer b.stop()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow probe did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for fastCount.Load() < 2 {
		select {
		case <-ctx.Done():
			t.Fatal("slow node blocked periodic checks on the healthy node")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
