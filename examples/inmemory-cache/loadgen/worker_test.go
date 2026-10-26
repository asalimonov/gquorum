//go:build !functional

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

func TestRetryRequestLimitsAndErrorClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		reply    resp.Reply
		err      error
		attempts int
	}{
		{"success", resp.Reply{Kind: '+', Str: "OK"}, nil, 1},
		{"transport", resp.Reply{}, errors.New("connection lost"), 3},
		{"loading", resp.Reply{Kind: '-', Str: "LOADING replica starting"}, nil, 3},
		{"quorum", resp.Reply{Kind: '-', Str: "TRYAGAIN no quorum"}, nil, 3},
		{"permanent", resp.Reply{Kind: '-', Str: "ERR invalid command"}, nil, 1},
		{"protocol", resp.Reply{}, resp.ErrProtocol, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			attempts := 0
			_, err := retryRequest(t.Context(), func(ctx context.Context) (resp.Reply, error) {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > requestTimeout {
					t.Error("missing or excessive request deadline")
				}
				attempts++
				return tc.reply, tc.err
			})
			if attempts != tc.attempts || (err == nil) != (tc.name == "success") {
				t.Fatalf("attempts=%d error=%v", attempts, err)
			}
		})
	}
}

func TestRetryRequestRecoversAndHonorsCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	attempts := 0
	call := func(context.Context) (resp.Reply, error) {
		attempts++
		if attempts == 1 {
			return resp.Reply{}, errors.New("connection lost")
		}
		return resp.Reply{Kind: '+', Str: "OK"}, nil
	}
	if reply, err := retryRequest(ctx, call); err != nil || reply.Str != "OK" || attempts != 2 {
		t.Fatalf("reply=%+v attempts=%d error=%v", reply, attempts, err)
	}
	cancel()
	if _, err := retryRequest(ctx, call); !errors.Is(err, context.Canceled) || attempts != 2 {
		t.Fatalf("cancellation started another request: attempts=%d error=%v", attempts, err)
	}
}

func TestRetryRequestShares500msBudget(t *testing.T) {
	t.Parallel()
	attempts := 0
	var retryDeadline time.Time
	_, _ = retryRequest(t.Context(), func(ctx context.Context) (resp.Reply, error) {
		attempts++
		if attempts == 1 {
			retryDeadline = time.Now().Add(retryWindow)
			return resp.Reply{}, errors.New("connection lost")
		}
		deadline, _ := ctx.Deadline()
		if deadline.After(retryDeadline.Add(10 * time.Millisecond)) {
			t.Error("retry exceeds shared 500ms budget")
		}
		<-ctx.Done()
		return resp.Reply{}, ctx.Err()
	})
	if attempts != 3 {
		t.Fatalf("got %d attempts, want initial plus two retries", attempts)
	}
}

func TestWorkerParallelismBounds(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{0, 1, 8, 256, 257} {
		cfg := config{Nodes: []string{"node1:6379"}, HealthPort: 8080, Workers: workers, Keys: 1024, Report: time.Second}
		if err := cfg.Validate(); (err == nil) != (workers >= 1 && workers <= 256) {
			t.Errorf("workers=%d error=%v", workers, err)
		}
	}
}

// A network deadline may expire while the workload timer is still pending.
type pendingDeadlineContext struct{ context.Context }

func (pendingDeadlineContext) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }

func TestWorkerStopsAtDeadlineBeforeContextTimerFires(t *testing.T) {
	t.Parallel()
	if contextError(pendingDeadlineContext{Context: t.Context()}) != context.DeadlineExceeded {
		t.Fatal("elapsed workload deadline would be counted as a request failure")
	}
}

func TestClassifyReadAgainstAcknowledgedWrites(t *testing.T) {
	t.Parallel()
	k := &keyState{name: "key:0001", acked: 5, written: 7, failures: map[uint64]bool{3: true}}
	bulk := func(v uint64) resp.Reply { return resp.Reply{Kind: '$', Str: encodeValue("key:0001", v, "p")} }
	cases := []struct {
		reply resp.Reply
		want  outcome
	}{
		{resp.Reply{Kind: '$', Nil: true}, miss},
		{bulk(5), hit},
		{bulk(7), hit}, // a later write's value can arrive first
		{bulk(3), uncertainHit},
		{bulk(4), staleRead},
		{bulk(8), corruptRead},
		{resp.Reply{Kind: '$', Str: encodeValue("key:0002", 5, "p")}, corruptRead},
		{resp.Reply{Kind: '$', Str: "garbage"}, corruptRead},
	}
	for _, c := range cases {
		if got := classify(k, c.reply); got != c.want {
			t.Errorf("%q: got %d, want %d", c.reply.Str, got, c.want)
		}
	}
}
