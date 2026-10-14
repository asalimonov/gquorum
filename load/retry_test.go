//go:build !functional

package load_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestProposeRetryResubmitsExactProposal(t *testing.T) {
	t.Parallel()
	want := load.Proposal{RequestID: "create/1", Data: []byte("payload")}
	var seen []load.Proposal
	propose := func(_ context.Context, p load.Proposal) (load.Result, error) {
		seen = append(seen, p)
		if len(seen) < 3 {
			return load.Result{}, fault.ErrNotLeader
		}
		return load.Result{Code: "ok"}, nil
	}
	options := fault.RetryOptions{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	out, err := load.ProposeRetry(context.Background(), propose, want, options)
	if err != nil || out.Code != "ok" || len(seen) != 3 {
		t.Fatalf("out=%+v err=%v attempts=%d", out, err, len(seen))
	}
	for _, p := range seen {
		if p.RequestID != want.RequestID || !bytes.Equal(p.Data, want.Data) {
			t.Fatalf("proposal changed between attempts: %+v", p)
		}
	}
	if _, err := load.ProposeRetry(context.Background(), nil, want, options); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("nil propose: %v", err)
	}
}
