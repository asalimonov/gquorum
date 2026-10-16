//go:build functional

package metaservice

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestOperationWaitPreservesExactAggregateAndCancellation(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"complete", "cancel", "stopped", "unavailable"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			g := &Group{stopped: make(chan struct{})}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := topology.OperationStatus{ID: "exec-v3-a1", State: topology.OperationRunning, TotalBlocks: 2,
				Blocks: []topology.BlockStatus{{GroupID: "a", State: topology.OperationFailed}, {GroupID: "b", State: topology.OperationRunning}}}
			calls := 0
			read := func(_ context.Context, id topology.OperationID) (topology.OperationStatus, error) {
				if id != want.ID {
					t.Fatal("wait redirected attempt", id)
				}
				calls++
				if outcome == "complete" && calls == 1 {
					return topology.OperationStatus{}, fault.ErrHistoryUnavailable
				}
				switch outcome {
				case "complete":
					want.State = topology.OperationFailed
					want.Blocks[1].State = topology.OperationSucceeded
				case "cancel":
					cancel()
				case "stopped":
					close(g.stopped)
				case "unavailable":
					return topology.OperationStatus{}, fault.ErrUnavailable
				}
				return want.Clone(), nil
			}
			got, err := g.waitOperationResult(ctx, want.ID, read)
			switch outcome {
			case "complete":
				if err != nil || calls != 2 || !reflect.DeepEqual(got, want) {
					t.Fatal(got, err, calls)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) || got.State.Terminal() {
					t.Fatal(got, err)
				}
			case "stopped":
				if !errors.Is(err, fault.ErrStopped) {
					t.Fatal(err)
				}
			case "unavailable":
				if !errors.Is(err, fault.ErrUnavailable) {
					t.Fatal(err)
				}
			}
		})
	}
}
