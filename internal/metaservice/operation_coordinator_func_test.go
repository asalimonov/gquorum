//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCoordinatorKeepsSchema3ClosedAndRequiresLeadership(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := singleMetaFixtureWith(t, func(*Options) {})[0]
	if err := g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	c := &operationCoordinator{g: g}
	if _, err := c.ReadyBlocks(ctx, 1); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := c.ReadyBlocks(canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	fixture := newMutationFixture(t)
	notLeader := &operationCoordinator{g: fixture.g}
	if _, err := notLeader.ReadyBlocks(ctx, 1); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	if err := notLeader.RecordOperationTransition(ctx, topology.OperationTransition{}); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
}
