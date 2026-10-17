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

func TestScopedCoordinatorPagesCurrentWorkAndRequiresLeadership(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) { o.LoadGroups = &statusController{} }))
	g.controller.Stop()
	c := &operationCoordinator{g: g}
	if _, err := c.ReadyBlocks(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadyCleanup(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadyFences(ctx, 1); err != nil {
		t.Fatal(err)
	}
	doc := loadDocument(t, g)
	other := doc.Groups[len(doc.Groups)-1]
	other.ID = "load-two"
	doc.Groups = append(doc.Groups, other)
	staged, err := g.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Approve(ctx, staged.Version); err != nil {
		t.Fatal(err)
	}
	var selected []string
	for range 3 {
		work, err := c.ReadyBlocks(ctx, 1)
		if err != nil || len(work) != 1 {
			t.Fatal(work, err)
		}
		selected = append(selected, work[0].GroupID)
	}
	if selected[0] != "load-one" || selected[1] != "load-two" || selected[2] != selected[0] {
		t.Fatal("cursor failed to advance or wrap", selected)
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
