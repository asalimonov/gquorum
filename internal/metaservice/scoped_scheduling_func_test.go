//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	internalscheduling "github.com/asalimonov/gquorum/internal/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedSchedulerAdapterKeepsSchema3Closed(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := singleMetaFixtureWith(t, func(*Options) {})[0]
	if err := g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	h := scopedSchedulingHost{schedulingHost{g}}
	for _, group := range []string{"", topology.MetaGroupID} {
		if _, err := h.ReadRepair(ctx, group); !errors.Is(err, fault.ErrUnsupported) {
			t.Fatal(group, err)
		}
	}
	capture := metaengine.PlanRead{Scopes: []metaengine.ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 1}}}
	if _, err := h.PlanRepair(ctx, capture, g.machine.Target()); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := h.ReadRepair(canceled, ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f := newMutationFixture(t)
	notLeader := scopedSchedulingHost{schedulingHost{f.g}}
	if _, err := notLeader.CommitRepair(ctx, internalscheduling.RepairCommit{}); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	if _, err := notLeader.CommitVoterChange(ctx, topology.VoterChange{}, internalscheduling.RepairCommit{}); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
}
