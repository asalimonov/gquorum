//go:build !functional

package scheduling_test

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/scheduling/schedulingtest"
	"github.com/asalimonov/gquorum/topology"
)

func TestExternalSchedulerContract(t *testing.T) {
	t.Parallel()
	factory := func() scheduling.RepairPolicy {
		p, err := scheduling.NewDefaultRepairPolicy(scheduling.DefaultRepairOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	schedulingtest.CheckRepairPolicy(t, factory, []schedulingtest.RepairPolicyCase{
		{Name: "empty observations", Input: scheduling.ScheduleInput{}, Want: scheduling.ScheduleDecision{Reason: "converged"}},
		{Name: "missing observation is not a repair", Input: scheduling.ScheduleInput{Target: topology.Document{ClusterID: "c", Groups: []topology.Group{{ID: "g"}}}}, Want: scheduling.ScheduleDecision{Reason: "approved_execution"}},
	})
}

func TestRepairPolicyInvalidOptionsAndCancellation(t *testing.T) {
	t.Parallel()
	if _, err := scheduling.NewDefaultRepairPolicy(scheduling.DefaultRepairOptions{StableFor: -1}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("negative stability: %v", err)
	}

	s, _ := scheduling.NewDefaultRepairPolicy(scheduling.DefaultRepairOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Schedule(ctx, scheduling.ScheduleInput{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
