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

func TestSchedulerAdapterReadsScopedStateAndPreservesProposalRefusals(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := singleMetaFixtureWith(t, func(*Options) {})[0]
	if err := g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	h := schedulingHost{g}
	for _, group := range []string{"", topology.MetaGroupID} {
		read, err := h.ReadRepair(ctx, group)
		if err != nil || read.Target.Version != 1 || len(read.Scopes) != 1 ||
			read.Scopes[0].GroupID != topology.MetaGroupID || read.Scopes[0].SourceVersion != 1 {
			t.Fatal(group, read, err)
		}
		if group == "" {
			continue
		}
		row := read.Input.Groups[group]
		if row.Observation == nil || row.Observation.SourceVersion != row.ConvergedVersion ||
			read.Input.Native[group].Applied == 0 {
			t.Fatal("repair did not bind native observation to captured progress", read)
		}
		desired := read.Target.Clone()
		desired.Version, desired.ParentVersion = read.Latest.Version+1, read.Target.Version
		desired.MetaGroup().Labels = map[string]string{"test": "declaration"}
		plan, err := h.PlanRepair(ctx, read, desired)
		if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != group || len(plan.Steps) != 0 {
			t.Fatal("declaration preview did not use scoped planning", plan, err)
		}
		// A real proposal must reach the FSM's CAS check. Missing correlation
		// metadata used to turn scheduler commits into Invalid at decoding.
		commit := internalscheduling.RepairCommit{Document: desired, Plan: plan,
			ExpectedVersion: read.Latest.Version + 1, ExpectedTarget: read.Target.Version, Scopes: read.Scopes}
		if _, err := h.CommitRepair(ctx, commit); !errors.Is(err, fault.ErrVersionConflict) {
			t.Fatal("repair lost its CAS refusal", err)
		}
		if _, err := h.CommitVoterChange(ctx, topology.VoterChange{GroupID: group}, commit); !errors.Is(err, fault.ErrVersionConflict) {
			t.Fatal("voter change lost its CAS refusal", err)
		}
	}
	if _, err := h.PlanRepair(ctx, metaengine.PlanRead{}, g.machine.Target()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := h.ReadRepair(canceled, ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f := newMutationFixture(t)
	notLeader := schedulingHost{f.g}
	if _, err := notLeader.CommitRepair(ctx, internalscheduling.RepairCommit{}); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	if _, err := notLeader.CommitVoterChange(ctx, topology.VoterChange{}, internalscheduling.RepairCommit{}); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
}
