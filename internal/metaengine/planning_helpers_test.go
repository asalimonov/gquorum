package metaengine

import (
	"context"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

type planningFixtureSource interface {
	PlanningInput(context.Context, topology.Document, map[string]topology.NativeState) (topology.PlanInput, error)
}

func testPlanningInput(tb testing.TB, source planningFixtureSource, desired topology.Document,
	observed map[string]topology.GroupStatus) topology.PlanInput {
	tb.Helper()
	native := make(map[string]topology.NativeState)
	for id, status := range observed {
		if status.Found && desired.Group(id) != nil {
			reporter := uint64(1)
			if len(status.ConfState.Voters) != 0 {
				reporter = status.ConfState.Voters[0]
			}
			native[id] = topology.NativeState{GroupID: id, ReplicaID: reporter, Applied: 1,
				ConfState: status.Clone().ConfState}
		}
	}
	input, err := source.PlanningInput(tb.Context(), desired, native)
	if err != nil {
		tb.Fatal(err)
	}
	for id, status := range observed {
		row := input.Groups[id]
		owned := status.Clone()
		row.GroupID, row.Observation = id, &owned
		input.Groups[id] = row
	}
	return input
}
