package metaservice

import (
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func testPlanningInput(tb testing.TB, source *metaengine.Machine, desired topology.Document,
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
	read := func(query metaengine.QueryRequest) metaengine.PlanRead {
		tb.Helper()
		data, err := json.Marshal(query)
		if err != nil {
			tb.Fatal(err)
		}
		result, err := source.Query(tb.Context(), data)
		if err != nil {
			tb.Fatal(err)
		}
		var captured metaengine.PlanRead
		if err := json.Unmarshal(result.Data, &captured); err != nil {
			tb.Fatal(err)
		}
		return captured
	}
	request := metaengine.QueryRequest{Kind: "planning", Desired: &desired}
	captured := read(request)
	request.Native = make(map[string]topology.NativeState)
	for _, scope := range captured.Scopes {
		if value, ok := native[scope.GroupID]; ok {
			request.Native[scope.GroupID] = value
		}
	}
	input := read(request).Input
	for id, status := range observed {
		row := input.Groups[id]
		owned := status.Clone()
		row.GroupID, row.Observation = id, &owned
		input.Groups[id] = row
	}
	return input
}

// Complete historical baselines are intentional fixture inputs. Assertions about
// current independent progress use Machine.Progress or Views.FullyConverged.
func fixtureConvergedDocument(t *testing.T, machine *metaengine.Machine) topology.Document {
	t.Helper()
	doc, err := machine.FullyConvergedDocument()
	if err != nil {
		t.Fatal(err)
	}
	return doc
}
