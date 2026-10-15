//go:build !functional

package scheduling

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestScheduleInputOwnsProgressWithoutStagedIntent(t *testing.T) {
	t.Parallel()
	group := &topology.Group{ID: "g", Replicas: []topology.Replica{{NodeName: "n", ReplicaID: 4}}}
	at := time.Unix(10, 0)
	in := ScheduleInput{Groups: map[string]topology.GroupProgress{"g": {
		GroupID: "g", Desired: group, Approved: group, Converged: group,
		ConvergedVersion: 7, OperationID: "exec-v8-a1",
		Blocked:     &topology.BlockedReason{Code: topology.BlockedCatchUp},
		Observation: &topology.GroupStatus{SourceVersion: 7, ReceivedAt: at, LiveVoters: map[uint64]bool{4: true}},
	}}}
	owned := in.Clone()
	progress := owned.Groups["g"]
	if progress.Desired != nil || in.Groups["g"].Desired != group {
		t.Fatal("policy received staged intent or clone altered the input")
	}
	if progress.ConvergedVersion != 7 || progress.OperationID != "exec-v8-a1" || progress.Observation.SourceVersion != 7 || !progress.Observation.ReceivedAt.Equal(at) {
		t.Fatalf("scope provenance changed: %+v", progress)
	}
	progress.Approved.Replicas[0].ReplicaID = 99
	progress.Converged.Replicas[0].NodeName = "changed"
	progress.Blocked.Code = "changed"
	progress.Observation.LiveVoters[4] = false
	delete(owned.Groups, "g")
	original := in.Groups["g"]
	if original.Approved.Replicas[0].ReplicaID != 4 || original.Converged.Replicas[0].NodeName != "n" || original.Blocked.Code != topology.BlockedCatchUp || !original.Observation.LiveVoters[4] {
		t.Fatal("policy aliases retained progress")
	}
	if (ScheduleInput{}).Clone().Groups != nil {
		t.Fatal("nil progress changed to an empty map")
	}
}
