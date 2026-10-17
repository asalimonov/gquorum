//go:build !functional

package operator

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRemainingPlanDistinguishesRetainedHostObligations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		progress topology.RetainedProgress
		host     string
		deleted  bool
		steps    []StepKind
		wantErr  bool
	}{
		{"hosted missing", topology.RetainedHosted, "b", false, nil, true},
		{"hosted covered", topology.RetainedHosted, "b", false, []StepKind{StepRemoveReplica, StepStopReplica, StepCleanupReplicaState}, false},
		{"detached debt", topology.RetainedCleanup, "b", false, nil, false},
		{"required debt stop missing", topology.RetainedCleanup, "a", false, nil, true},
		{"required debt stop", topology.RetainedCleanup, "a", false, []StepKind{StepStopReplica}, false},
		{"already stopped", topology.RetainedStopped, "a", false, nil, false},
		{"deletion debt stop missing", topology.RetainedCleanup, "b", true, nil, true},
		{"deletion debt stop", topology.RetainedCleanup, "b", true, []StepKind{StepStopReplica}, false},
		{"hosted deletion missing", topology.RetainedHosted, "b", true, []StepKind{StepStopReplica}, true},
		{"stopped deletion", topology.RetainedStopped, "b", true, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := Document{Groups: []Group{{ID: "work", Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}}}}}
			if tc.deleted {
				target.Groups = nil
			}
			input := topology.PlanInput{Retained: map[string]topology.RetainedGroup{"work": {Replicas: []topology.RetainedReplica{
				{ReplicaKey: topology.ReplicaKey{NodeName: tc.host, ReplicaID: 2}, Progress: tc.progress},
			}}}}
			ex := Record{}
			for i, kind := range tc.steps {
				ex.Steps = append(ex.Steps, Step{Seq: i + 1, Kind: kind, GroupID: "work", NodeName: tc.host, ReplicaID: 2})
			}
			native := map[string]topology.NativeState{"work": {GroupID: "work", ReplicaID: 1, Applied: 10,
				ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
			err := ex.validateScope(RemainingPlanView{Input: input, Target: target}, native, "work",
				Rules{Limits: Limits{MaxNodes: 3, MaxReplicas: 5}})
			if tc.wantErr && !errors.Is(err, fault.ErrReplanRequired) || !tc.wantErr && err != nil {
				t.Fatal("wrong retained obligation", err)
			}
		})
	}
}
