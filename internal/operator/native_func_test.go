//go:build functional

package operator

import (
	"context"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type nativeController struct {
	GroupEffects
	state topology.NativeState
	err   error
}

func (g nativeController) ReadNative(context.Context, string, ...storage.Identity) (topology.NativeState, error) {
	return g.state, g.err
}

func TestFinalConvergenceRequiresConfirmedNativeMembership(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"unavailable", "different_membership", "foreign_group"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			active, target, _ := growDocuments()
			step := Step{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: "n2", ReplicaID: 2}
			f := newControllerFixture(active, target, []Step{step}, 1, OperatorRunning)
			f.groups.statuses[step.GroupID] = statusFor(step.GroupID, []uint64{1, 2}, nil, 1)
			fake := nativeController{GroupEffects: f.groups, state: topology.NativeState{GroupID: step.GroupID, ReplicaID: 1, Applied: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}
			switch mode {
			case "unavailable":
				fake.err = fault.ErrUnavailable
			case "different_membership":
				fake.state.ConfState.Voters = []uint64{1}
			case "foreign_group":
				fake.state.GroupID = "other"
			}
			f.engine.groups = fake
			f.engine.finish(t.Context(), f.coord.operator, target, &step)
			ex, _ := f.coord.CurrentOperator(t.Context())
			if ex.State == OperatorSucceeded || f.store.activationRuns != 0 {
				t.Fatal("diagnostic status advanced Active", ex)
			}
			f.engine.groups = f.groups
			f.engine.finish(t.Context(), ex, target, &step)
			ex, _ = f.coord.CurrentOperator(t.Context())
			if ex.State != OperatorSucceeded || f.store.activationRuns != 1 {
				t.Fatal("confirmed membership did not unblock completion", ex)
			}
		})
	}
}
