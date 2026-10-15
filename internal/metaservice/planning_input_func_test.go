//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Native membership is independent of the controller's cached status sample.
type planningController struct {
	*statusController
	native    topology.NativeState
	nativeErr error
}

func (c *planningController) ReadNative(context.Context, string, ...storage.Identity) (topology.NativeState, error) {
	return c.native.Clone(), c.nativeErr
}

func TestPlanForUsesNativeMembershipInsteadOfCachedConfState(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"different membership", "foreign group", "unknown reporter", "zero applied"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			controller := &planningController{statusController: &statusController{value: topology.GroupStatus{
				GroupID: "work", Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
				HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true}}},
				native: topology.NativeState{GroupID: "work", ReplicaID: 1, Applied: 20,
					ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}
			switch mode {
			case "foreign group":
				controller.native.GroupID = "foreign"
			case "unknown reporter":
				controller.native.ReplicaID = 99
			case "zero applied":
				controller.native.Applied = 0
			}
			g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) {
				o.Limits.MaxNodes = 2
				o.LoadGroups = controller
				o.Founders = append(o.Founders, FoundingMember{Node: topology.Node{Name: "n2", Type: "worker"},
					MemberID: 2, Address: "127.0.0.1:1", PinnedLearner: true})
			}))
			g.controller.Stop()
			doc := g.state().Active()
			doc.Groups = append(doc.Groups, topology.Group{ID: "work", NextReplicaID: 3, Replicas: []topology.Replica{
				{NodeName: g.Identity().NodeName, ReplicaID: 1, Role: topology.ReplicaVoter},
				{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner},
			}})
			desired, err := g.Stage(t.Context(), doc, doc.Version)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := g.PlanFor(t.Context(), desired)
			if mode != "different membership" {
				if !errors.Is(err, fault.ErrIdentityMismatch) {
					t.Fatal("invalid native state was accepted", err)
				}
				return
			}
			if err != nil || len(plan.Steps) != 0 {
				t.Fatal("cached membership created an unnecessary demotion", plan, err)
			}
			input, err := g.planningInput(t.Context(), desired)
			if err != nil {
				t.Fatal(err)
			}
			if len(input.Groups["work"].Observation.ConfState.Voters) != 2 || len(input.Native["work"].ConfState.Voters) != 1 {
				t.Fatal("observation and native evidence were conflated")
			}
			input.Groups["work"].Observation.LiveVoters[1] = false
			input.Native["work"].ConfState.Voters[0] = 99
			if !controller.value.LiveVoters[1] || controller.native.ConfState.Voters[0] != 1 {
				t.Fatal("planning input aliases caller-owned evidence")
			}
		})
	}
}

func TestPlanningNativeAbsenceOnlyAllowsFreshFounding(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"fresh", "converged", "issued", "retained", "deleted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newMutationFixture(t)
			f.g.opts.LoadGroups = &planningController{nativeErr: fault.ErrUnavailable}
			active := f.g.machine.Active()
			input := topology.PlanInput{Groups: map[string]topology.GroupProgress{}, Retained: map[string]topology.RetainedGroup{}}
			switch mode {
			case "converged":
				input.Groups["work"] = topology.GroupProgress{ConvergedVersion: 1, Converged: active.Group("work")}
			case "issued":
				input.Retained["work"] = topology.RetainedGroup{FoundingIssued: true}
			case "retained":
				input.Retained["work"] = topology.RetainedGroup{Replicas: []topology.RetainedReplica{
					{ReplicaKey: topology.ReplicaKey{NodeName: "n1", ReplicaID: 1}, Progress: topology.RetainedHosted}}}
			case "deleted":
				input.Groups["work"] = topology.GroupProgress{ConvergedVersion: 2}
			}
			read, err := f.g.readPlanningGroup(t.Context(), input, f.g.machine.Latest(), "work")
			if mode == "fresh" {
				if err != nil || read.native.Applied != 0 || read.observation.Found {
					t.Fatal("fresh founding was not preserved", read, err)
				}
			} else if !errors.Is(err, fault.ErrUnavailable) {
				t.Fatal("missing established state authorized refounding", err)
			}
		})
	}
}

func TestPlanningDeletionDoesNotReadDeletedGroupQuorum(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	// All declarations are removed for this collector-only probe. There is no
	// live engine or load controller; any native read would fail immediately.
	desired := f.g.machine.Active()
	desired.Groups = nil
	input, err := f.g.planningInput(t.Context(), desired)
	if err != nil || len(input.Native) != 0 || input.Groups["work"].Converged == nil {
		t.Fatal("host-only deletion depended on a deleted group's quorum", input, err)
	}
}
