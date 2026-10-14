//go:build !functional

package gquorum

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestNodeWatchBeforeStartBoundAndFinalState(t *testing.T) {
	t.Parallel()
	opts := nodeOptionsForTest(t)
	opts.MaxWatchers = 1
	n, err := NewNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	watch, err := n.Watch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	initial := <-watch
	if initial.Revision != 1 || initial.Status.State != NodeNew {
		t.Fatal(initial)
	}
	if _, err := n.Watch(t.Context()); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	// Existing construction fixtures panic if dependencies are acquired. Stop
	// before Start plus watching must retain the no-I/O construction contract.
	if err := n.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	final, open := <-watch
	if !open || final.Status.State != NodeStopped || final.Revision <= initial.Revision {
		t.Fatal(final, open)
	}
	if _, open := <-watch; open {
		t.Fatal("watch did not close")
	}
	late, err := n.Watch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if o := <-late; o.Revision != final.Revision || o.Status.State != NodeStopped {
		t.Fatal(o)
	}
	if _, open := <-late; open {
		t.Fatal("late watch did not close")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := n.Watch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestNodeObservationOwnsNestedStatus(t *testing.T) {
	t.Parallel()
	version := uint64(4)
	operator := topology.OperatorID("operation")
	original := NodeObservation{Status: NodeStatus{
		Meta:        topology.GroupStatus{ConfState: raftpb.ConfState{Voters: []uint64{1}}, Progress: map[uint64]topology.ReplicaProgress{1: {Match: 4}}, LiveVoters: map[uint64]bool{1: true}},
		Scheduling:  topology.SchedulingStatus{BlockingVersion: &version, CurrentOperatorID: &operator, CurrentAction: &topology.SchedulingActionStatus{GroupID: "work"}},
		SelfRemoval: &topology.SelfRemovalStatus{Version: 4}, Loads: []LocalReplicaStatus{{RunID: "original"}},
	}}
	copy := cloneNodeObservation(original)
	copy.Status.Meta.ConfState.Voters[0] = 9
	copy.Status.Meta.Progress[1] = topology.ReplicaProgress{Match: 9}
	copy.Status.Meta.LiveVoters[1] = false
	*copy.Status.Scheduling.BlockingVersion = 9
	*copy.Status.Scheduling.CurrentOperatorID = "changed"
	copy.Status.Scheduling.CurrentAction.GroupID = "changed"
	copy.Status.SelfRemoval.Version = 9
	copy.Status.Loads[0].RunID = "changed"
	if original.Status.Meta.ConfState.Voters[0] != 1 || original.Status.Meta.Progress[1].Match != 4 || !original.Status.Meta.LiveVoters[1] || version != 4 || operator != "operation" || original.Status.Scheduling.CurrentAction.GroupID != "work" || original.Status.SelfRemoval.Version != 4 || original.Status.Loads[0].RunID != "original" {
		t.Fatal("clone retained mutable aliases", original)
	}
}
