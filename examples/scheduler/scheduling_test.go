//go:build !functional

package main

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/load"
)

const leader = 1

func cluster(active ...bool) []load.ReplicaProgress {
	replicas := []load.ReplicaProgress{{ReplicaID: leader, NodeName: "node1", Voter: true, Self: true, RecentActive: true}}
	for i, ok := range active {
		replicas = append(replicas, load.ReplicaProgress{ReplicaID: uint64(i + 2), NodeName: []string{"node2", "node3"}[i], Voter: true, RecentActive: ok})
	}
	return replicas
}

func owned(id int64, state string, replica uint64) task.Task {
	t := task.Task{TaskID: id, State: state, Attempt: 1, DurationMS: 2000}
	if replica != 0 {
		r, node := int64(replica), "node"
		t.ExecutorReplicaID, t.ExecutorNode = &r, &node
	}
	return t
}

func revoked(r task.Revoke) map[int64]string {
	out := map[int64]string{}
	for _, item := range r.Tasks {
		out[item.Key.TaskID] = item.ExpectedState
	}
	return out
}

func TestRevocationRules(t *testing.T) {
	t.Parallel()
	now := time.Now()
	view := task.State{Outstanding: []task.Task{
		owned(1, "assigned", leader), // promoted executor's queue: revoke now
		owned(2, "running", leader),  // promoted executor's running task: keep
		owned(3, "assigned", 2),      // healthy executor: keep
		owned(4, "assigned", 9),      // not a voter any more: revoke now
		owned(5, "pending", 0),       // already pending: ignore
	}}
	s := newSchedulerSession()
	got := revoked(s.revocations(now, leader, view, cluster(true)))
	if len(got) != 2 || got[1] != "assigned" || got[4] != "assigned" {
		t.Fatal(got)
	}
}

func TestRevocationWaitsForSuspicionWindowAndRunningTimeout(t *testing.T) {
	t.Parallel()
	start := time.Now()
	view := task.State{Outstanding: []task.Task{owned(1, "running", 2), owned(2, "running", 3)}}
	s := newSchedulerSession()
	if got := revoked(s.revocations(start, leader, view, cluster(false, true))); len(got) != 0 {
		t.Fatal("revoked before the suspicion window", got)
	}
	if got := revoked(s.revocations(start.Add(suspectAfter), leader, view, cluster(false, true))); len(got) != 1 || got[1] != "running" {
		t.Fatal("inactive executor kept its work", got)
	}
	// Activity resets suspicion.
	s = newSchedulerSession()
	s.revocations(start, leader, view, cluster(false, true))
	if got := revoked(s.revocations(start.Add(suspectAfter), leader, view, cluster(true, true))); len(got) != 0 {
		t.Fatal("recovered executor lost its work", got)
	}
	// A running attempt seen beyond its duration plus grace is revoked.
	expiry := start.Add(2*time.Second + runningGrace + time.Millisecond)
	if got := revoked(s.revocations(expiry, leader, view, cluster(true, true))); len(got) != 2 {
		t.Fatal("stuck running attempts kept", got)
	}
}

func TestFreeSlotsAndPickExecutor(t *testing.T) {
	t.Parallel()
	view := task.State{}
	for id := int64(1); id <= task.QueueCapacity; id++ {
		view.Outstanding = append(view.Outstanding, owned(id, "assigned", 2))
	}
	replicas := cluster(true, false)
	s := newSchedulerSession()
	s.observe(time.Now(), replicas)
	slots := freeSlots(leader, view, replicas, s)
	if len(slots) != 1 || slots[2] != 1 {
		t.Fatal("leader or inactive executor eligible", slots)
	}
	first := func(int) int { return 0 }
	if e, ok := pickExecutor(slots, replicas, first); !ok || e.ReplicaID != 2 || e.NodeName != "node2" {
		t.Fatal(e, ok)
	}
	if _, ok := pickExecutor(slots, replicas, first); ok {
		t.Fatal("capacity exceeded")
	}
}

func TestNextTaskResumesRunningThenLowestAssigned(t *testing.T) {
	t.Parallel()
	view := task.State{Outstanding: []task.Task{owned(1, "assigned", 2), owned(2, "running", 2), owned(3, "assigned", 3), owned(4, "assigned", 2)}}
	next, queued := nextTask(view, 2)
	if next == nil || next.TaskID != 2 || len(queued) != 2 || queued[0].TaskID != 1 || queued[1].TaskID != 4 {
		t.Fatal(next, queued)
	}
	view.Outstanding[1].State = "pending"
	view.Outstanding[1].ExecutorReplicaID = nil
	if next, queued := nextTask(view, 2); next == nil || next.TaskID != 1 || len(queued) != 1 || queued[0].TaskID != 4 {
		t.Fatal("picked task must not be reported as queued", next, queued)
	}
	if next, queued := nextTask(view, 7); next != nil || len(queued) != 0 {
		t.Fatal(next, queued)
	}
}
