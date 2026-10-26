package main

import (
	"time"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/load"
)

const (
	suspectAfter    = 5 * time.Second  // owner inactive this long loses its work
	runningGrace    = 10 * time.Second // allowance beyond a task's own duration
	schedulerTick   = time.Second
	proposalTimeout = 3 * time.Second // re-read task state promptly after uncertainty
)

// schedulerSession holds one leadership term's local timers. They are hints:
// every decision is a CAS command that Apply re-validates.
type schedulerSession struct {
	suspect map[uint64]time.Time   // executor replica ID → first seen inactive
	running map[task.Key]time.Time // attempt → first seen running
}

func newSchedulerSession() *schedulerSession {
	return &schedulerSession{suspect: map[uint64]time.Time{}, running: map[task.Key]time.Time{}}
}

// observe refreshes suspicion timers from the leader's Raft progress and
// returns the replicas currently considered alive voters.
func (s *schedulerSession) observe(now time.Time, replicas []load.ReplicaProgress) map[uint64]load.ReplicaProgress {
	voters := map[uint64]load.ReplicaProgress{}
	for _, r := range replicas {
		if !r.Voter {
			continue
		}
		voters[r.ReplicaID] = r
		if r.RecentActive {
			delete(s.suspect, r.ReplicaID)
		} else if _, ok := s.suspect[r.ReplicaID]; !ok {
			s.suspect[r.ReplicaID] = now
		}
	}
	for id := range s.suspect {
		if _, ok := voters[id]; !ok {
			delete(s.suspect, id)
		}
	}
	return voters
}

// revocations decides which assignments to return to pending:
//   - the leader's own queued (assigned) work, immediately;
//   - work owned by a replica that is no longer a voter, immediately;
//   - work owned by a replica inactive for suspectAfter;
//   - a running attempt seen longer than its duration plus runningGrace.
//
// The leader's own running task is kept so it can finish.
func (s *schedulerSession) revocations(now time.Time, self uint64, view task.State,
	replicas []load.ReplicaProgress) task.Revoke {
	voters := s.observe(now, replicas)
	var revoke task.Revoke
	live := map[task.Key]bool{}
	for _, t := range view.Outstanding {
		if t.State == "pending" {
			continue
		}
		key, owner := t.Key(), t.Owner().ReplicaID
		live[key] = true
		s.observeRunning(now, t)
		_, voter := voters[owner]
		since, suspected := s.suspect[owner]
		expired := t.State == "running" &&
			now.Sub(s.running[key]) > time.Duration(t.DurationMS)*time.Millisecond+runningGrace
		if (owner == self && t.State == "assigned") || !voter || expired ||
			(suspected && now.Sub(since) >= suspectAfter) {
			if len(revoke.Tasks) < task.MaxBatch {
				revoke.Tasks = append(revoke.Tasks, task.Revocation{Key: key, ExpectedState: t.State})
			}
		}
	}
	for key := range s.running {
		if !live[key] {
			delete(s.running, key)
		}
	}
	return revoke
}

// freeSlots returns, per eligible executor, how many more assignments fit. An
// eligible executor is an active voter other than the leader.
func freeSlots(self uint64, view task.State, replicas []load.ReplicaProgress, s *schedulerSession) map[uint64]int {
	used := map[uint64]int{}
	for _, t := range view.Outstanding {
		if t.ExecutorReplicaID != nil && *t.ExecutorReplicaID > 0 {
			used[uint64(*t.ExecutorReplicaID)]++
		}
	}
	slots := map[uint64]int{}
	for _, r := range replicas {
		if _, suspected := s.suspect[r.ReplicaID]; r.Voter && r.RecentActive && !suspected && r.ReplicaID != self &&
			r.NodeName != "" {
			if free := task.QueueCapacity + 1 - used[r.ReplicaID]; free > 0 {
				slots[r.ReplicaID] = free
			}
		}
	}
	return slots
}

// pickExecutor reserves one slot on a randomly chosen eligible executor.
func pickExecutor(slots map[uint64]int, replicas []load.ReplicaProgress, intn func(int) int) (task.Executor, bool) {
	var candidates []load.ReplicaProgress
	for _, r := range replicas { // replicas are sorted, so the choice is reproducible for a given intn
		if slots[r.ReplicaID] > 0 {
			candidates = append(candidates, r)
		}
	}
	if len(candidates) == 0 {
		return task.Executor{}, false
	}
	chosen := candidates[intn(len(candidates))]
	slots[chosen.ReplicaID]--
	return task.Executor{ReplicaID: chosen.ReplicaID, NodeName: chosen.NodeName}, true
}

func (s *schedulerSession) observeRunning(now time.Time, t task.Task) {
	key := t.Key()
	if t.State == "running" {
		if _, seen := s.running[key]; !seen {
			s.running[key] = now
		}
	}
}
