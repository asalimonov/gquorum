//go:build !functional

package task

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/load"
)

var (
	node2 = Executor{ReplicaID: 2, NodeName: "node2"}
	node3 = Executor{ReplicaID: 3, NodeName: "node3"}
	seed  = strings.Repeat("a", 64)
	value = strings.Repeat("b", 64)
)

// machine replays commands against an in-memory state, like Apply does.
type machine struct {
	t     *testing.T
	state State
	rows  map[int64]Task
	index int64
}

func newMachine(t *testing.T) *machine {
	return &machine{t: t, state: State{Meta: Metadata{ID: 1}}, rows: map[int64]Task{}}
}

func (m *machine) apply(kind string, body any, want string) CommandResult {
	m.t.Helper()
	p, err := Encode(kind, body)
	if err != nil {
		m.t.Fatal(err)
	}
	m.index++
	c, err := Decode(load.Command{Index: uint64(m.index), RequestID: p.RequestID, Data: p.Data})
	if err != nil {
		m.t.Fatal(err)
	}
	view := m.view()
	view.Lookup = map[int64]Task{}
	for _, id := range ReferencedIDs(c) {
		if row, ok := m.rows[id]; ok {
			view.Lookup[id] = row
		}
	}
	transition := Transition(view, c, m.index)
	next, changed, out := transition.State, transition.Changed, transition.Result
	if out.Code != want {
		m.t.Fatalf("%s: got %s %s, want %s", kind, out.Code, out.Data, want)
	}
	m.state.Meta = next.Meta
	for _, row := range changed {
		m.rows[row.TaskID] = row
	}
	var decoded CommandResult
	if err := json.Unmarshal(out.Data, &decoded); err != nil {
		m.t.Fatal(err)
	}
	return decoded
}

func (m *machine) view() State {
	view := State{Meta: m.state.Meta}
	for id := int64(1); id <= m.state.Meta.LastTaskID; id++ {
		if row, ok := m.rows[id]; ok && !Terminal(row) {
			view.Outstanding = append(view.Outstanding, row)
		}
	}
	return view
}

func (m *machine) create(executors ...Executor) []int64 {
	m.t.Helper()
	create := Create{ExpectedLastTaskID: m.state.Meta.LastTaskID}
	for _, e := range executors {
		create.Tasks = append(create.Tasks, NewTask{Seed: seed, DurationMS: 2000, Executor: e})
	}
	return m.apply("create", create, "ok").TaskIDs
}

func TestCreateIsCompareAndSwapOnLastTaskID(t *testing.T) {
	t.Parallel()
	m := newMachine(t)
	ids := m.create(node2, node3)
	if len(ids) != 2 || ids[0] != 1 || m.state.Meta.LastTaskID != 2 {
		t.Fatal(ids, m.state.Meta)
	}
	// A retried create after a lost reply carries the old expectation.
	m.apply("create", Create{ExpectedLastTaskID: 0, Tasks: []NewTask{{Seed: seed, DurationMS: 2000, Executor: node2}}}, "stale")
	m.apply("create", Create{ExpectedLastTaskID: 2, Tasks: []NewTask{{Seed: seed, DurationMS: 100, Executor: node2}}}, "invalid_command")
	m.apply("create", Create{ExpectedLastTaskID: 2, Tasks: []NewTask{{Seed: "x", DurationMS: 2000, Executor: node2}}}, "invalid_command")
	m.apply("create", Create{ExpectedLastTaskID: 2}, "invalid_command")
	if m.state.Meta.LastTaskID != 2 {
		t.Fatal("refusal changed metadata")
	}
}

func TestCapacityPerExecutorAndPendingPause(t *testing.T) {
	t.Parallel()
	m := newMachine(t)
	m.create(node2, node2, node2)
	m.create(node2, node2) // QueueCapacity waiting plus one running slot
	m.apply("create", Create{ExpectedLastTaskID: m.state.Meta.LastTaskID, Tasks: []NewTask{{Seed: seed, DurationMS: 2000, Executor: node2}}}, "capacity_full")
	m.create(node3)
	// Revoked work is pending; creation pauses until it is reassigned.
	m.apply("revoke", Revoke{Tasks: []Revocation{{Key: Key{TaskID: 1, Attempt: 1}, ExpectedState: "assigned"}}}, "ok")
	m.apply("create", Create{ExpectedLastTaskID: m.state.Meta.LastTaskID, Tasks: []NewTask{{Seed: seed, DurationMS: 2000, Executor: node3}}}, "capacity_full")
}

func TestAssignAndRevokeAreFencedByAttemptAndState(t *testing.T) {
	t.Parallel()
	m := newMachine(t)
	m.create(node2)
	m.apply("assign", Assign{Tasks: []Assignment{{TaskID: 1, ExpectedAttempt: 1, Executor: node3}}}, "stale") // not pending
	m.apply("revoke", Revoke{Tasks: []Revocation{{Key: Key{TaskID: 1, Attempt: 1}, ExpectedState: "running"}}}, "stale")
	m.apply("revoke", Revoke{Tasks: []Revocation{{Key: Key{TaskID: 1, Attempt: 1}, ExpectedState: "assigned"}}}, "ok")
	if row := m.rows[1]; row.State != "pending" || row.ExecutorReplicaID != nil {
		t.Fatal(row)
	}
	m.apply("revoke", Revoke{Tasks: []Revocation{{Key: Key{TaskID: 1, Attempt: 1}, ExpectedState: "assigned"}}}, "stale")
	m.apply("assign", Assign{Tasks: []Assignment{{TaskID: 1, ExpectedAttempt: 1, Executor: node3}, {TaskID: 1, ExpectedAttempt: 1, Executor: node3}}}, "invalid_command")
	m.apply("assign", Assign{Tasks: []Assignment{{TaskID: 1, ExpectedAttempt: 1, Executor: node3}}}, "ok")
	if row := m.rows[1]; row.State != "assigned" || row.Attempt != 2 || row.Owner() != node3 {
		t.Fatal(row)
	}
	m.apply("assign", Assign{Tasks: []Assignment{{TaskID: 1, ExpectedAttempt: 1, Executor: node3}}}, "stale")
	m.apply("assign", Assign{Tasks: []Assignment{{TaskID: 99, ExpectedAttempt: 1, Executor: node3}}}, "not_found")
}

func TestStartAndFinishAreIdempotentAndFenced(t *testing.T) {
	t.Parallel()
	m := newMachine(t)
	m.create(node2)
	key := Key{TaskID: 1, Attempt: 1}
	m.apply("start", Start{Key: key, Executor: node3}, "stale")
	m.apply("finish", Finish{Key: key, Executor: node2, Value: value}, "conflict") // not started
	m.apply("start", Start{Key: key, Executor: node2}, "ok")
	m.apply("start", Start{Key: key, Executor: node2}, "already_running")
	out := m.apply("finish", Finish{Key: key, Executor: node2, Value: value}, "succeeded")
	if out.Task == nil || *out.Task.Result != value || m.state.Meta.SucceededCount != 1 {
		t.Fatal(out, m.state.Meta)
	}
	m.apply("finish", Finish{Key: key, Executor: node2, Value: value}, "succeeded")
	m.apply("finish", Finish{Key: key, Executor: node2, Value: seed}, "conflict")
	m.apply("start", Start{Key: key, Executor: node2}, "succeeded")
	if m.state.Meta.SucceededCount != 1 {
		t.Fatal("retry counted twice")
	}
	m.apply("finish", Finish{Key: key, Executor: node2, Value: value, Error: "both"}, "invalid_command")
}

func TestStaleAttemptCannotFinishAfterReassignment(t *testing.T) {
	t.Parallel()
	m := newMachine(t)
	m.create(node2)
	old := Key{TaskID: 1, Attempt: 1}
	m.apply("start", Start{Key: old, Executor: node2}, "ok")
	m.apply("revoke", Revoke{Tasks: []Revocation{{Key: old, ExpectedState: "running"}}}, "ok")
	m.apply("assign", Assign{Tasks: []Assignment{{TaskID: 1, ExpectedAttempt: 1, Executor: node3}}}, "ok")
	m.apply("finish", Finish{Key: old, Executor: node2, Value: value}, "stale")
	current := Key{TaskID: 1, Attempt: 2}
	m.apply("start", Start{Key: current, Executor: node3}, "ok")
	m.apply("finish", Finish{Key: current, Executor: node3, Error: "business failure"}, "failed")
	if m.state.Meta.FailedCount != 1 || m.rows[1].State != "failed" {
		t.Fatal(m.state.Meta, m.rows[1])
	}
}

func TestMissingResultDistinguishesRetiredHistory(t *testing.T) {
	t.Parallel()
	meta := Metadata{LastTaskID: 5}
	if MissingResult(meta, 3).Code != "task_retired" || MissingResult(meta, 6).Code != "not_found" {
		t.Fatal("missing classification")
	}
}
