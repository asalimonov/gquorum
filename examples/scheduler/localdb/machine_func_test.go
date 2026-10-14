//go:build functional

package localdb

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
)

var testSeed, testValue = strings.Repeat("a", 64), strings.Repeat("b", 64)

type machineDriver struct {
	t     *testing.T
	m     *Machine
	index uint64
	log   []load.Command
}

func newMachineDriver(t *testing.T) *machineDriver {
	t.Helper()
	db := schemaFixture(t)
	if err := ensureSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	return &machineDriver{t: t, m: &Machine{db: db, identity: storage.Identity{NodeName: "node1", ClusterID: "test", GroupID: "scheduler", ReplicaID: 11}}}
}

func (d *machineDriver) command(kind string, body any, want string) task.CommandResult {
	d.t.Helper()
	p, err := task.Encode(kind, body)
	if err != nil {
		d.t.Fatal(err)
	}
	d.index++
	entry := load.Command{Index: d.index, Term: 1, RequestID: p.RequestID, Data: p.Data}
	d.log = append(d.log, entry)
	got, err := d.m.Apply(d.t.Context(), entry)
	if err != nil || got.Code != want {
		d.t.Fatalf("%s: got %s %s, %v; want %s", kind, got.Code, got.Data, err, want)
	}
	var out task.CommandResult
	if err := json.Unmarshal(got.Data, &out); err != nil {
		d.t.Fatal(err)
	}
	return out
}

func executor(n int) task.Executor {
	return task.Executor{ReplicaID: uint64(n + 10), NodeName: []string{"node1", "node2", "node3"}[n-1]}
}

func (d *machineDriver) state() task.State {
	d.t.Helper()
	out, err := d.m.Query(d.t.Context(), []byte(`{"kind":"state"}`))
	if err != nil || out.Code != "ok" {
		d.t.Fatal(out, err)
	}
	var view task.State
	if err := jsonwire.Decode(out.Data, &view); err != nil {
		d.t.Fatal(err)
	}
	return view
}

func (d *machineDriver) create(n int, e task.Executor) []int64 {
	d.t.Helper()
	body := task.Create{ExpectedLastTaskID: d.state().Meta.LastTaskID}
	for range n {
		body.Tasks = append(body.Tasks, task.NewTask{Seed: testSeed, DurationMS: 2000, Executor: e})
	}
	return d.command("create", body, "ok").TaskIDs
}

func TestFSMAppliesCompareAndSwapLifecycle(t *testing.T) {
	t.Parallel()
	d := newMachineDriver(t)
	ids := d.create(2, executor(2))
	if len(ids) != 2 {
		t.Fatal(ids)
	}
	// A lost-reply retry of the same create is refused, not duplicated.
	d.command("create", task.Create{ExpectedLastTaskID: 0, Tasks: []task.NewTask{{Seed: testSeed, DurationMS: 2000, Executor: executor(2)}}}, "stale")
	key := task.Key{TaskID: 1, Attempt: 1}
	d.command("start", task.Start{Key: key, Executor: executor(2)}, "ok")
	d.command("revoke", task.Revoke{Tasks: []task.Revocation{{Key: key, ExpectedState: "running"}, {Key: task.Key{TaskID: 2, Attempt: 1}, ExpectedState: "assigned"}}}, "ok")
	d.command("assign", task.Assign{Tasks: []task.Assignment{{TaskID: 1, ExpectedAttempt: 1, Executor: executor(3)}}}, "ok")
	d.command("finish", task.Finish{Key: key, Executor: executor(2), Value: testValue}, "stale")
	current := task.Key{TaskID: 1, Attempt: 2}
	d.command("start", task.Start{Key: current, Executor: executor(3)}, "ok")
	d.command("finish", task.Finish{Key: current, Executor: executor(3), Value: testValue}, "succeeded")
	d.command("finish", task.Finish{Key: current, Executor: executor(3), Value: testValue}, "succeeded")
	view := d.state()
	if view.Meta.SucceededCount != 1 || len(view.Outstanding) != 1 || view.Outstanding[0].State != "pending" || view.Outstanding[0].ExecutorReplicaID != nil {
		t.Fatalf("unexpected state %+v", view)
	}
	out, err := d.m.Apply(t.Context(), load.Command{Index: d.index + 1, Data: []byte(`{"version":2,"kind":"start","body":{"sql":"DROP TABLE tasks"}}`)})
	if err != nil || out.Code != "invalid_command" {
		t.Fatal(out, err)
	}
	for _, q := range []string{`{"kind":"task","task_id":99}`, `{"kind":"tasks","limit":0}`, `{"kind":"sql"}`} {
		if out, err := d.m.Query(t.Context(), []byte(q)); err != nil || out.Code == "ok" {
			t.Fatal("invalid query accepted", q, out, err)
		}
	}
}

func TestFSMSnapshotMatchesReplayAndPrunesHistory(t *testing.T) {
	t.Parallel()
	d := newMachineDriver(t)
	d.create(1, executor(2))
	d.command("start", task.Start{Key: task.Key{TaskID: 1, Attempt: 1}, Executor: executor(2)}, "ok")
	d.create(1, executor(3))
	snapshot, err := d.m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	replay := newMachineDriver(t)
	for _, entry := range d.log {
		if _, err := replay.m.Apply(t.Context(), entry); err != nil {
			t.Fatal(err)
		}
	}
	replayed, err := replay.m.Snapshot(t.Context())
	if err != nil || !bytes.Equal(snapshot, replayed) {
		t.Fatal("replay differs from snapshot", err)
	}
	restored := newMachineDriver(t)
	if err := restored.m.Restore(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	if got, err := restored.m.Snapshot(t.Context()); err != nil || !bytes.Equal(got, snapshot) {
		t.Fatal("restore differs", err)
	}
	if err := restored.m.Restore(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if view := restored.state(); view.Meta.LastTaskID != 0 || len(view.Outstanding) != 0 {
		t.Fatal("Restore(nil) kept rows", view)
	}
	var foreign snapshotState
	if err := jsonwire.Decode(snapshot, &foreign); err != nil {
		t.Fatal(err)
	}
	foreign.Meta.SucceededCount = 5
	data, _ := json.Marshal(foreign)
	if err := restored.m.Restore(t.Context(), data); err == nil {
		t.Fatal("inconsistent counters restored")
	}

	// Fill the terminal history directly, then finish one more task.
	history := newMachineDriver(t)
	tx, err := beginTransaction(t.Context(), history.m.db, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]task.Task, 0, task.HistoryLimit)
	for id := int64(1); id <= task.HistoryLimit; id++ {
		replica, node, value, index := int64(12), "node2", testValue, id
		rows = append(rows, task.Task{TaskID: id, State: "succeeded", Seed: testSeed, DurationMS: 2000, Attempt: 1, ExecutorReplicaID: &replica, ExecutorNode: &node, CreatedIndex: id, UpdatedIndex: id, StartedIndex: &index, FinishedIndex: &index, Result: &value})
	}
	if err := insertTasks(tx, rows); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadata(tx, task.Metadata{ID: 1, LastTaskID: task.HistoryLimit, SucceededCount: task.HistoryLimit}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Close()
	history.index = 2 * task.HistoryLimit
	id := history.create(1, executor(2))[0]
	key := task.Key{TaskID: id, Attempt: 1}
	history.command("start", task.Start{Key: key, Executor: executor(2)}, "ok")
	history.command("finish", task.Finish{Key: key, Executor: executor(2), Value: testValue}, "succeeded")
	out, err := history.m.Query(t.Context(), []byte(`{"kind":"task","task_id":1}`))
	if err != nil || out.Code != "task_retired" {
		t.Fatal("oldest terminal row not pruned", out, err)
	}
	if _, err := history.m.Snapshot(t.Context()); err != nil {
		t.Fatal("pruned history fails validation", err)
	}
}
