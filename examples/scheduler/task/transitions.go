package task

import (
	"encoding/json"
	"math"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/load"
)

const (
	codeInvalidCommand  = "invalid_command"
	codeStale           = "stale"
	codeOK              = "ok"
	commandStart        = "start"
	stateRunning        = "running"
	maxFinishErrorBytes = 512
)

func result(code string, value CommandResult) load.Result {
	data, _ := json.Marshal(value)
	return load.Result{Code: code, Data: data}
}

// Reject returns a deterministic refusal with an empty payload.
func Reject(code string) load.Result { return result(code, CommandResult{}) }

// MissingResult distinguishes pruned history from unknown IDs.
func MissingResult(meta Metadata, id int64) load.Result {
	if id > 0 && id <= meta.LastTaskID {
		return Reject("task_retired")
	}
	return Reject("not_found")
}

// TransitionResult contains the resulting view, changed rows and deterministic reply.
type TransitionResult struct {
	State   State
	Changed []Task
	Result  load.Result
}

type transition struct {
	view        State
	index       int64
	tasks       map[int64]Task
	outstanding map[uint64]int64
	pending     bool
}

// Transition applies one decoded command to view at log index. It returns the
// updated view, the complete set of changed rows and the result. A refusal
// changes nothing; batches are all-or-nothing.
//
// Scheduler commands need no epoch: Leadership.Propose admits them only in the
// proposer's term, and a new leader reads after every surviving older entry.
// Their CAS expectations reject anything decided on a stale view.
func Transition(view State, c Command, index int64) TransitionResult {
	change := transition{view: view, index: index, tasks: make(map[int64]Task, len(view.Outstanding)+len(view.Lookup)),
		outstanding: map[uint64]int64{}}
	for _, t := range view.Outstanding {
		change.tasks[t.TaskID] = t
		change.pending = change.pending || t.State == "pending"
		if t.ExecutorReplicaID != nil && *t.ExecutorReplicaID > 0 {
			change.outstanding[uint64(*t.ExecutorReplicaID)]++
		}
	}
	for id, t := range view.Lookup {
		change.tasks[id] = t
	}
	switch c.Kind {
	case "create":
		return change.create(c)
	case "assign":
		return change.assign(c)
	case "revoke":
		return change.revoke(c)
	case commandStart, "finish":
		return change.attempt(c)
	default:
		return TransitionResult{State: view, Result: Reject(codeInvalidCommand)}
	}
}

func (change *transition) hasRoom(e Executor) bool {
	return change.outstanding[e.ReplicaID] < QueueCapacity+1
}

// attempt handles executor commands for one exact (task, attempt, executor).
//
// TODO(too-complex): Apply does not refuse a start from the current scheduler
// node, because the replicated state no longer names the scheduler. The
// executor starts nothing new while leader and the scheduler revokes its own
// queued assignments instead.
func (change *transition) attempt(c Command) TransitionResult {
	command, valid := decodeAttempt(c)
	if !valid {
		return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
	}
	key, executor, finish := command.key, command.executor, command.finish

	t, ok := change.tasks[key.TaskID]
	if !ok {
		return TransitionResult{State: change.view, Result: MissingResult(change.view.Meta, key.TaskID)}
	}
	if t.Attempt != key.Attempt || t.ExecutorReplicaID == nil || *t.ExecutorReplicaID <= 0 ||
		uint64(*t.ExecutorReplicaID) != executor.ReplicaID {
		return TransitionResult{State: change.view, Result: Reject(codeStale)}
	}
	if Terminal(t) {
		if repeatedAttempt(t, c.Kind, finish) {
			return TransitionResult{State: change.view, Result: result(t.State, CommandResult{Task: &t})}
		}
		return TransitionResult{State: change.view, Result: Reject("conflict")}
	}
	if c.Kind == commandStart {
		if t.State == stateRunning {
			return TransitionResult{State: change.view, Result: result("already_running", CommandResult{Task: &t})}
		}
		t.State, t.StartedIndex, t.UpdatedIndex = stateRunning, &change.index, change.index
		return TransitionResult{State: change.view, Changed: []Task{t}, Result: result(codeOK, CommandResult{Task: &t})}
	}
	if t.State != stateRunning {
		return TransitionResult{State: change.view, Result: Reject("conflict")}
	}
	t.FinishedIndex, t.UpdatedIndex = &change.index, change.index
	if finish.Error != "" {
		t.State, t.ErrorText = "failed", &finish.Error
		change.view.Meta.FailedCount++
	} else {
		t.State, t.Result = "succeeded", &finish.Value
		change.view.Meta.SucceededCount++
	}
	return TransitionResult{State: change.view, Changed: []Task{t}, Result: result(t.State, CommandResult{Task: &t})}
}

func (change *transition) create(c Command) TransitionResult {
	var changed []Task
	var ids []int64
	var b Create
	decodeErr := jsonwire.Decode(c.Body, &b)
	if decodeErr != nil || len(b.Tasks) == 0 || len(b.Tasks) > BurstSize {
		return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
	}
	if b.ExpectedLastTaskID != change.view.Meta.LastTaskID {
		return TransitionResult{State: change.view, Result: Reject(codeStale)}
	}
	if change.pending || change.view.Meta.LastTaskID > math.MaxInt64-int64(len(b.Tasks)) {
		return TransitionResult{State: change.view, Result: Reject("capacity_full")}
	}
	for i, n := range b.Tasks {
		if !CanonicalHash(n.Seed) || n.DurationMS < MinDuration.Milliseconds() ||
			n.DurationMS > MaxDuration.Milliseconds() || !ValidExecutor(n.Executor) {
			return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
		}
		if !change.hasRoom(n.Executor) {
			return TransitionResult{State: change.view, Result: Reject("capacity_full")}
		}
		change.outstanding[n.Executor.ReplicaID]++
		t := Task{
			TaskID:       change.view.Meta.LastTaskID + int64(i) + 1,
			State:        "assigned",
			Seed:         n.Seed,
			DurationMS:   n.DurationMS,
			Attempt:      1,
			CreatedIndex: change.index,
			UpdatedIndex: change.index,
		}
		if err := t.setOwner(n.Executor); err != nil {
			return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
		}
		changed = append(changed, t)
		ids = append(ids, t.TaskID)
	}
	change.view.Meta.LastTaskID += int64(len(b.Tasks))

	return TransitionResult{State: change.view, Changed: changed, Result: result(codeOK, CommandResult{TaskIDs: ids})}
}

func (change *transition) assign(c Command) TransitionResult {
	var changed []Task
	var ids []int64
	seen := map[int64]bool{}
	var b Assign
	decodeErr := jsonwire.Decode(c.Body, &b)
	if decodeErr != nil || len(b.Tasks) == 0 || len(b.Tasks) > MaxBatch {
		return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
	}
	for _, item := range b.Tasks {
		if item.TaskID <= 0 || item.ExpectedAttempt <= 0 || seen[item.TaskID] || !ValidExecutor(item.Executor) {
			return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
		}
		seen[item.TaskID] = true
		t, ok := change.tasks[item.TaskID]
		if !ok {
			return TransitionResult{State: change.view, Result: MissingResult(change.view.Meta, item.TaskID)}
		}
		if t.State != "pending" || t.Attempt != item.ExpectedAttempt || t.Attempt == math.MaxInt64 {
			return TransitionResult{State: change.view, Result: Reject(codeStale)}
		}
		if !change.hasRoom(item.Executor) {
			return TransitionResult{State: change.view, Result: Reject("capacity_full")}
		}
		change.outstanding[item.Executor.ReplicaID]++
		t.Attempt++
		t.State, t.UpdatedIndex = "assigned", change.index
		if err := t.setOwner(item.Executor); err != nil {
			return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
		}
		changed = append(changed, t)
		ids = append(ids, t.TaskID)
	}

	return TransitionResult{State: change.view, Changed: changed, Result: result(codeOK, CommandResult{TaskIDs: ids})}
}

func (change *transition) revoke(c Command) TransitionResult {
	var changed []Task
	var ids []int64
	seen := map[int64]bool{}
	var b Revoke
	decodeErr := jsonwire.Decode(c.Body, &b)
	if decodeErr != nil || len(b.Tasks) == 0 || len(b.Tasks) > MaxBatch {
		return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
	}
	for _, item := range b.Tasks {
		if seen[item.Key.TaskID] || (item.ExpectedState != "assigned" && item.ExpectedState != stateRunning) ||
			!KeyValid(item.Key) {
			return TransitionResult{State: change.view, Result: Reject(codeInvalidCommand)}
		}
		seen[item.Key.TaskID] = true
		t, ok := change.tasks[item.Key.TaskID]
		if !ok {
			return TransitionResult{State: change.view, Result: MissingResult(change.view.Meta, item.Key.TaskID)}
		}
		if t.Attempt != item.Key.Attempt || t.State != item.ExpectedState {
			return TransitionResult{State: change.view, Result: Reject(codeStale)}
		}
		t.State, t.UpdatedIndex = "pending", change.index
		t.ExecutorReplicaID, t.ExecutorNode, t.StartedIndex, t.FinishedIndex,
			t.Result, t.ErrorText = nil, nil, nil, nil, nil, nil
		changed = append(changed, t)
		ids = append(ids, t.TaskID)
	}

	return TransitionResult{State: change.view, Changed: changed, Result: result(codeOK, CommandResult{TaskIDs: ids})}
}

type attemptCommand struct {
	key      Key
	executor Executor
	finish   Finish
}

func decodeAttempt(c Command) (attemptCommand, bool) {
	var command attemptCommand
	if c.Kind == commandStart {
		var body Start
		if jsonwire.Decode(c.Body, &body) != nil {
			return command, false
		}
		command.key, command.executor = body.Key, body.Executor
	} else {
		finish := &command.finish
		decodeErr := jsonwire.Decode(c.Body, finish)
		if decodeErr != nil ||
			(finish.Error != "" && (finish.Value != "" || len(finish.Error) > maxFinishErrorBytes)) ||
			(finish.Error == "" && !CanonicalHash(finish.Value)) {
			return command, false
		}
		command.key, command.executor = finish.Key, finish.Executor
	}
	return command, KeyValid(command.key) && ValidExecutor(command.executor)
}

func repeatedAttempt(t Task, kind string, finish Finish) bool {
	return kind == commandStart || (t.State == "succeeded" && t.Result != nil && *t.Result == finish.Value &&
		finish.Error == "") || (t.State == "failed" && t.ErrorText != nil &&
		*t.ErrorText == finish.Error && finish.Value == "")
}
