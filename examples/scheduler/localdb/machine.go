package localdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"xorm.io/xorm"
)

const (
	maxQueryBytes   = 4096
	maxTaskPageSize = 100
)

// Machine owns one replica's SQLite projection. All methods except Close run on
// the library's serialized state-machine lane; workers use Group.Read/Propose.
// The library restores the complete snapshot and replays the committed tail on
// every open, so the projection needs no applied-index bookkeeping.
type Machine struct {
	db       *xorm.Engine
	identity storage.Identity
}

var _ load.StateMachine = (*Machine)(nil)
var _ load.QueryMachine = (*Machine)(nil)

// Apply validates and transactionally applies a committed task command.
func (m *Machine) Apply(ctx context.Context, entry load.Command) (load.Result, error) {
	index, err := task.CheckedInt(entry.Index)
	if err != nil || index <= 0 {
		return load.Result{}, errors.New("invalid applied index")
	}
	c, err := task.Decode(entry)
	if err != nil {
		return task.Reject("invalid_command"), nil
	}
	tx, err := beginTransaction(ctx, m.db, nil)
	if err != nil {
		return load.Result{}, err
	}
	defer tx.Close()
	view, err := readState(tx)
	if err != nil {
		return load.Result{}, err
	}
	view.Lookup, err = lookupReferencedTasks(tx, task.ReferencedIDs(c))
	if err != nil {
		return load.Result{}, err
	}
	transition := task.Transition(view, c, index)
	view, changed, out := transition.State, transition.Changed, transition.Result
	if len(changed) == 0 {
		return out, nil // refusals and idempotent repeats change nothing
	}
	writeTasks := updateTasks
	if c.Kind == "create" {
		writeTasks = insertTasks
	}
	if err := writeTasks(tx, changed); err != nil {
		return load.Result{}, err
	}
	if err := writeMetadata(tx, view.Meta); err != nil {
		return load.Result{}, err
	}
	if c.Kind == "finish" && task.Terminal(changed[0]) {
		_, err = tx.Tx().Tx.ExecContext(ctx,
			`DELETE FROM tasks WHERE task_id IN (SELECT task_id FROM tasks WHERE `+
				`state IN ('succeeded','failed') ORDER BY `+
				`finished_index DESC, task_id DESC LIMIT -1 OFFSET ?)`, task.HistoryLimit)
		if err != nil {
			return load.Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return load.Result{}, err
	}
	return out, nil
}

// Query reads task state or a bounded task page within a read-only transaction.
func (m *Machine) Query(ctx context.Context, data []byte) (load.Result, error) {
	var q task.Query
	if len(data) > maxQueryBytes || jsonwire.Decode(data, &q) != nil {
		return task.Reject("invalid_command"), nil
	}
	tx, err := beginTransaction(ctx, m.db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return load.Result{}, err
	}
	defer tx.Close()
	view, err := readState(tx)
	if err != nil {
		return load.Result{}, err
	}
	var value any
	switch {
	case q.Kind == "state" && q.TaskID == 0 && q.After == 0 && q.Limit == 0:
		value = view
	case q.Kind == "task" && q.TaskID > 0 && q.After == 0 && q.Limit == 0:
		row, err := lookupTask(tx, q.TaskID)
		if err != nil {
			return load.Result{}, err
		}
		if row == nil {
			return task.MissingResult(view.Meta, q.TaskID), nil
		}
		value = row
	case q.Kind == "tasks" && q.TaskID == 0 && q.After >= 0 && q.Limit >= 1 && q.Limit <= maxTaskPageSize:
		rows, err := tasksAfter(tx, q.After, q.Limit+1)
		if err != nil {
			return load.Result{}, err
		}
		page := task.Page{Tasks: rows}
		if len(rows) > q.Limit {
			page.Tasks = rows[:q.Limit]
			page.Next = page.Tasks[len(page.Tasks)-1].TaskID
		}
		value = page
	default:
		return task.Reject("invalid_command"), nil
	}
	body, err := json.Marshal(value)
	return load.Result{Code: "ok", Data: body}, err
}

// Close releases the application's SQLite engine.
func (m *Machine) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.db.Close()
}

func lookupReferencedTasks(tx *xorm.Session, ids []int64) (map[int64]task.Task, error) {
	rows := map[int64]task.Task{}
	for _, id := range ids {
		row, err := lookupTask(tx, id)
		if err != nil {
			return nil, err
		}
		if row != nil {
			rows[id] = *row
		}
	}
	return rows, nil
}
