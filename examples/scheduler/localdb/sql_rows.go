package localdb

import (
	"context"
	"database/sql"
	"errors"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"xorm.io/xorm"
)

// Every FSM call owns one session. Closing an uncommitted session rolls back.
func beginTransaction(ctx context.Context, db *xorm.Engine, options *sql.TxOptions) (*xorm.Session, error) {
	session := db.NewSession().Context(ctx)
	if err := session.BeginTx(options); err != nil {
		session.Close()
		return nil, err
	}
	return session, nil
}

func allTasks(tx *xorm.Session) ([]task.Task, error) {
	rows := []task.Task{}
	err := tx.Asc("task_id").Find(&rows)
	return rows, err
}
func tasksAfter(tx *xorm.Session, after int64, limit int) ([]task.Task, error) {
	rows := []task.Task{}
	err := tx.Where("task_id > ?", after).
		Asc("task_id").
		Limit(limit).
		Find(&rows)
	return rows, err
}
func lookupTask(tx *xorm.Session, id int64) (*task.Task, error) {
	var row task.Task
	found, err := tx.ID(id).Get(&row)
	if err != nil || !found {
		return nil, err
	}
	return &row, nil
}

// readState loads metadata and every unfinished task ordered by ID.
func readState(tx *xorm.Session) (task.State, error) {
	var view task.State
	found, err := tx.ID(1).Get(&view.Meta)
	if err != nil {
		return view, err
	}
	if !found {
		return view, errors.New("missing application metadata")
	}
	view.Outstanding = []task.Task{}
	err = tx.In("state", "pending", "assigned", "running").
		Asc("task_id").
		Find(&view.Outstanding)
	return view, err
}

func writeMetadata(tx *xorm.Session, meta task.Metadata) error {
	_, err := tx.ID(1).
		Cols("last_task_id", "succeeded_count", "failed_count").
		Update(&meta)
	return err
}

// insertTasks inserts new tasks and restored snapshot rows.
func insertTasks(tx *xorm.Session, rows []task.Task) error {
	if len(rows) == 0 {
		return nil
	}
	_, err := tx.Insert(&rows)
	return err
}

// updateTasks writes full rows, including NULL owners/results cleared by revocation.
func updateTasks(tx *xorm.Session, rows []task.Task) error {
	for _, row := range rows {
		affected, err := tx.ID(row.TaskID).AllCols().Nullable("executor_replica_id", "executor_node", "started_index",
			"finished_index",
			"result", "error_text").
			Update(&row)
		if err != nil {
			return err
		}
		if affected != 1 {
			return errors.New("task update did not affect one row")
		}
	}
	return nil
}
