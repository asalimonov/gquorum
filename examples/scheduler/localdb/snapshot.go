package localdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/examples/scheduler/task"
)

// MaxSnapshotBytes bounds the logical application snapshot.
const MaxSnapshotBytes = 8 << 20

type snapshotState struct {
	FormatVersion int           `json:"format_version"`
	ClusterID     string        `json:"cluster_id"`
	GroupID       string        `json:"group_id"`
	Meta          task.Metadata `json:"meta"`
	Tasks         []task.Task   `json:"tasks"`
}

// Snapshot encodes validated metadata and all retained task rows with exact
// application identity.
func (m *Machine) Snapshot(ctx context.Context) ([]byte, error) {
	tx, err := beginTransaction(ctx, m.db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	view, err := readState(tx)
	if err != nil {
		return nil, err
	}
	snapshot := snapshotState{
		FormatVersion: task.SchemaVersion,
		ClusterID:     m.identity.ClusterID,
		GroupID:       m.identity.GroupID,
		Meta:          view.Meta,
	}
	if snapshot.Tasks, err = allTasks(tx); err != nil {
		return nil, err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return nil, err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxSnapshotBytes {
		return nil, errors.New("snapshot exceeds bound")
	}
	return data, nil
}

// Restore replaces all rows atomically. Restore(nil) is the empty state.
func (m *Machine) Restore(ctx context.Context, data []byte) error {
	snapshot := snapshotState{
		FormatVersion: task.SchemaVersion,
		ClusterID:     m.identity.ClusterID,
		GroupID:       m.identity.GroupID,
		Meta:          task.Metadata{ID: 1},
	}
	if len(data) > MaxSnapshotBytes {
		return errors.New("snapshot exceeds bound")
	}
	if len(data) > 0 {
		snapshot = snapshotState{}
		if err := jsonwire.Decode(data, &snapshot); err != nil {
			return err
		}
	}
	if snapshot.ClusterID != m.identity.ClusterID || snapshot.GroupID != m.identity.GroupID {
		return errors.New("foreign snapshot identity")
	}
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}
	tx, err := beginTransaction(ctx, m.db, nil)
	if err != nil {
		return err
	}
	defer tx.Close()
	if _, err := tx.Tx().Tx.ExecContext(ctx, `DELETE FROM tasks; DELETE FROM app_meta;`); err != nil {
		return err
	}
	if _, err := tx.Insert(&snapshot.Meta); err != nil {
		return err
	}
	if err := insertTasks(tx, snapshot.Tasks); err != nil {
		return err
	}
	return tx.Commit()
}

// validateSnapshot checks row shapes and counters before any mutation.
func validateSnapshot(s snapshotState) error {
	invalid := errors.New("invalid scheduler snapshot")
	m := s.Meta
	if s.FormatVersion != task.SchemaVersion || s.ClusterID == "" || s.GroupID == "" || m.ID != 1 || m.LastTaskID < 0 ||
		m.SucceededCount < 0 || m.FailedCount < 0 || m.SucceededCount+m.FailedCount > m.LastTaskID {
		return invalid
	}
	lastID, unfinished, finished := int64(0), int64(0), int64(0)
	for _, t := range s.Tasks {
		if t.TaskID <= lastID || t.TaskID > m.LastTaskID || t.Attempt <= 0 ||
			(t.State == "pending") != (t.ExecutorReplicaID == nil) ||
			(t.ExecutorReplicaID == nil) != (t.ExecutorNode == nil) || !task.CanonicalHash(t.Seed) {
			return invalid
		}
		lastID = t.TaskID
		if !validSnapshotTaskState(t) {
			return invalid
		}
		if task.Terminal(t) {
			finished++
		} else {
			unfinished++
		}
	}
	if finished > task.HistoryLimit || unfinished != m.LastTaskID-m.SucceededCount-m.FailedCount {
		return invalid
	}
	return nil
}

func validSnapshotTaskState(t task.Task) bool {
	switch t.State {
	case "pending", "assigned":
		if t.StartedIndex != nil || t.FinishedIndex != nil || t.Result != nil || t.ErrorText != nil {
			return false
		}
	case "running":
		if t.StartedIndex == nil || t.FinishedIndex != nil || t.Result != nil || t.ErrorText != nil {
			return false
		}
	case "succeeded":
		if t.FinishedIndex == nil || t.Result == nil || t.ErrorText != nil || !task.CanonicalHash(*t.Result) {
			return false
		}
	case "failed":
		if t.FinishedIndex == nil || t.Result != nil || t.ErrorText == nil || *t.ErrorText == "" {
			return false
		}
	default:
		return false
	}
	return true
}
