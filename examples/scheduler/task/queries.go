package task

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/load"
)

// Error is a definite replicated refusal, distinct from a timeout whose
// command may still commit.
type Error struct{ Code string }

func (e *Error) Error() string { return fmt.Sprintf("scheduler: %s", e.Code) }

// Query selects a state-machine read; it cannot execute SQL.
type Query struct {
	Kind   string `json:"kind"` // state, task, tasks
	TaskID int64  `json:"task_id,omitempty"`
	After  int64  `json:"after,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// Page carries retained tasks and the exclusive cursor for the next page.
type Page struct {
	Tasks []Task `json:"tasks"`
	Next  int64  `json:"next"`
}

func consensusQuery(ctx context.Context, group *load.Group, q Query, dst any) error {
	body, err := json.Marshal(q)
	if err != nil {
		return err
	}
	out, err := group.Read(ctx, body)
	if err != nil {
		return err
	}
	if out.Code != "ok" {
		return &Error{Code: out.Code}
	}
	return jsonwire.Decode(out.Data, dst)
}

// ReadState returns metadata and unfinished tasks after quorum confirmation.
func ReadState(ctx context.Context, group *load.Group) (State, error) {
	var out State
	err := consensusQuery(ctx, group, Query{Kind: "state"}, &out)
	return out, err
}

// ReadTask returns one retained task, or a replicated not-found/retired result.
func ReadTask(ctx context.Context, group *load.Group, id int64) (Task, error) {
	var out Task
	err := consensusQuery(ctx, group, Query{Kind: "task", TaskID: id}, &out)
	return out, err
}

// ReadTasks returns a bounded page ordered by increasing task ID.
func ReadTasks(ctx context.Context, group *load.Group, after int64, limit int) (Page, error) {
	var out Page
	err := consensusQuery(ctx, group, Query{Kind: "tasks", After: after, Limit: limit}, &out)
	return out, err
}
