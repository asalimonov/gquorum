package task

// Metadata tracks monotonic task IDs and lifetime totals.
type Metadata struct {
	ID             int64 `xorm:"'id' pk" json:"id"`
	LastTaskID     int64 `xorm:"'last_task_id'" json:"last_task_id"`
	SucceededCount int64 `xorm:"'succeeded_count'" json:"succeeded_count"`
	FailedCount    int64 `xorm:"'failed_count'" json:"failed_count"`
}

// Task is one durable job. Attempt and executor replica fence stale results;
// nil owner fields mean pending and nil result fields mean no outcome yet.
type Task struct {
	TaskID            int64   `xorm:"'task_id' pk" json:"task_id"`
	State             string  `xorm:"'state'" json:"state"`
	Seed              string  `xorm:"'seed'" json:"seed"`
	DurationMS        int64   `xorm:"'duration_ms'" json:"duration_ms"`
	Attempt           int64   `xorm:"'attempt'" json:"attempt"`
	ExecutorReplicaID *int64  `xorm:"'executor_replica_id'" json:"executor_replica_id"`
	ExecutorNode      *string `xorm:"'executor_node'" json:"executor_node"`
	CreatedIndex      int64   `xorm:"'created_index'" json:"created_index"`
	UpdatedIndex      int64   `xorm:"'updated_index'" json:"updated_index"`
	StartedIndex      *int64  `xorm:"'started_index'" json:"started_index"`
	FinishedIndex     *int64  `xorm:"'finished_index'" json:"finished_index"`
	Result            *string `xorm:"'result'" json:"result"`
	ErrorText         *string `xorm:"'error_text'" json:"error_text"`
}

// TableName returns the persisted application metadata table name.
func (Metadata) TableName() string { return "app_meta" }

// TableName returns the persisted task table name.
func (Task) TableName() string { return "tasks" }

// State is the bounded scheduler view: metadata plus unfinished tasks ordered
// by ID. Lookup is an Apply-local supplement holding referenced rows,
// including terminal ones; it is never encoded.
type State struct {
	Meta        Metadata       `json:"meta"`
	Outstanding []Task         `json:"outstanding"`
	Lookup      map[int64]Task `json:"-"`
}

// Key returns the task's current attempt key.
func (t Task) Key() Key { return Key{TaskID: t.TaskID, Attempt: t.Attempt} }

// Owner returns the assigned executor, or a zero Executor while pending.
func (t Task) Owner() Executor {
	if t.ExecutorReplicaID == nil || *t.ExecutorReplicaID <= 0 || t.ExecutorNode == nil {
		return Executor{}
	}
	return Executor{ReplicaID: uint64(*t.ExecutorReplicaID), NodeName: *t.ExecutorNode}
}

// Terminal reports whether the task has an accepted outcome.
func Terminal(t Task) bool {
	return t.State == "succeeded" || t.State == "failed"
}

func (t *Task) setOwner(e Executor) error {
	replica, err := CheckedInt(e.ReplicaID)
	if err != nil {
		return err
	}
	node := e.NodeName
	t.ExecutorReplicaID, t.ExecutorNode = &replica, &node
	return nil
}
