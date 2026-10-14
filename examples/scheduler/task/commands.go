package task

import "encoding/json"

// Executor identifies the replica that owns an assignment. Apply checks
// ReplicaID; NodeName is carried for logs and HTTP.
type Executor struct {
	ReplicaID uint64 `json:"replica_id"`
	NodeName  string `json:"node_name"`
}

// Key names one execution attempt.
type Key struct {
	TaskID  int64 `json:"task_id"`
	Attempt int64 `json:"attempt"`
}

// Command is the versioned replicated envelope; Body is one typed body below.
type Command struct {
	Version int             `json:"version"`
	Kind    string          `json:"kind"`
	Body    json.RawMessage `json:"body"`
}
