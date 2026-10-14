package task

// Start marks an exact task attempt running for its assigned executor.
type Start struct {
	Key      Key      `json:"key"`
	Executor Executor `json:"executor"`
}

// Finish records the result or business failure of an exact executor-owned
// attempt.
type Finish struct {
	Key      Key      `json:"key"`
	Executor Executor `json:"executor"`
	Value    string   `json:"value"` // 64 hex chars on success
	Error    string   `json:"error"` // nonempty on business failure; at most 512 bytes
}
