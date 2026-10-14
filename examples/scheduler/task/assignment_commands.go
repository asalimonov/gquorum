package task

// Assignment binds a task's expected attempt to its next executor.
type Assignment struct {
	TaskID          int64    `json:"task_id"`
	ExpectedAttempt int64    `json:"expected_attempt"`
	Executor        Executor `json:"executor"`
}

// Assign requests a batch of guarded task assignments.
type Assign struct {
	Tasks []Assignment `json:"tasks"`
}
