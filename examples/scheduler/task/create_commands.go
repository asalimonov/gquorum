package task

// NewTask supplies the seed, duration and initial executor for a task.
type NewTask struct {
	Seed       string   `json:"seed"` // 32 random bytes as 64 lowercase hex chars
	DurationMS int64    `json:"duration_ms"`
	Executor   Executor `json:"executor"`
}

// Create requests a batch of new tasks after the expected last task ID.
type Create struct {
	ExpectedLastTaskID int64     `json:"expected_last_task_id"`
	Tasks              []NewTask `json:"tasks"`
}
