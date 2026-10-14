package task

// CommandResult is the JSON payload of a command's load.Result.
type CommandResult struct {
	TaskIDs []int64 `json:"task_ids,omitempty"`
	Task    *Task   `json:"task,omitempty"`
}
