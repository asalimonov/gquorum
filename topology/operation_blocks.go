package topology

// PlanBlock partitions one group's immutable step sequences into execution and cleanup.
type PlanBlock struct {
	GroupID       string `json:"group_id"`
	SourceVersion uint64 `json:"source_version"`
	Steps         []int  `json:"steps"`
	Cleanup       []int  `json:"cleanup,omitempty"`
}

// StepDependency binds an admission or removal barrier to immutable step sequences.
type StepDependency struct {
	StepSeq  int   `json:"step_seq"`
	Requires []int `json:"requires"`
}
