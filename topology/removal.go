package topology

// SelfRemovalStatus identifies one durable, approved local membership fence.
// Notified means the asynchronous callback was invoked, not that the caller's
// external teardown completed. Callback errors do not undo the removal.
type SelfRemovalStatus struct {
	Fenced       bool   `json:"fenced"`
	Notified     bool   `json:"notified"`
	MemberID     uint64 `json:"member_id"`
	Version      uint64 `json:"version"`
	AppliedIndex uint64 `json:"applied_index"`
	Reason       string `json:"reason"`
	ErrorCode    string `json:"error_code,omitempty"`
}
