package topology

import (
	"time"

	"github.com/asalimonov/gquorum/storage"
)

// CleanupProgress reports exact proven milestones without exposing opaque authority tokens.
type CleanupProgress struct {
	Identity    storage.Identity `json:"identity"`
	Storage     StorageClass     `json:"storage"`
	MemberID    uint64           `json:"member_id"`
	OperationID OperationID      `json:"operation_id"`
	RemovedAt   *time.Time       `json:"removed_at,omitempty"`
	StoppedAt   *time.Time       `json:"stopped_at,omitempty"`
	RetiredAt   *time.Time       `json:"retired_at,omitempty"`
	CollectedAt *time.Time       `json:"collected_at,omitempty"`
	NotBefore   *time.Time       `json:"not_before,omitempty"`
	Blocked     *BlockedReason   `json:"blocked,omitempty"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// CleanupSummary bounds local diagnosis while full exact debt is available through progress pages.
type CleanupSummary struct {
	Pending         int        `json:"pending"`
	Unconfirmed     int        `json:"unconfirmed"`
	OldestPendingAt *time.Time `json:"oldest_pending_at,omitempty"`
}

func cloneCleanupProgress(p CleanupProgress) CleanupProgress {
	owned := p
	owned.RemovedAt = cloneProgressTime(p.RemovedAt)
	owned.StoppedAt = cloneProgressTime(p.StoppedAt)
	owned.RetiredAt = cloneProgressTime(p.RetiredAt)
	owned.CollectedAt = cloneProgressTime(p.CollectedAt)
	owned.NotBefore = cloneProgressTime(p.NotBefore)
	owned.Blocked = cloneBlockedReason(p.Blocked)
	return owned
}

func cloneProgressTime(at *time.Time) *time.Time {
	if at == nil {
		return nil
	}
	owned := *at
	return &owned
}
