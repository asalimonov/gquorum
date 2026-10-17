package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// cleanupRecord is exact retained debt. Storage is immutable; memory retirement
// finishes its debt without manufacturing a durable collection timestamp.
type cleanupRecord struct {
	Identity       storage.Identity                  `json:"identity"`
	Storage        topology.StorageClass             `json:"storage"`
	MemberID       uint64                            `json:"member_id"`
	OperationID    topology.OperationID              `json:"operation_id"`
	StopAction     *topology.AuthorizedStep          `json:"stop_action,omitempty"`
	RetireAction   *topology.AuthorizedStep          `json:"retire_action,omitempty"`
	ReadmissionKey *topology.AuthorityKey            `json:"readmission_key,omitempty"`
	Removed        *topology.EffectReceipt           `json:"removed,omitempty"`
	Stopped        *topology.EffectReceipt           `json:"stopped,omitempty"`
	Retired        *topology.EffectReceipt           `json:"retired,omitempty"`
	Collection     *authoritystore.CollectionReceipt `json:"collection,omitempty"`
	RemovedAt      *time.Time                        `json:"removed_at,omitempty"`
	StoppedAt      *time.Time                        `json:"stopped_at,omitempty"`
	RetiredAt      *time.Time                        `json:"retired_at,omitempty"`
	NotBefore      *time.Time                        `json:"not_before,omitempty"`
	Blocked        *topology.BlockedReason           `json:"blocked,omitempty"`
	CreatedAt      time.Time                         `json:"created_at"`
	UpdatedAt      time.Time                         `json:"updated_at"`
}

func (r cleanupRecord) clone() cleanupRecord {
	owned := r
	owned.StopAction, owned.RetireAction = copyCleanupValue(r.StopAction), copyCleanupValue(r.RetireAction)
	owned.ReadmissionKey, owned.Collection = copyCleanupValue(r.ReadmissionKey), copyCleanupValue(r.Collection)
	owned.Removed, owned.Stopped, owned.Retired = cloneCleanupReceipt(r.Removed),
		cloneCleanupReceipt(r.Stopped), cloneCleanupReceipt(r.Retired)
	owned.RemovedAt, owned.StoppedAt, owned.RetiredAt = copyCleanupValue(r.RemovedAt),
		copyCleanupValue(r.StoppedAt), copyCleanupValue(r.RetiredAt)
	owned.NotBefore, owned.Blocked = copyCleanupValue(r.NotBefore), copyCleanupValue(r.Blocked)
	return owned
}

func copyCleanupValue[T any](value *T) *T {
	if value == nil {
		return nil
	}
	owned := *value
	return &owned
}

func cloneCleanupReceipt(receipt *topology.EffectReceipt) *topology.EffectReceipt {
	if receipt == nil {
		return nil
	}
	owned := receipt.Clone()
	return &owned
}
