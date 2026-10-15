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
