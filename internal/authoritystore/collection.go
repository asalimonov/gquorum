package authoritystore

import (
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	collectionPrepared  = "prepared"
	collectionCollected = "collected"
)

// CollectionRecord pins exact retirement proof before collection and retains it
// until reporting and store-local proof release have both succeeded.
type CollectionRecord struct {
	Retired     storage.RetiredReplica  `json:"retired"`
	MemberID    uint64                  `json:"member_id"`
	Grant       topology.AuthorityGrant `json:"grant"`
	Retirement  topology.EffectReceipt  `json:"retirement"`
	Sequence    uint64                  `json:"sequence"`
	State       string                  `json:"state"`
	CollectedAt *time.Time              `json:"collected_at,omitempty"`
}

// CollectionReceipt proves a host recorded completion for one exact retirement.
// Authentication and retained grant validation are separate acceptance checks.
type CollectionReceipt struct {
	Identity      storage.Identity      `json:"identity"`
	MemberID      uint64                `json:"member_id"`
	RetirementKey topology.AuthorityKey `json:"retirement_key"`
	Token         string                `json:"token"`
	LocalSequence uint64                `json:"local_sequence"`
	CollectedAt   time.Time             `json:"collected_at"`
}

// CollectionResult is either retained acceptance or a safe-read absence result.
// Settled never asserts that the receipt was historically accepted.
type CollectionResult struct {
	Receipt      CollectionReceipt `json:"receipt"`
	AppliedIndex uint64            `json:"applied_index"`
	Settled      bool              `json:"settled"`
}

// Clone owns the nested grant, receipt action and collection timestamp.
func (r CollectionRecord) Clone() CollectionRecord {
	owned := r
	owned.Grant = r.Grant.Clone()
	owned.Retirement = r.Retirement.Clone()
	if r.CollectedAt != nil {
		at := *r.CollectedAt
		owned.CollectedAt = &at
	}
	return owned
}
