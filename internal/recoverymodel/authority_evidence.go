package recoverymodel

import (
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// AuthorityFence is the unchanged authority-journal fence encoding.
type AuthorityFence struct {
	Grant    topology.AuthorityGrant `json:"grant"`
	Sequence uint64                  `json:"sequence"`
	Complete bool                    `json:"complete"`
}

// AuthorityEffect is the unchanged authority-journal execution encoding.
type AuthorityEffect struct {
	Grant    topology.AuthorityGrant  `json:"grant"`
	Target   topology.AuthorityTarget `json:"target"`
	Sequence uint64                   `json:"sequence"`
	State    string                   `json:"state"`
	Receipt  *topology.EffectReceipt  `json:"receipt,omitempty"`
}

// AuthorityCollection is the unchanged authority-journal collection encoding.
type AuthorityCollection struct {
	Retired     storage.RetiredReplica  `json:"retired"`
	MemberID    uint64                  `json:"member_id"`
	Grant       topology.AuthorityGrant `json:"grant"`
	Retirement  topology.EffectReceipt  `json:"retirement"`
	Sequence    uint64                  `json:"sequence"`
	State       string                  `json:"state"`
	CollectedAt *time.Time              `json:"collected_at,omitempty"`
}

// Clone owns the nested authority and collection timestamp.
func (r AuthorityCollection) Clone() AuthorityCollection {
	owned := r
	owned.Grant = r.Grant.Clone()
	owned.Retirement = r.Retirement.Clone()
	owned.CollectedAt = clonePointer(r.CollectedAt)
	return owned
}
