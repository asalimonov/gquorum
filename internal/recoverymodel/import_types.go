//revive:disable:max-public-structs These records describe one replicated recovery import.
package recoverymodel

import (
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ImportState defines private replicated recovery evidence.
type ImportState struct {
	Schema      uint32            `json:"schema"` // 1
	RecoveryID  string            `json:"recovery_id"`
	PlanDigest  string            `json:"plan_digest"`
	Source      topology.Identity `json:"source"`
	Survivor    storage.Identity  `json:"survivor"` // B's exact _meta identity
	Version     uint64            `json:"version"`  // recovery topology version, initially 2
	Phase       string            `json:"phase"`    // importing or complete
	Groups      []ImportGroup     `json:"groups"`
	Policies    []PolicyState     `json:"policies"`
	AcceptedAt  time.Time         `json:"accepted_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
}

// ImportGroup defines private replicated recovery evidence.
type ImportGroup struct {
	GroupID     string           `json:"group_id"`
	Source      SeedSource       `json:"source"`
	Destination storage.Identity `json:"destination"`
	Consumed    *Consumed        `json:"consumed,omitempty"`
}

// PolicyState defines private replicated recovery evidence.
type PolicyState struct {
	GroupID        string `json:"group_id"` // includes _meta
	PolicyDigest   string `json:"policy_digest"`
	PolicyReleased bool   `json:"policy_released"`
}

// Consumed defines private replicated recovery evidence.
type Consumed struct {
	Identity       storage.Identity       `json:"identity"`
	Checkpoint     Position               `json:"checkpoint"`
	SnapshotDigest string                 `json:"snapshot_digest"`
	HostReceipt    topology.EffectReceipt `json:"host_receipt"`
	GroupReceipt   topology.EffectReceipt `json:"group_receipt"`
}

// ImportProposal imports approved source definitions atomically.
type ImportProposal struct {
	Schema          uint32            `json:"schema"`
	RecoveryID      string            `json:"recovery_id"`
	PlanDigest      string            `json:"plan_digest"`
	ExpectedVersion uint64            `json:"expected_version"` // exactly 1
	Source          topology.Identity `json:"source"`
	Survivor        storage.Identity  `json:"survivor"`
	ApprovedSource  topology.Document `json:"approved_source"`
	Sources         []SeedSource      `json:"sources"`
	Floors          []AllocationFloor `json:"floors"`
	Retired         []string          `json:"retired"`
	Actor           string            `json:"actor"`
	Comment         string            `json:"comment"`
	At              time.Time         `json:"at"`
	ProposalID      string            `json:"proposal_id"`
}

// GroupConsumedProposal defines private replicated recovery evidence.
type GroupConsumedProposal struct {
	RecoveryID string   `json:"recovery_id"`
	PlanDigest string   `json:"plan_digest"`
	GroupID    string   `json:"group_id"`
	Evidence   Consumed `json:"evidence"`
	ProposalID string   `json:"proposal_id"`
}

// CompleteImportProposal defines private replicated recovery evidence.
type CompleteImportProposal struct {
	RecoveryID string    `json:"recovery_id"`
	PlanDigest string    `json:"plan_digest"`
	At         time.Time `json:"at"`
	ProposalID string    `json:"proposal_id"`
}
