package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// RecordCollectionProposal accepts retained proof from an authenticated courier.
// The service binds Courier to the transport identity; it is not caller authority.
type RecordCollectionProposal struct {
	Receipt        authoritystore.CollectionReceipt `json:"receipt"`
	Courier        topology.Identity                `json:"courier"`
	ReadmissionKey *topology.AuthorityKey           `json:"readmission_key,omitempty"`
	At             time.Time                        `json:"at"`
	ProposalID     string                           `json:"proposal_id"`
}

// BeginCleanupEffectProposal selects existing debt, never fresh retirement intent.
type BeginCleanupEffectProposal struct {
	Identity   storage.Identity `json:"identity"`
	StepSeq    int              `json:"step_seq"`
	ProposalID string           `json:"proposal_id"`
}

// RecordCleanupStatusProposal diagnoses deferral without changing proof milestones.
type RecordCleanupStatusProposal struct {
	Identity      storage.Identity        `json:"identity"`
	MemberID      uint64                  `json:"member_id"`
	RetirementKey topology.AuthorityKey   `json:"retirement_key"`
	NotBefore     *time.Time              `json:"not_before,omitempty"`
	Blocked       *topology.BlockedReason `json:"blocked,omitempty"`
	At            time.Time               `json:"at"`
	ProposalID    string                  `json:"proposal_id"`
}

// UnmarshalJSON refuses unsupported fields and preserves exact courier linkage.
func (p *RecordCollectionProposal) UnmarshalJSON(data []byte) error {
	type payload RecordCollectionProposal
	var decoded payload
	if err := decodeScopedPayload(data, &decoded, "receipt", "courier", "at"); err != nil {
		return err
	}
	*p = RecordCollectionProposal(decoded)
	return nil
}

// UnmarshalJSON refuses client-selected authority classes.
func (p *BeginCleanupEffectProposal) UnmarshalJSON(data []byte) error {
	type payload BeginCleanupEffectProposal
	var decoded payload
	if err := decodeScopedPayload(data, &decoded, "identity", "step_seq"); err != nil {
		return err
	}
	*p = BeginCleanupEffectProposal(decoded)
	return nil
}

// UnmarshalJSON requires the host/proof tuple even when clearing a diagnostic.
func (p *RecordCleanupStatusProposal) UnmarshalJSON(data []byte) error {
	type payload RecordCleanupStatusProposal
	var decoded payload
	if err := decodeScopedPayload(data, &decoded, "identity", "member_id", "retirement_key", "at"); err != nil {
		return err
	}
	*p = RecordCleanupStatusProposal(decoded)
	return nil
}
