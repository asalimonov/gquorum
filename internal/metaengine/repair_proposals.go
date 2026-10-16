package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// CommitRepairOperatorProposal stages and approves a repair plan under
// replicated compare-and-swap.
type CommitRepairOperatorProposal struct {
	Doc             topology.Document `json:"doc"`
	ExpectedVersion uint64            `json:"expected_version"`
	Plan            topology.Plan     `json:"plan"`
	At              time.Time         `json:"at"`
	ProposalID      string            `json:"proposal_id"`
}

// CommitVoterChangeProposal commits an exact voter change with its guarded
// document and plan.
type CommitVoterChangeProposal struct {
	Change          topology.VoterChange `json:"change"`
	Doc             topology.Document    `json:"doc"`
	ExpectedVersion uint64               `json:"expected_version"`
	ExpectedTarget  uint64               `json:"expected_target,omitempty"`
	Scopes          []ScopeExpectation   `json:"scopes,omitempty"`
	Plan            topology.Plan        `json:"plan"`
	At              time.Time            `json:"at"`
	ProposalID      string               `json:"proposal_id"`
}

// CommitRepairOperationProposal atomically approves repair and rebases staged intent.
// The FSM computes the rebase; the caller supplies only the approved repair candidate.
type CommitRepairOperationProposal struct {
	Doc             topology.Document  `json:"doc"`
	ExpectedVersion uint64             `json:"expected_version"`
	ExpectedTarget  uint64             `json:"expected_target"`
	Scopes          []ScopeExpectation `json:"scopes"`
	Plan            topology.Plan      `json:"plan"`
	At              time.Time          `json:"at"`
	ProposalID      string             `json:"proposal_id"`
}

// UnmarshalJSON preserves both CAS boundaries and refuses caller-selected rebase state.
func (p *CommitRepairOperationProposal) UnmarshalJSON(data []byte) error {
	type payload CommitRepairOperationProposal
	var decoded payload
	err := decodeScopedPayload(data, &decoded, "doc", "expected_version", "expected_target", "scopes", "plan", "at")
	if err != nil {
		return err
	}
	if err := validatePlanScopes(decoded.Plan, decoded.Scopes); err != nil {
		return err
	}
	*p = CommitRepairOperationProposal(decoded)
	return nil
}
