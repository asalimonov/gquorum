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
	Plan            topology.Plan        `json:"plan"`
	At              time.Time            `json:"at"`
	ProposalID      string               `json:"proposal_id"`
}
