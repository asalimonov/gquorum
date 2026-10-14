package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// BeginOperatorProposal requests approval and execution of a version's
// immutable plan.
type BeginOperatorProposal struct {
	Version    uint64          `json:"version"`
	Plan       topology.Plan   `json:"plan"`
	Actor      string          `json:"actor"`
	Origin     topology.Origin `json:"origin"`
	At         time.Time       `json:"at"`
	ProposalID string          `json:"proposal_id"`
}

// TransitionOperatorProposal advances an exact operator step's replicated
// execution state.
type TransitionOperatorProposal struct {
	Transition topology.OperatorTransition `json:"transition"`
	ProposalID string                      `json:"proposal_id"`
}

// ActivateTargetProposal requests convergence of the exact approved operator
// attempt.
type ActivateTargetProposal struct {
	OperatorID topology.OperatorID `json:"execution_id"`
	Version    uint64              `json:"version"`
	Attempt    uint32              `json:"attempt"`
	ProposalID string              `json:"proposal_id"`
}
