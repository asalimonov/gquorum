package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// BeginOperationProposal approves an immutable plan against captured scope owners.
type BeginOperationProposal struct {
	Version           uint64             `json:"version"`
	Plan              topology.Plan      `json:"plan"`
	Scopes            []ScopeExpectation `json:"scopes"`
	Actor             string             `json:"actor"`
	Origin            topology.Origin    `json:"origin"`
	At                time.Time          `json:"at"`
	ProposalID        string             `json:"proposal_id"`
	DeletionSemantics DeletionSemantics  `json:"deletion_semantics,omitempty"`
}

// RetryOperationProposal names a failed attempt; the FSM derives its immutable plan.
type RetryOperationProposal struct {
	Previous   topology.OperationID `json:"previous"`
	Scopes     []ScopeExpectation   `json:"scopes"`
	At         time.Time            `json:"at"`
	ProposalID string               `json:"proposal_id"`
}

// TransitionOperationProposal advances one exact required block step.
type TransitionOperationProposal struct {
	Transition topology.OperationTransition `json:"transition"`
	ProposalID string                       `json:"proposal_id"`
}

// UnmarshalJSON checks payload shape; replicated guards validate plan semantics.
func (p *BeginOperationProposal) UnmarshalJSON(data []byte) error {
	type payload BeginOperationProposal
	var decoded payload
	if err := decodeScopedPayload(data, &decoded, "version", "plan", "scopes", "actor", "origin", "at"); err != nil {
		return err
	}
	if err := validatePlanScopes(decoded.Plan, decoded.Scopes); err != nil {
		return err
	}
	if decoded.DeletionSemantics > DeletionSemanticsDetached {
		return fault.ErrUnsupported
	}
	*p = BeginOperationProposal(decoded)
	return nil
}

// UnmarshalJSON refuses caller-selected plans, authority classes and missing fields.
func (p *RetryOperationProposal) UnmarshalJSON(data []byte) error {
	type payload RetryOperationProposal
	var decoded payload
	if err := decodeScopedPayload(data, &decoded, "previous", "scopes", "at"); err != nil {
		return err
	}
	*p = RetryOperationProposal(decoded)
	return nil
}

// UnmarshalJSON retains the full transition tuple and proposal correlation ID.
func (p *TransitionOperationProposal) UnmarshalJSON(data []byte) error {
	type payload TransitionOperationProposal
	var decoded payload
	if err := decodeScopedPayload(data, &decoded, "transition"); err != nil {
		return err
	}
	*p = TransitionOperationProposal(decoded)
	return nil
}
