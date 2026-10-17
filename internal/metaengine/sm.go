package metaengine

import (
	"errors"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/topology"
)

var errDurablePersist = errors.New("meta: durable state persist failed")

type statePersister interface {
	SaveState(data []byte) error
}

// ApplyResult carries the deterministic acceptance, refusal and evidence from a
// metadata proposal.
type ApplyResult struct {
	Collection      *authoritystore.CollectionResult `json:"collection,omitempty"`
	ExpectedVersion uint64                           `json:"expected_version,omitempty"`
	RebasedTo       uint64                           `json:"rebased_to,omitempty"`
	Grants          []topology.AuthorityGrant        `json:"grants,omitempty"`
	ProposalID      string
	CurrentVersion  uint64
	Accepted        bool
	Reason          string
	Code            proposalErrorCode
	Dropped         bool
	Doc             topology.Document
	OperationID     topology.OperationID     `json:"operation_id,omitempty"`
	Grant           *topology.AuthorityGrant `json:"grant,omitempty"`
	Admission       AdmissionRecord
	Placement       *topology.PlacementReport `json:"placement,omitempty"`
}

const errorFormat = "%v"

type proposalErrorCode string

const (
	proposalVersionConflict  proposalErrorCode = "version_conflict"
	proposalAuthorization    proposalErrorCode = "authorization"
	proposalGuardrail        proposalErrorCode = "guardrail"
	proposalAuthorityPending proposalErrorCode = "authority_pending"
	proposalAuthorityFenced  proposalErrorCode = "authority_fenced"
)

const historyRetention = 128
