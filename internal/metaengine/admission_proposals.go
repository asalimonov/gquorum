package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// BindAdmissionProposal binds a process admission to its exact member and
// startup path.
type BindAdmissionProposal struct {
	Admission       AdmissionRecord `json:"admission"`
	Founding        bool            `json:"founding,omitempty"`
	ReplaceInstance bool            `json:"replace_instance,omitempty"`
	At              time.Time       `json:"at"`
	ProposalID      string          `json:"proposal_id"`
}

// CommitReadmissionProposal stages replacement identities and their exact
// admission plan.
type CommitReadmissionProposal struct {
	Admission       AdmissionRecord    `json:"admission"`
	Doc             topology.Document  `json:"doc"`
	ExpectedVersion uint64             `json:"expected_version"`
	ExpectedTarget  uint64             `json:"expected_target"`
	Scopes          []ScopeExpectation `json:"scopes"`
	Plan            topology.Plan      `json:"plan"`
	At              time.Time          `json:"at"`
	ProposalID      string             `json:"proposal_id"`
}

// ReadyAdmissionProposal acknowledges preparation of an exact admission
// identity.
type ReadyAdmissionProposal struct {
	NodeName    string               `json:"node_name"`
	InstanceID  string               `json:"instance_id"`
	MemberID    uint64               `json:"member_id"`
	OperationID topology.OperationID `json:"execution_id"`
	ProposalID  string               `json:"proposal_id"`
}
