package metaengine

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

type ProposalKind string

const (
	KindProposeDocument      ProposalKind = "propose_document"
	KindAdmitNode            ProposalKind = "admit_node"
	KindBeginOperator        ProposalKind = "begin_execution"
	KindCommitRepairOperator ProposalKind = "commit_repair_execution"
	KindCommitVoterChange    ProposalKind = "commit_voter_change"
	KindTransitionOperator   ProposalKind = "transition_execution"
	KindActivateTarget       ProposalKind = "activate_target"
	KindBindAdmission        ProposalKind = "bind_admission"
	KindCommitReadmission    ProposalKind = "commit_readmission"
	KindReadyAdmission       ProposalKind = "ready_admission"
	KindGrantSession         ProposalKind = "grant_session"
	KindRevokeSession        ProposalKind = "revoke_session"
	KindAcquireLock          ProposalKind = "acquire_lock"
)

type envelope struct {
	SchemaVersion uint32          `json:"schema_version"`
	Kind          ProposalKind    `json:"kind"`
	Body          json.RawMessage `json:"body"`
}

type ProposeDocument struct {
	Doc             topology.Document `json:"doc"`
	ExpectedVersion uint64            `json:"expected_version"`
	ProposalID      string            `json:"proposal_id"`
}

type AdmitNode struct {
	Node            topology.Node `json:"node"`
	MemberID        uint64        `json:"member_id,omitempty"`
	ReplaceExisting bool          `json:"replace_existing,omitempty"`
	At              time.Time     `json:"at"`
	ProposalID      string        `json:"proposal_id"`
}

type BeginOperatorProposal struct {
	Version    uint64          `json:"version"`
	Plan       topology.Plan   `json:"plan"`
	Actor      string          `json:"actor"`
	Origin     topology.Origin `json:"origin"`
	At         time.Time       `json:"at"`
	ProposalID string          `json:"proposal_id"`
}

type CommitRepairOperatorProposal struct {
	Doc             topology.Document `json:"doc"`
	ExpectedVersion uint64            `json:"expected_version"`
	Plan            topology.Plan     `json:"plan"`
	At              time.Time         `json:"at"`
	ProposalID      string            `json:"proposal_id"`
}

type CommitVoterChangeProposal struct {
	Change          topology.VoterChange `json:"change"`
	Doc             topology.Document    `json:"doc"`
	ExpectedVersion uint64               `json:"expected_version"`
	Plan            topology.Plan        `json:"plan"`
	At              time.Time            `json:"at"`
	ProposalID      string               `json:"proposal_id"`
}

type TransitionOperatorProposal struct {
	Transition topology.OperatorTransition `json:"transition"`
	ProposalID string                      `json:"proposal_id"`
}

type ActivateTargetProposal struct {
	OperatorID topology.OperatorID `json:"execution_id"`
	Version    uint64              `json:"version"`
	Attempt    uint32              `json:"attempt"`
	ProposalID string              `json:"proposal_id"`
}

type BindAdmissionProposal struct {
	Admission       AdmissionRecord `json:"admission"`
	Founding        bool            `json:"founding,omitempty"`
	ReplaceInstance bool            `json:"replace_instance,omitempty"`
	At              time.Time       `json:"at"`
	ProposalID      string          `json:"proposal_id"`
}

type CommitReadmissionProposal struct {
	Admission       AdmissionRecord   `json:"admission"`
	Doc             topology.Document `json:"doc"`
	ExpectedVersion uint64            `json:"expected_version"`
	Plan            topology.Plan     `json:"plan"`
	At              time.Time         `json:"at"`
	ProposalID      string            `json:"proposal_id"`
}

type ReadyAdmissionProposal struct {
	NodeName   string              `json:"node_name"`
	InstanceID string              `json:"instance_id"`
	MemberID   uint64              `json:"member_id"`
	OperatorID topology.OperatorID `json:"execution_id"`
	ProposalID string              `json:"proposal_id"`
}

type GrantSession struct {
	SessionID  string `json:"session_id"`
	NodeName   string `json:"node_name"`
	InstanceID string `json:"instance_id"`
	TTLSeconds int    `json:"ttl_seconds"`
	ProposalID string `json:"proposal_id"`
}

type RevokeSession struct {
	SessionID  string `json:"session_id"`
	Reason     string `json:"reason,omitempty"`
	ProposalID string `json:"proposal_id"`
}

type AcquireLock struct {
	Key        string `json:"key"`
	SessionID  string `json:"session_id"`
	ProposalID string `json:"proposal_id"`
}

func Encode(proposal any) ([]byte, error) {
	var kind ProposalKind
	switch proposal.(type) {
	case ProposeDocument:
		kind = KindProposeDocument
	case AdmitNode:
		kind = KindAdmitNode
	case BeginOperatorProposal:
		kind = KindBeginOperator
	case CommitRepairOperatorProposal:
		kind = KindCommitRepairOperator
	case CommitVoterChangeProposal:
		kind = KindCommitVoterChange
	case TransitionOperatorProposal:
		kind = KindTransitionOperator
	case ActivateTargetProposal:
		kind = KindActivateTarget
	case BindAdmissionProposal:
		kind = KindBindAdmission
	case CommitReadmissionProposal:
		kind = KindCommitReadmission
	case ReadyAdmissionProposal:
		kind = KindReadyAdmission
	case GrantSession:
		kind = KindGrantSession
	case RevokeSession:
		kind = KindRevokeSession
	case AcquireLock:
		kind = KindAcquireLock
	default:
		return nil, fmt.Errorf("meta: cannot encode proposal of type %T", proposal)
	}
	body, err := json.Marshal(proposal)
	if err != nil {
		return nil, fmt.Errorf("meta: marshal %s body: %w", kind, err)
	}
	data, err := json.Marshal(envelope{SchemaVersion: 1, Kind: kind, Body: body})
	if err != nil {
		return nil, fmt.Errorf("meta: marshal envelope: %w", err)
	}
	return data, nil
}

func decodeEnvelope(data []byte) (ProposalKind, json.RawMessage, error) {
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return "", nil, fmt.Errorf("meta: unmarshal envelope: %w", err)
	}
	if e.SchemaVersion != 1 {
		return "", nil, fmt.Errorf("meta: unsupported envelope version %d", e.SchemaVersion)
	}
	if e.Kind == "" || len(e.Body) == 0 {
		return "", nil, fmt.Errorf("meta: missing command kind or body")
	}
	return e.Kind, e.Body, nil
}

// DecodeProposal permits only the versioned meta command set. The transport
// validates caller scope before submitting the decoded command to Raft.
func DecodeProposal(data []byte) (any, error) {
	kind, body, err := decodeEnvelope(data)
	if err != nil {
		return nil, err
	}
	var proposal any
	switch kind {
	case KindProposeDocument:
		proposal = &ProposeDocument{}
	case KindAdmitNode:
		proposal = &AdmitNode{}
	case KindBeginOperator:
		proposal = &BeginOperatorProposal{}
	case KindCommitRepairOperator:
		proposal = &CommitRepairOperatorProposal{}
	case KindCommitVoterChange:
		proposal = &CommitVoterChangeProposal{}
	case KindTransitionOperator:
		proposal = &TransitionOperatorProposal{}
	case KindActivateTarget:
		proposal = &ActivateTargetProposal{}
	case KindBindAdmission:
		proposal = &BindAdmissionProposal{}
	case KindCommitReadmission:
		proposal = &CommitReadmissionProposal{}
	case KindReadyAdmission:
		proposal = &ReadyAdmissionProposal{}
	case KindGrantSession:
		proposal = &GrantSession{}
	case KindRevokeSession:
		proposal = &RevokeSession{}
	case KindAcquireLock:
		proposal = &AcquireLock{}
	default:
		return nil, fmt.Errorf("meta: unknown proposal kind %q", kind)
	}
	if string(body) == "null" {
		return nil, fmt.Errorf("meta: null command body")
	}
	if err = json.Unmarshal(body, proposal); err != nil {
		return nil, err
	}
	return proposal, nil
}
