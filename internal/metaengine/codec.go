package metaengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

const metaEnvelopeSchemaVersion = 3

// ProposalKind identifies a versioned replicated metadata operation.
type ProposalKind string

// Metadata proposal kinds identify authority, topology, admission, session and
// lock operations.
const (
	KindRecordReadmissionReceipts ProposalKind = "record_readmission_receipts"
	KindBeginReadmissionAuthority ProposalKind = "begin_readmission_authority"
	KindGrantTransfer             ProposalKind = "grant_transfer"
	KindBeginEffect               ProposalKind = "begin_effect"
	KindRecordFence               ProposalKind = "record_fence"
	KindRecordEffect              ProposalKind = "record_effect"
	KindProposeDocument           ProposalKind = "propose_document"
	KindAdmitNode                 ProposalKind = "admit_node"
	KindBeginOperator             ProposalKind = "begin_execution"
	KindCommitRepairOperator      ProposalKind = "commit_repair_execution"
	KindCommitVoterChange         ProposalKind = "commit_voter_change"
	KindTransitionOperator        ProposalKind = "transition_execution"
	KindActivateTarget            ProposalKind = "activate_target"
	KindBindAdmission             ProposalKind = "bind_admission"
	KindCommitReadmission         ProposalKind = "commit_readmission"
	KindReadyAdmission            ProposalKind = "ready_admission"
	KindGrantSession              ProposalKind = "grant_session"
	KindRevokeSession             ProposalKind = "revoke_session"
	KindAcquireLock               ProposalKind = "acquire_lock"
)

type envelope struct {
	SchemaVersion uint32          `json:"schema_version"`
	Kind          ProposalKind    `json:"kind"`
	Body          json.RawMessage `json:"body"`
}

// ProposeDocument stages a topology document under expected-version
// compare-and-swap.
type ProposeDocument struct {
	Doc             topology.Document `json:"doc"`
	ExpectedVersion uint64            `json:"expected_version"`
	Intent          *Intent           `json:"intent,omitempty"`
	ProposalID      string            `json:"proposal_id"`
}

// AdmitNode supplies a node declaration and optional replacement identity for
// admission.
type AdmitNode struct {
	Node            topology.Node `json:"node"`
	MemberID        uint64        `json:"member_id,omitempty"`
	ReplaceExisting bool          `json:"replace_existing,omitempty"`
	At              time.Time     `json:"at"`
	ProposalID      string        `json:"proposal_id"`
}

// Encode wraps a supported metadata proposal in the versioned replicated
// envelope.
func Encode(proposal any) ([]byte, error) {
	var kind ProposalKind
	switch proposal.(type) {
	case RecordReadmissionReceiptsProposal:
		kind = KindRecordReadmissionReceipts
	case BeginReadmissionAuthority:
		kind = KindBeginReadmissionAuthority
	case GrantTransferProposal:
		kind = KindGrantTransfer
	case RecordFenceProposal:
		kind = KindRecordFence
	case RecordEffectProposal:
		kind = KindRecordEffect
	case BeginEffectProposal:
		kind = KindBeginEffect
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
		return nil, fault.New(fault.Unsupported, fmt.Sprintf("meta: cannot encode proposal of type %T", proposal), nil)
	}
	body, err := json.Marshal(proposal)
	if err != nil {
		return nil, fmt.Errorf("meta: marshal %s body: %w", kind, err)
	}
	data, err := json.Marshal(envelope{SchemaVersion: metaEnvelopeSchemaVersion, Kind: kind, Body: body})
	if err != nil {
		return nil, fmt.Errorf("meta: marshal envelope: %w", err)
	}
	return data, nil
}

func decodeEnvelope(data []byte) (envelope, error) {
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return envelope{}, fmt.Errorf("meta: unmarshal envelope: %w", err)
	}
	if e.SchemaVersion != metaEnvelopeSchemaVersion {
		return envelope{}, fault.New(fault.Unsupported, fmt.Sprintf("unsupported meta envelope version %d",
			e.SchemaVersion), nil)
	}
	if e.Kind == "" || len(e.Body) == 0 {
		return envelope{}, errors.New("meta: missing command kind or body")
	}
	return e, nil
}

// DecodeProposal permits only the versioned meta command set. The transport
// validates caller scope before submitting the decoded command to Raft.
func DecodeProposal(data []byte) (any, error) {
	decoded, err := decodeEnvelope(data)
	kind, body := decoded.Kind, decoded.Body
	if err != nil {
		return nil, err
	}
	var proposal any
	switch kind {
	case KindRecordReadmissionReceipts:
		proposal = &RecordReadmissionReceiptsProposal{}
	case KindBeginReadmissionAuthority:
		proposal = &BeginReadmissionAuthority{}
	case KindGrantTransfer:
		proposal = &GrantTransferProposal{}
	case KindRecordFence:
		proposal = &RecordFenceProposal{}
	case KindRecordEffect:
		proposal = &RecordEffectProposal{}
	case KindBeginEffect:
		proposal = &BeginEffectProposal{}
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
		return nil, errors.New("meta: null command body")
	}
	if err = json.Unmarshal(body, proposal); err != nil {
		return nil, err
	}
	return proposal, nil
}
