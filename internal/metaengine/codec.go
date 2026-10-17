package metaengine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

const metaEnvelopeSchemaVersion = 4

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
	KindCommitVoterChange         ProposalKind = "commit_voter_change"
	KindBindAdmission             ProposalKind = "bind_admission"
	KindCommitReadmission         ProposalKind = "commit_readmission"
	KindReadyAdmission            ProposalKind = "ready_admission"
	KindGrantSession              ProposalKind = "grant_session"
	KindRevokeSession             ProposalKind = "revoke_session"
	KindAcquireLock               ProposalKind = "acquire_lock"
	KindBeginOperation            ProposalKind = "begin_operation"
	KindRetryOperation            ProposalKind = "retry_operation"
	KindTransitionOperation       ProposalKind = "transition_operation"
	KindCommitRepairOperation     ProposalKind = "commit_repair_operation"
	KindRecordCollection          ProposalKind = "record_collection"
	KindBeginCleanupEffect        ProposalKind = "begin_cleanup_effect"
	KindRecordCleanupStatus       ProposalKind = "record_cleanup_status"
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

// Encode wraps a supported metadata proposal in the versioned replicated
// envelope.
func Encode(proposal any) ([]byte, error) {
	kind, ok := proposalKind(proposal)
	if !ok {
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

func proposalKind(proposal any) (ProposalKind, bool) {
	switch proposal.(type) {
	case BeginOperationProposal:
		return KindBeginOperation, true
	case RetryOperationProposal:
		return KindRetryOperation, true
	case TransitionOperationProposal:
		return KindTransitionOperation, true
	case CommitRepairOperationProposal:
		return KindCommitRepairOperation, true
	case RecordCollectionProposal:
		return KindRecordCollection, true
	case BeginCleanupEffectProposal:
		return KindBeginCleanupEffect, true
	case RecordCleanupStatusProposal:
		return KindRecordCleanupStatus, true
	case RecordReadmissionReceiptsProposal:
		return KindRecordReadmissionReceipts, true
	case BeginReadmissionAuthority:
		return KindBeginReadmissionAuthority, true
	case GrantTransferProposal:
		return KindGrantTransfer, true
	case RecordFenceProposal:
		return KindRecordFence, true
	case RecordEffectProposal:
		return KindRecordEffect, true
	case BeginEffectProposal:
		return KindBeginEffect, true
	case ProposeDocument:
		return KindProposeDocument, true
	case CommitVoterChangeProposal:
		return KindCommitVoterChange, true
	case BindAdmissionProposal:
		return KindBindAdmission, true
	case CommitReadmissionProposal:
		return KindCommitReadmission, true
	case ReadyAdmissionProposal:
		return KindReadyAdmission, true
	case GrantSession:
		return KindGrantSession, true
	case RevokeSession:
		return KindRevokeSession, true
	case AcquireLock:
		return KindAcquireLock, true
	default:
		return "", false
	}
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
	proposal, err := newProposal(kind)
	if err != nil {
		return nil, err
	}
	if string(body) == "null" {
		return nil, errors.New("meta: null command body")
	}
	if err = json.Unmarshal(body, proposal); err != nil {
		return nil, err
	}
	return proposal, nil
}

func newProposal(kind ProposalKind) (any, error) {
	var proposal any
	switch kind {
	case KindBeginOperation:
		proposal = &BeginOperationProposal{}
	case KindRetryOperation:
		proposal = &RetryOperationProposal{}
	case KindTransitionOperation:
		proposal = &TransitionOperationProposal{}
	case KindCommitRepairOperation:
		proposal = &CommitRepairOperationProposal{}
	case KindRecordCollection:
		proposal = &RecordCollectionProposal{}
	case KindBeginCleanupEffect:
		proposal = &BeginCleanupEffectProposal{}
	case KindRecordCleanupStatus:
		proposal = &RecordCleanupStatusProposal{}
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
	case KindCommitVoterChange:
		proposal = &CommitVoterChangeProposal{}
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
	return proposal, nil
}
