package metaengine

import (
	"encoding/json"
	"fmt"

	"github.com/asalimonov/gquorum/fault"
)

type proposalApplication struct {
	result  ApplyResult
	changed bool
}

func applyScopedProposal(state *scopedState, index uint64, kind ProposalKind,
	body json.RawMessage) (proposalApplication, error) {
	if state == nil {
		err := fault.New(fault.Unsupported, fmt.Sprintf("meta: unavailable proposal %q", kind), nil)
		return proposalApplication{}, err
	}
	next := *state
	result, err := dispatchScopedProposal(&next, index, kind, body)
	if err != nil || !result.changed {
		return result, err
	}
	if len(next.History) > historyRetention {
		if err := next.pruneRetainedProofs(); err != nil {
			return proposalApplication{}, err
		}
	}
	*state = next
	return result, nil
}

func dispatchScopedProposal(state *scopedState, index uint64, kind ProposalKind,
	body json.RawMessage) (proposalApplication, error) {
	if result, err := dispatchScopedEvidence(state, index, kind, body); result != nil || err != nil {
		if result == nil {
			return proposalApplication{}, err
		}
		return *result, err
	}
	switch kind {
	case "record_collection":
		return applyScopedJSONProposal(index, body, state.applyRecordCollection)
	case KindProposeDocument:
		return applyScopedJSONProposal(index, body, state.applyProposeDocument)
	case KindBindAdmission:
		return applyScopedJSONProposal(index, body, state.applyBindAdmission)
	case KindCommitReadmission:
		return applyScopedJSONProposal(index, body, state.applyCommitReadmission)
	case KindReadyAdmission:
		return applyScopedJSONProposal(index, body, state.applyReadyAdmission)
	case "begin_cleanup_effect":
		return applyScopedJSONProposal(index, body, state.applyBeginCleanupEffect)
	case "record_cleanup_status":
		return applyScopedJSONProposal(index, body, state.applyRecordCleanupStatus)
	case KindCommitVoterChange:
		return applyScopedJSONProposal(index, body, state.applyCommitVoterChange)
	case "commit_repair_operation":
		return applyScopedJSONProposal(index, body, state.applyCommitRepairOperation)
	case "retry_operation":
		return applyScopedJSONProposal(index, body, state.applyRetryOperation)
	case "begin_operation":
		return applyScopedJSONProposal(index, body, state.applyBeginOperation)
	case "transition_operation":
		return applyScopedJSONProposal(index, body, state.applyTransitionOperation)
	case KindBeginEffect:
		return applyScopedJSONProposal(index, body, state.applyBeginEffect)
	case KindGrantTransfer:
		return applyScopedJSONProposal(index, body, state.applyGrantTransfer)
	case KindBeginReadmissionAuthority:
		return applyScopedJSONProposal(index, body, state.applyBeginReadmissionAuthority)
	case KindGrantSession:
		return applyScopedJSONProposal(index, body, state.applyScopedGrantSession)
	case KindRevokeSession:
		return applyScopedJSONProposal(index, body, state.applyScopedRevokeSession)
	case KindAcquireLock:
		return applyScopedJSONProposal(index, body, state.applyScopedAcquireLock)
	default:
		// A committed command this binary cannot interpret is not a business
		// refusal: another replica may apply it. Halt before advancing the index.
		return proposalApplication{}, fmt.Errorf("meta: unknown scoped proposal %q", kind)
	}
}

func dispatchScopedEvidence(state *scopedState, index uint64, kind ProposalKind,
	body json.RawMessage) (*proposalApplication, error) {
	switch kind {
	case KindRecordFence:
		var p RecordFenceProposal
		if err := decodeScopedPayload(body, &p, "evidence"); err != nil {
			return nil, err
		}
		result, err := state.applyRecordFence(index, p)
		return &result, err
	case KindRecordEffect:
		var p RecordEffectProposal
		if err := decodeScopedPayload(body, &p, "receipt", "at"); err != nil {
			return nil, err
		}
		result, err := state.applyRecordEffect(index, p)
		return &result, err
	case KindRecordReadmissionReceipts:
		var p RecordReadmissionReceiptsProposal
		if err := decodeScopedPayload(body, &p, "admission", "receipts", "at"); err != nil {
			return nil, err
		}
		result, err := state.applyRecordReadmissionReceipts(index, p)
		return &result, err
	default:
		return nil, nil
	}
}

func applyScopedJSONProposal[P any](index uint64, body json.RawMessage,
	apply func(uint64, P) (proposalApplication, error)) (proposalApplication, error) {
	var p P
	if err := json.Unmarshal(body, &p); err != nil {
		return proposalApplication{}, err
	}
	return apply(index, p)
}
