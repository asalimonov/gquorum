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

func applyJSONProposal[P any](body json.RawMessage, operation string,
	apply func(P) (ApplyResult, bool)) (proposalApplication, error) {
	var p P
	if err := json.Unmarshal(body, &p); err != nil {
		if operation != "" {
			return proposalApplication{}, fmt.Errorf("meta: unmarshal %s: %w", operation, err)
		}
		return proposalApplication{}, err
	}
	result, changed := apply(p)
	return proposalApplication{result: result, changed: changed}, nil
}

func (sm *topologySM) applyProposal(index uint64, kind ProposalKind,
	body json.RawMessage) (proposalApplication, error) {
	switch kind {
	case KindRecordReadmissionReceipts:
		return applyJSONProposal(body, "", sm.applyRecordReadmissionReceipts)
	case KindBeginReadmissionAuthority:
		return applyIndexedJSONProposal(index, body, "", sm.applyBeginReadmissionAuthority)
	case KindGrantTransfer:
		return applyIndexedJSONProposal(index, body, "", sm.applyGrantTransfer)
	case KindRecordFence:
		return applyJSONProposal(body, "", sm.applyRecordFence)
	case KindRecordEffect:
		return applyJSONProposal(body, "", sm.applyRecordEffect)
	case KindBeginEffect:
		return applyIndexedJSONProposal(index, body, "", sm.applyBeginEffect)
	case KindProposeDocument:
		return applyJSONProposal(body, "propose_document", sm.applyProposeDocument)
	case KindAdmitNode:
		return applyJSONProposal(body, "admit_node", sm.applyAdmitNode)
	case KindBeginOperator:
		return applyIndexedJSONProposal(index, body, "begin_execution", sm.applyBeginOperator)
	case KindCommitRepairOperator:
		return applyIndexedJSONProposal(index, body, "commit_repair_execution", sm.applyCommitRepairOperator)
	case KindTransitionOperator:
		return applyIndexedJSONProposal(index, body, "transition_execution", sm.applyTransitionOperator)
	case KindCommitVoterChange:
		return applyIndexedJSONProposal(index, body, "commit_voter_change", sm.applyCommitVoterChange)
	case KindActivateTarget:
		return applyJSONProposal(body, "activate_target", sm.applyActivateTarget)
	case KindBindAdmission:
		return applyJSONProposal(body, "bind_admission", sm.applyBindAdmission)
	case KindCommitReadmission:
		return applyIndexedJSONProposal(index, body, "commit_readmission", sm.applyCommitReadmission)
	case KindReadyAdmission:
		return applyJSONProposal(body, "ready_admission", sm.applyReadyAdmission)
	case KindGrantSession:
		return applyJSONProposal(body, "grant_session", sm.applyGrantSession)
	case KindRevokeSession:
		return applyJSONProposal(body, "revoke_session", sm.applyRevokeSession)
	case KindAcquireLock:
		return applyJSONProposal(body, "acquire_lock", sm.applyAcquireLock)
	default:
		// Schema 3 cannot persist scoped mutations. Remove this nil adapter at
		// Task 78, when the complete scoped state replaces this dispatcher.
		return applyScopedProposal(nil, index, kind, body)
	}
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
	switch kind {
	case "record_collection":
		return applyScopedJSONProposal(index, body, state.applyRecordCollection)
	case KindProposeDocument:
		return applyScopedJSONProposal(index, body, state.applyProposeDocument)
	case KindBindAdmission:
		return applyScopedJSONProposal(index, body, state.applyBindAdmission)
	case KindReadyAdmission:
		return applyScopedJSONProposal(index, body, state.applyReadyAdmission)
	case "begin_cleanup_effect":
		return applyScopedJSONProposal(index, body, state.applyBeginCleanupEffect)
	case "record_cleanup_status":
		return applyScopedJSONProposal(index, body, state.applyRecordCleanupStatus)
	case KindRecordFence:
		var p RecordFenceProposal
		if err := decodeScopedPayload(body, &p, "evidence"); err != nil {
			return proposalApplication{}, err
		}
		return state.applyRecordFence(index, p)
	case KindRecordEffect:
		var p RecordEffectProposal
		if err := decodeScopedPayload(body, &p, "receipt", "at"); err != nil {
			return proposalApplication{}, err
		}
		return state.applyRecordEffect(index, p)
	case KindRecordReadmissionReceipts:
		var p RecordReadmissionReceiptsProposal
		if err := decodeScopedPayload(body, &p, "admission", "receipts", "at"); err != nil {
			return proposalApplication{}, err
		}
		return state.applyRecordReadmissionReceipts(index, p)
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
	default:
		return proposalApplication{}, fault.ErrUnsupported
	}
}

func applyIndexedJSONProposal[P any](index uint64, body json.RawMessage, operation string,
	apply func(uint64, P) (ApplyResult, bool)) (proposalApplication, error) {
	return applyJSONProposal(body, operation, func(p P) (ApplyResult, bool) { return apply(index, p) })
}

func applyScopedJSONProposal[P any](index uint64, body json.RawMessage,
	apply func(uint64, P) (proposalApplication, error)) (proposalApplication, error) {
	var p P
	if err := json.Unmarshal(body, &p); err != nil {
		return proposalApplication{}, err
	}
	return apply(index, p)
}
