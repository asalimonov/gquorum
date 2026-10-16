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
	switch kind {
	case "begin_operation":
		var p BeginOperationProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return proposalApplication{}, err
		}
		return state.applyBeginOperation(index, p)
	case "transition_operation":
		var p TransitionOperationProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return proposalApplication{}, err
		}
		return state.applyTransitionOperation(index, p)
	case KindBeginEffect:
		var p BeginEffectProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return proposalApplication{}, err
		}
		return state.applyBeginEffect(index, p)
	case KindGrantTransfer:
		var p GrantTransferProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return proposalApplication{}, err
		}
		return state.applyGrantTransfer(index, p)
	case KindBeginReadmissionAuthority:
		var p BeginReadmissionAuthority
		if err := json.Unmarshal(body, &p); err != nil {
			return proposalApplication{}, err
		}
		return state.applyBeginReadmissionAuthority(index, p)
	default:
		return proposalApplication{}, fault.ErrUnsupported
	}
}

func applyIndexedJSONProposal[P any](index uint64, body json.RawMessage, operation string,
	apply func(uint64, P) (ApplyResult, bool)) (proposalApplication, error) {
	return applyJSONProposal(body, operation, func(p P) (ApplyResult, bool) { return apply(index, p) })
}
