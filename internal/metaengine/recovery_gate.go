package metaengine

import (
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
)

func (s *scopedState) recoveryCommandAllowed(kind ProposalKind, body []byte) error {
	if s.Recovery == nil || s.Recovery.Phase != "importing" {
		return nil
	}
	switch kind {
	case KindImportRecovery, KindRecoveryGroupConsumed, KindCompleteRecoveryImport, KindRevokeSession,
		KindRecordFence, KindRecordEffect, KindBeginEffect, KindTransitionOperation, KindGrantTransfer:
		return nil // Existing exact immutable-grant/action/resource authorization still applies.
	case KindRetryOperation:
		var p RetryOperationProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return err
		}
		if op := s.indexes.operations[p.Previous]; op != nil && op.Status.Version == s.Recovery.Version {
			return nil
		}
	default:
		return s.recoveryProcessCommandAllowed(kind, body)
	}
	return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
}

func (s *scopedState) recoveryProcessCommandAllowed(kind ProposalKind, body []byte) error {
	switch kind {
	case KindGrantSession:
		var p GrantSession
		if err := json.Unmarshal(body, &p); err != nil {
			return err
		}
		if p.NodeName == s.Recovery.Survivor.NodeName {
			return nil
		}
	case KindAcquireLock:
		var p AcquireLock
		if err := json.Unmarshal(body, &p); err != nil {
			return err
		}
		if p.Key == "node/"+s.Recovery.Survivor.NodeName {
			return nil
		}
	case KindBindAdmission:
		var p BindAdmissionProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return err
		}
		if p.Founding && p.Admission.NodeName == s.Recovery.Survivor.NodeName &&
			p.Admission.MemberID == s.Recovery.Survivor.ReplicaID {
			return nil
		}
	default:
	}
	return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
}
