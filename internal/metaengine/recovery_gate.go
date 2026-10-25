package metaengine

import (
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

// RecoveryCommandScope contains authoritative identity and retained retry version.
// Local ingress also uses it before a replicated import exists (Version=0).
type RecoveryCommandScope struct {
	Owner                 topology.Identity
	Version, RetryVersion uint64
}

// ValidateRecoveryCommand is the shared pending-import command predicate.
// General ingress must separately reject all private recovery commands.
func ValidateRecoveryCommand(proposal any, scope RecoveryCommandScope) error {
	switch p := proposal.(type) {
	case *model.ImportProposal, *model.GroupConsumedProposal, *model.CompleteImportProposal,
		*RevokeSession, *RecordFenceProposal, *RecordEffectProposal, *BeginEffectProposal,
		*TransitionOperationProposal:
		return nil // Existing exact courier/grant/resource validation still applies.
	case *RetryOperationProposal:
		if scope.Version != 0 && scope.RetryVersion == scope.Version {
			return nil
		}
	case *GrantSession:
		if p.NodeName == scope.Owner.NodeName {
			return nil
		}
	case *AcquireLock:
		if p.Key == "node/"+scope.Owner.NodeName {
			return nil
		}
	case *BindAdmissionProposal:
		if p.Founding && p.Admission.NodeName == scope.Owner.NodeName &&
			p.Admission.MemberID == scope.Owner.MemberID {
			return nil
		}
	default:
	}
	return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
}

func (s *scopedState) recoveryCommandAllowed(kind ProposalKind, body []byte) error {
	if s.Recovery == nil || s.Recovery.Phase != "importing" {
		return nil
	}
	proposal, err := newProposal(kind)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, proposal); err != nil {
		return err
	}
	owner := s.Recovery.Survivor
	scope := RecoveryCommandScope{Owner: topology.Identity{NodeName: owner.NodeName,
		ClusterID: owner.ClusterID, MemberID: owner.ReplicaID}, Version: s.Recovery.Version}
	if retry, ok := proposal.(*RetryOperationProposal); ok {
		if operation := s.indexes.operations[retry.Previous]; operation != nil {
			scope.RetryVersion = operation.Status.Version
		}
	}
	return ValidateRecoveryCommand(proposal, scope)
}
