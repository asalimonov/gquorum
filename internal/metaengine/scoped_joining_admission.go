package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) bindJoiningAdmission(p BindAdmissionProposal) (AdmissionRecord, error) {
	record := p.Admission
	record.State = AdmissionPending
	var conflict *AdmissionRecord
	for i := range s.Admissions {
		prior := &s.Admissions[i]
		if prior.NodeName == record.NodeName && !prior.State.terminal() {
			conflict = prior
		}
	}
	record, rejected := admissionInstanceReplacement(p, record, conflict)
	if rejected != nil {
		return AdmissionRecord{}, fault.ErrOperationAuthorization
	}
	if err := s.validateJoiningAdmission(record); err != nil {
		return AdmissionRecord{}, err
	}
	if s.admissionMemberPrepared(record) {
		// Rebinding a pending process cannot reuse a member whose preparation
		// already took effect, even if its cursor acknowledgement is late.
		return AdmissionRecord{}, fault.ErrOperationAuthorization
	}
	return record, nil
}

func (s *scopedState) validateJoiningAdmission(a AdmissionRecord) error {
	op := s.admissionOperation(a.NodeName, a.MemberID)
	if op == nil || op.Status.ID != topology.OperationID(a.OperatorID) || !validAdmissionOrigin(a, s.indexes) {
		return fault.ErrOperationAuthorization
	}
	if _, valid := scopedAdmissionProgress(a, op, s.indexes, s.authorityCatalog()); !valid {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func (s *scopedState) admissionMemberPrepared(a AdmissionRecord) bool {
	if row := s.indexes.groups[topology.MetaGroupID]; row != nil &&
		groupHasMember(row.Declaration, a.NodeName, a.MemberID) {
		return true
	}
	for _, record := range s.authorityCatalog().records {
		if metadataPreparationConsumed(record, a.NodeName, a.MemberID) {
			return true
		}
	}
	return false
}

func (s *scopedState) applyReadyAdmission(index uint64, p ReadyAdmissionProposal) (proposalApplication, error) {
	for _, record := range s.Admissions {
		if record.NodeName != p.NodeName || record.InstanceID != p.InstanceID ||
			record.MemberID != p.MemberID || record.OperatorID != p.OperatorID {
			continue
		}
		if record.State == AdmissionSuperseded || record.State == AdmissionRetired {
			return proposalApplication{}, fault.ErrAuthorityFenced
		}
		if record.State == AdmissionCompleted {
			return proposalApplication{result: admissionResult(p.ProposalID, record)}, nil
		}
		if err := s.validateJoiningAdmission(record); err != nil {
			return proposalApplication{}, err
		}
		if record.State.isPrepared() {
			return proposalApplication{result: admissionResult(p.ProposalID, record)}, nil
		}
		if !scopedAdmissionRetirement(record, s, s.indexes, s.authorityCatalog()) {
			return proposalApplication{}, fault.ErrAuthorityPending
		}
		record.State = AdmissionReady
		return s.installAdmission(index, p.ProposalID, record)
	}
	return proposalApplication{}, fault.ErrOperationAuthorization
}
