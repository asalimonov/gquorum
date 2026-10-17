package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyBindAdmission(index uint64, p BindAdmissionProposal) (proposalApplication, error) {
	if err := validateAdmissionBinding(p); err != nil {
		return proposalApplication{}, err
	}
	position := slices.IndexFunc(s.Admissions, func(a AdmissionRecord) bool {
		return a.NodeName == p.Admission.NodeName && a.InstanceID == p.Admission.InstanceID
	})
	if position >= 0 {
		existing := &s.Admissions[position]
		if existing.MemberID != p.Admission.MemberID || existing.OperationID != p.Admission.OperationID {
			return proposalApplication{}, fault.ErrOperationAuthorization
		}
		return proposalApplication{result: admissionResult(p.ProposalID, *existing)}, nil
	}
	record, err := s.boundAdmission(p)
	if err != nil {
		return proposalApplication{}, err
	}
	return s.installAdmission(index, p.ProposalID, record)
}

func (s *scopedState) boundAdmission(p BindAdmissionProposal) (AdmissionRecord, error) {
	if !p.Founding {
		return s.bindJoiningAdmission(p)
	}
	identity := topology.Identity{ClusterID: s.Target.ClusterID, NodeName: p.Admission.NodeName,
		MemberID: p.Admission.MemberID, InstanceID: p.Admission.InstanceID}
	founding, ready := s.admissionBasis(identity)
	if !ready || founding == "" || founding != topology.OperationID(p.Admission.OperationID) {
		return AdmissionRecord{}, fault.ErrOperationAuthorization
	}
	for _, prior := range s.Admissions {
		if prior.NodeName == identity.NodeName && prior.MemberID == identity.MemberID {
			// Founding consumes the seed identity once. A retained restart uses
			// its durable pin/session path; a replacement requires a fresh ID.
			return AdmissionRecord{}, fault.ErrOperationAuthorization
		}
	}
	record := p.Admission
	record.State = AdmissionCompleted
	return record, nil
}

func (s *scopedState) installAdmission(index uint64, id string, record AdmissionRecord) (proposalApplication, error) {
	next := *s
	next.Admissions = slices.Clone(s.Admissions)
	position := -1
	for i, prior := range next.Admissions {
		if prior.NodeName != record.NodeName {
			continue
		}
		if prior.InstanceID == record.InstanceID {
			position = i
		} else if !prior.State.terminal() {
			next.Admissions[i].State = AdmissionSuperseded
		}
	}
	if position >= 0 {
		next.Admissions[position] = record
	} else {
		next.Admissions = append(next.Admissions, record)
	}
	next.AppliedIndex = index
	var err error
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = next
	return proposalApplication{changed: true, result: admissionResult(id, record)}, nil
}

func validateAdmissionBinding(p BindAdmissionProposal) error {
	if err := validateAdmissionIdentity(p.Admission); err != nil {
		return fault.New(fault.Invalid, "invalid admission identity", err)
	}
	if p.Admission.ReadmissionInstanceID != "" || p.Admission.ReadmissionOperatorID != "" || p.At.IsZero() {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func admissionResult(id string, record AdmissionRecord) ApplyResult {
	return ApplyResult{ProposalID: id, Accepted: true, Admission: cloneAdmission(record)}
}
