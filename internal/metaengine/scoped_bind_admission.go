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
		if existing.MemberID != p.Admission.MemberID || existing.OperatorID != p.Admission.OperatorID {
			return proposalApplication{}, fault.ErrOperationAuthorization
		}
		return proposalApplication{result: admissionResult(p.ProposalID, *existing)}, nil
	}
	if !p.Founding {
		return proposalApplication{}, fault.ErrUnsupported // Task 51: exact non-founding block/prefix binding.
	}
	identity := topology.Identity{ClusterID: s.Target.ClusterID, NodeName: p.Admission.NodeName,
		MemberID: p.Admission.MemberID, InstanceID: p.Admission.InstanceID}
	founding, ready := s.admissionBasis(identity)
	if !ready || founding == "" || founding != topology.OperationID(p.Admission.OperatorID) {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	for _, prior := range s.Admissions {
		if prior.NodeName == identity.NodeName && prior.MemberID == identity.MemberID {
			// Founding consumes the seed identity once. A retained restart uses
			// its durable pin/session path; a replacement requires a fresh ID.
			return proposalApplication{}, fault.ErrOperationAuthorization
		}
	}
	next := *s
	record := p.Admission
	record.State = AdmissionCompleted
	next.Admissions = append(slices.Clone(s.Admissions), record)
	next.AppliedIndex = index
	var err error
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = next
	return proposalApplication{changed: true, result: admissionResult(p.ProposalID, record)}, nil
}

func validateAdmissionBinding(p BindAdmissionProposal) error {
	if err := validateAdmissionIdentity(p.Admission); err != nil {
		return err
	}
	if p.Admission.ReadmissionInstanceID != "" || p.Admission.ReadmissionOperatorID != "" || p.At.IsZero() {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func admissionResult(id string, record AdmissionRecord) ApplyResult {
	return ApplyResult{ProposalID: id, Accepted: true, Admission: cloneAdmission(record)}
}
