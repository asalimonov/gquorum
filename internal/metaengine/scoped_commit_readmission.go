package metaengine

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateReadmissionScope is shared by service preflight and replicated apply.
// Only the returning member's required scopes must match approved intent.
func ValidateReadmissionScope(input topology.PlanInput, name string, member uint64) error {
	row := input.Nodes[name]
	if row.OperationID != "" {
		return fault.New(fault.Unavailable, "node is owned by operation "+string(row.OperationID), nil)
	}
	if member == 0 || row.ConvergedVersion == 0 || row.Converged == nil || row.MemberID != member ||
		input.Approved.MemberID(name) != member {
		return fault.ErrVersionConflict
	}
	for _, group := range input.Approved.Groups {
		if !slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool { return r.NodeName == name }) {
			continue
		}
		proven := input.Groups[group.ID]
		if proven.OperationID != "" {
			return fault.New(fault.Unavailable, "group is owned by operation "+string(proven.OperationID), nil)
		}
		if proven.ConvergedVersion == 0 || proven.Converged == nil || proven.Deleting ||
			!reflect.DeepEqual(proven.Converged, &group) {
			message := fmt.Sprintf("group %q is not converged to approved intent; "+
				"revert or retry its change first", group.ID)
			return fault.New(fault.VersionConflict, message, nil)
		}
	}
	return nil
}

func (s *scopedState) applyCommitReadmission(index uint64, p CommitReadmissionProposal) (proposalApplication, error) {
	if err := validateAdmissionBinding(BindAdmissionProposal{Admission: p.Admission, At: p.At}); err != nil {
		return proposalApplication{}, err
	}
	if p.Admission.OperationID != "" || p.Admission.State != "" {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	for _, prior := range s.Admissions {
		if prior.NodeName == p.Admission.NodeName && prior.InstanceID == p.Admission.InstanceID {
			return s.replayReadmission(p, prior)
		}
	}
	repair := CommitRepairOperationProposal{Doc: p.Doc, ExpectedVersion: p.ExpectedVersion,
		ExpectedTarget: p.ExpectedTarget, Scopes: p.Scopes, Plan: p.Plan, At: p.At, ProposalID: p.ProposalID}
	if p.ExpectedVersion != s.Latest.Version || p.ExpectedTarget != s.Target.Version {
		return s.repairConflict(repair), nil
	}
	pair, err := s.readmissionDocuments(p)
	if err != nil {
		return proposalApplication{}, err
	}
	// Both helpers prepare owned candidates. A binding refusal must discard the
	// approval, allocations, rebase and revocations together.
	next := *s
	result, err := next.commitRepairDocuments(index, pair, repair)
	if err != nil || !result.result.Accepted {
		return result, err
	}
	admission := p.Admission
	admission.OperationID = topology.OperationID(result.result.OperationID)
	bound, err := next.applyBindAdmission(index,
		BindAdmissionProposal{Admission: admission, At: p.At, ProposalID: p.ProposalID})
	if err != nil {
		return proposalApplication{}, err
	}
	result.result.Admission = bound.result.Admission
	*s = next
	return result, nil
}

func (s *scopedState) readmissionDocuments(p CommitReadmissionProposal) (repairDocuments, error) {
	name := p.Admission.NodeName
	if !slices.Contains(s.Activated, name) {
		return repairDocuments{}, fault.New(fault.OperationAuthorization, "node has never activated", nil)
	}
	if err := ValidateReadmissionScope(s.planningDeclarations(), name, s.Target.MemberID(name)); err != nil {
		return repairDocuments{}, err
	}
	expected, err := buildReadmissionDocument(s.Target, name, p.At)
	if err != nil {
		return repairDocuments{}, err
	}
	pair, err := s.prepareRepairDocuments(expected, p.At)
	if err != nil {
		return repairDocuments{}, err
	}
	if !reflect.DeepEqual(normalizedReadmissionDocument(p), pair.Repair) ||
		p.Admission.MemberID != pair.Repair.MemberID(name) ||
		!hasAdmissionSequence(p.Plan.Steps, pair.Repair, name, p.Admission.MemberID) {
		return repairDocuments{}, fault.ErrOperationAuthorization
	}
	return pair, nil
}

func normalizedReadmissionDocument(p CommitReadmissionProposal) topology.Document {
	doc := p.Doc.Clone()
	doc.Version, doc.ParentVersion, doc.RebasedFrom, doc.CreatedAt = p.ExpectedVersion+1, p.ExpectedTarget, 0, p.At
	canonicalizeDocument(&doc)
	return doc
}

func (s *scopedState) replayReadmission(p CommitReadmissionProposal,
	prior AdmissionRecord) (proposalApplication, error) {
	origin := prior.ReadmissionOperatorID
	if origin == "" {
		origin = topology.OperationID(prior.OperationID)
	}
	op := s.indexes.operations[origin]
	if op == nil {
		return proposalApplication{}, fault.ErrHistoryUnavailable
	}
	doc, found := s.indexes.documents[op.Status.Version]
	if !found {
		return proposalApplication{}, fault.ErrHistoryUnavailable
	}
	if prior.MemberID != p.Admission.MemberID || !op.Plan.Equal(p.Plan) ||
		!reflect.DeepEqual(doc, normalizedReadmissionDocument(p)) {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	return proposalApplication{result: ApplyResult{Accepted: true, ProposalID: p.ProposalID,
		Doc: doc.Clone(), OperationID: op.Status.ID, Admission: cloneAdmission(prior)}}, nil
}
