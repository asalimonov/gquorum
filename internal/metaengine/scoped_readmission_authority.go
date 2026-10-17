package metaengine

import (
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyBeginReadmissionAuthority(index uint64,
	p BeginReadmissionAuthority) (proposalApplication, error) {
	admission, err := s.readmissionForAuthority(p)
	if err != nil {
		return proposalApplication{}, err
	}
	id := ReadmissionID(admission)
	catalog := s.authorityCatalog()
	grants, err := catalog.retainedScopedReadmission(p, id)
	if err != nil {
		return proposalApplication{}, err
	}
	if len(grants) != 0 {
		return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grants: grants}}, nil
	}
	former := unretiredFormerMembers(s, s.indexes, catalog, admission)
	if len(former) > 1 {
		return proposalApplication{}, fault.New(fault.Unavailable,
			"multiple former process owners require retirement", nil)
	}
	want := previousApprovedMember(s, s.indexes, p.NodeName, p.MemberID)
	for member := range former {
		want = member
	}
	if p.PreviousMemberID != want {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	if err := s.validateJoiningAdmission(admission); err != nil {
		return proposalApplication{}, err
	}
	targets := s.scopedReadmissionTargets(p)
	if len(targets[topology.MetaGroupID]) == 0 {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	for _, group := range slices.Sorted(maps.Keys(targets)) {
		resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: p.NodeName}
		if err := s.resourcesAvailable([]topology.AuthorityResource{resource}); err != nil {
			return proposalApplication{}, err
		}
		grants = append(grants, topology.AuthorityGrant{Purpose: topology.AuthorityReadmit, AdmissionID: id,
			Key:       topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: group, Revision: index},
			Resources: []topology.AuthorityResource{resource},
			Targets:   slices.SortedFunc(maps.Keys(targets[group]), topology.AuthorityTarget.Compare)})
	}
	if err := s.installGrants(index, grants); err != nil {
		return proposalApplication{}, err
	}
	return proposalApplication{changed: true,
		result: ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grants: grants}}, nil
}

func (s *scopedState) readmissionForAuthority(p BeginReadmissionAuthority) (AdmissionRecord, error) {
	if p.PreviousMemberID == 0 || p.PreviousMemberID >= p.MemberID || s.Target.MemberID(p.NodeName) != p.MemberID {
		return AdmissionRecord{}, fault.ErrOperationAuthorization
	}
	for _, admission := range s.Admissions {
		if admission.NodeName != p.NodeName || admission.InstanceID != p.InstanceID ||
			admission.MemberID != p.MemberID || admission.OperationID != p.OperationID {
			continue
		}
		if admission.State == AdmissionSuperseded || admission.State == AdmissionRetired {
			return AdmissionRecord{}, fault.ErrAuthorityFenced
		}
		return admission, nil
	}
	return AdmissionRecord{}, fault.ErrOperationAuthorization
}

func (a authorityCatalog) retainedScopedReadmission(p BeginReadmissionAuthority,
	id string) ([]topology.AuthorityGrant, error) {
	var grants []topology.AuthorityGrant
	for _, record := range a.snapshotRecords() {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityReadmit || grant.AdmissionID != id {
			continue
		}
		if err := a.currentGrant(&record); err != nil {
			return nil, err
		}
		if slices.ContainsFunc(grant.Targets, func(target topology.AuthorityTarget) bool {
			return target.MemberID != p.PreviousMemberID
		}) {
			return nil, fault.ErrOperationAuthorization
		}
		grants = append(grants, grant.Clone())
	}
	sortGrants(grants)
	return grants, nil
}

func (s *scopedState) scopedReadmissionTargets(
	p BeginReadmissionAuthority) map[string]map[topology.AuthorityTarget]bool {
	targets := make(map[string]map[topology.AuthorityTarget]bool)
	add := func(group string, target topology.AuthorityTarget) {
		if !readmissionFormerTarget(s.Target, p, group, target) {
			return
		}
		if targets[group] == nil {
			targets[group] = make(map[topology.AuthorityTarget]bool)
		}
		targets[group][target] = true
	}
	for _, doc := range s.authorityDocuments() {
		for _, group := range doc.Groups {
			for _, replica := range group.Replicas {
				add(group.ID, topology.AuthorityTarget{NodeName: replica.NodeName,
					MemberID: doc.MemberID(replica.NodeName), ReplicaID: replica.ReplicaID})
			}
		}
	}
	for _, record := range s.AuthorityRecords {
		for _, target := range record.Grant.Targets {
			add(record.Grant.Key.GroupID, target)
		}
	}
	for _, debt := range s.Cleanup {
		add(debt.Identity.GroupID, topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
			MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID})
	}
	return targets
}

func readmissionFormerTarget(desired topology.Document, p BeginReadmissionAuthority,
	group string, target topology.AuthorityTarget) bool {
	return target.NodeName == p.NodeName && target.MemberID == p.PreviousMemberID &&
		(group != topology.MetaGroupID || target.ReplicaID == p.PreviousMemberID) &&
		documentReplica(desired, group, target.ReplicaID) == nil
}
