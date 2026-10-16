package metaengine

import (
	"cmp"
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type blockRef struct {
	OperationID topology.OperationID
	GroupID     string
}

type scopedRevocation struct {
	state     *scopedState
	catalog   authorityCatalog
	requested map[blockRef]*scopedRevocationRecord
	resources map[topology.AuthorityResource]map[blockRef]bool
	targets   map[string]map[topology.AuthorityTarget]bool
	index     uint64
}

// Called on an owned candidate only. All fallible work precedes publication.
// Legacy whole-document revocation is removed at the schema cutover (Task 78).
func (s *scopedState) revokeAuthorityLocked(index uint64, blocks []blockRef,
	successor topology.OperationID) error {
	if index <= s.AppliedIndex {
		return fault.ErrOperationAuthorization
	}
	lookup, err := s.buildIndexes()
	if err != nil {
		return err
	}
	catalog, err := restoreScopedAuthority(s, lookup)
	if err != nil {
		return err
	}
	t := scopedRevocation{state: s, catalog: catalog.clone(), index: index,
		requested: make(map[blockRef]*scopedRevocationRecord),
		resources: make(map[topology.AuthorityResource]map[blockRef]bool),
		targets:   make(map[string]map[topology.AuthorityTarget]bool)}
	t.copyRevocations()
	selected := make(map[blockRef]bool, len(blocks))
	for _, block := range blocks {
		selected[block] = true
		t.request(block)
	}
	conflicts := s.successorResources(lookup.operations[successor])
	for _, record := range s.AuthorityRecords {
		if t.shouldClose(record, selected, conflicts) {
			if err := t.closeRecord(record.Grant, successor); err != nil {
				return err
			}
		}
	}
	if err := t.installFences(); err != nil {
		return err
	}
	t.publish()
	return nil
}

func (t *scopedRevocation) copyRevocations() {
	for _, row := range t.state.Revocations {
		owned := row
		owned.Obligations = (topology.AuthorityStatus{Obligations: row.Obligations}).Clone().Obligations
		t.requested[blockRef{row.OperationID, row.GroupID}] = &owned
	}
}

func (t *scopedRevocation) request(block blockRef) *scopedRevocationRecord {
	row := t.requested[block]
	if row == nil {
		row = &scopedRevocationRecord{OperationID: block.OperationID, GroupID: block.GroupID}
		t.requested[block] = row
	}
	row.Requested = true
	return row
}

func (t *scopedRevocation) shouldClose(record scopedAuthorityRecord, selected map[blockRef]bool,
	conflicts map[topology.AuthorityResource]bool) bool {
	grant := record.Grant
	if grant.Purpose == topology.AuthorityFence {
		return false
	}
	if grant.Purpose == topology.AuthorityReadmit {
		// Unchanged exact retirement can outlive the original admission owner.
		return !t.state.readmissionStillBound(grant.AdmissionID)
	}
	if record.Class == "execution" && selected[blockRef{topology.OperationID(grant.OperatorID), grant.Key.GroupID}] {
		return true
	}
	return slices.ContainsFunc(grant.Resources, func(resource topology.AuthorityResource) bool {
		return conflicts[resource]
	})
}

func (s *scopedState) readmissionStillBound(id string) bool {
	return slices.ContainsFunc(s.Admissions, func(a AdmissionRecord) bool {
		return ReadmissionID(a) == id && s.Target.MemberID(a.NodeName) == a.MemberID
	})
}

func (s *scopedState) readmissionOwner(id string) topology.OperationID {
	for _, admission := range s.Admissions {
		if ReadmissionID(admission) == id {
			return admission.originOperator()
		}
	}
	return ""
}

func (t *scopedRevocation) closeRecord(grant topology.AuthorityGrant, successor topology.OperationID) error {
	owner := topology.OperationID(grant.OperatorID)
	if grant.Purpose == topology.AuthorityReadmit {
		owner = t.state.readmissionOwner(grant.AdmissionID)
	}
	if owner == "" {
		owner = successor // Direct administrative transfer has no execution owner.
	}
	if owner == "" {
		return fault.ErrOperationAuthorization
	}
	ref := blockRef{owner, grant.Key.GroupID}
	row := t.request(ref)
	record := t.catalog.records[grant.Key]
	record.Closed = true
	for _, resource := range grant.Resources {
		if t.catalog.resourceSettled(record, resource) {
			continue
		}
		floor := t.catalog.records[t.catalog.floors[resource]]
		if floor != nil && floor.Grant.Purpose == topology.AuthorityFence {
			row.Obligations = appendObligation(row.Obligations,
				topology.FenceObligation{Key: floor.Grant.Key, Resource: resource})
			continue
		}
		t.addResource(resource, ref, grant.Targets)
	}
	return nil
}

func (t *scopedRevocation) addResource(resource topology.AuthorityResource,
	owner blockRef, targets []topology.AuthorityTarget) {
	if t.resources[resource] == nil {
		t.resources[resource] = make(map[blockRef]bool)
	}
	t.resources[resource][owner] = true
	if t.targets[resource.GroupID] == nil {
		t.targets[resource.GroupID] = make(map[topology.AuthorityTarget]bool)
	}
	for _, target := range targets {
		if resource.Kind == topology.AuthorityGroup || resource.NodeName == target.NodeName {
			t.targets[resource.GroupID][target] = true
		}
	}
}

func (t *scopedRevocation) installFences() error {
	for _, group := range slices.Sorted(maps.Keys(t.targets)) {
		grant := topology.AuthorityGrant{Purpose: topology.AuthorityFence,
			Key: topology.AuthorityKey{ClusterID: t.state.Target.ClusterID, GroupID: group, Revision: t.index}}
		for resource := range t.resources {
			if resource.GroupID == group {
				grant.Resources = append(grant.Resources, resource)
			}
		}
		grant.Targets = slices.SortedFunc(maps.Keys(t.targets[group]), topology.AuthorityTarget.Compare)
		slices.SortFunc(grant.Resources, topology.AuthorityResource.Compare)
		if err := t.installFence(grant); err != nil {
			return err
		}
	}
	return nil
}

func (t *scopedRevocation) installFence(grant topology.AuthorityGrant) error {
	if err := grant.Validate(); err != nil {
		return err
	}
	if t.catalog.records[grant.Key] != nil {
		return fault.ErrIdentityMismatch
	}
	t.catalog.records[grant.Key] = &authorityRecord{Grant: grant}
	for _, resource := range grant.Resources {
		t.catalog.floors[resource] = grant.Key
		for owner := range t.resources[resource] {
			row := t.request(owner)
			row.Obligations = appendObligation(row.Obligations,
				topology.FenceObligation{Key: grant.Key, Resource: resource})
		}
	}
	return nil
}

func (t *scopedRevocation) publish() {
	classes := make(map[topology.AuthorityKey]string, len(t.state.AuthorityRecords))
	for _, row := range t.state.AuthorityRecords {
		classes[row.Grant.Key] = row.Class
	}
	records := make([]scopedAuthorityRecord, 0, len(t.catalog.records))
	for _, row := range t.catalog.snapshotRecords() {
		class := classes[row.Grant.Key]
		if class == "" {
			class = "execution"
		}
		records = append(records, scopedAuthorityRecord{authorityRecord: row, Class: class})
	}
	revocations := make([]scopedRevocationRecord, 0, len(t.requested))
	for _, row := range t.requested {
		row.Obligations = t.catalog.settledObligations(row.Obligations)
		revocations = append(revocations, *row)
	}
	slices.SortFunc(revocations, func(a, b scopedRevocationRecord) int {
		return cmp.Or(cmp.Compare(a.OperationID, b.OperationID), cmp.Compare(a.GroupID, b.GroupID))
	})
	t.state.AuthorityRecords, t.state.AuthorityFloors = records, t.catalog.snapshotFloors()
	t.state.Revocations = revocations
}

func (a authorityCatalog) settledObligations(obligations []topology.FenceObligation) []topology.FenceObligation {
	var retained []topology.FenceObligation
	for _, ob := range obligations {
		ob = a.obligationWithEvidence(ob)
		if ob.Evidence == nil && a.fenceConsumed(ob.Key, ob.Resource) {
			continue
		}
		retained = append(retained, ob)
	}
	return retained
}
