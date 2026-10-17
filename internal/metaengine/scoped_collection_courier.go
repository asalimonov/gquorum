package metaengine

import (
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// COLLECTION_LINEAGE: a metadata readmission rebinds the whole host journal.
// Collection rows keep their original member and replica. Later links therefore
// need not name the old load replica, whose retirement is proved separately.
func (s *scopedState) consumedJournalLinks(node string) map[uint64][]AdmissionRecord {
	links := map[uint64][]AdmissionRecord{}
	catalog := s.authorityCatalog()
	for _, record := range s.AuthorityRecords {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityReadmit || grant.Key.GroupID != topology.MetaGroupID ||
			grant.Targets[0].NodeName != node || !catalog.readmissionBatchConsumed(grant.AdmissionID) {
			continue
		}
		for _, admission := range s.Admissions {
			if admission.ReadmissionInstanceID == "" && ReadmissionID(admission) == grant.AdmissionID {
				former := grant.Targets[0].MemberID
				links[former] = append(links[former], admission)
			}
		}
	}
	return links
}

func (s *scopedState) validateCollectionCourier(debt cleanupRecord, p RecordCollectionProposal) error {
	courier := p.Courier
	if courier.ClusterID != debt.Identity.ClusterID || courier.NodeName != debt.Identity.NodeName ||
		courier.MemberID == 0 || strings.TrimSpace(courier.InstanceID) == "" ||
		strings.ContainsRune(courier.InstanceID, 0) {
		return fault.ErrIdentityMismatch
	}
	links := s.consumedJournalLinks(courier.NodeName)
	last := AdmissionRecord{MemberID: debt.MemberID}
	for len(links[last.MemberID]) != 0 {
		next := links[last.MemberID]
		if len(next) != 1 || next[0].MemberID <= last.MemberID {
			return fault.ErrUnavailable
		}
		last = next[0]
	}
	if courier.MemberID < last.MemberID {
		return fault.ErrAuthorityFenced
	}
	if courier.MemberID > last.MemberID {
		return fault.ErrUnavailable
	}
	if p.ReadmissionKey != nil {
		grant := s.authorityCatalog().records[*p.ReadmissionKey]
		if last.OperationID == "" || grant == nil || grant.Grant.Purpose != topology.AuthorityReadmit ||
			grant.Grant.AdmissionID != ReadmissionID(last) {
			return fault.ErrOperationAuthorization
		}
	}
	return nil
}

func (r *scopedRetention) closeCollectionLineage(debt cleanupRecord) {
	if debt.Storage == topology.StorageMemory || debt.Retired == nil {
		return
	}
	links := r.state.consumedJournalLinks(debt.Identity.NodeName)
	queue := []uint64{debt.MemberID}
	seen := map[uint64]bool{}
	for len(queue) != 0 {
		member := queue[0]
		queue = queue[1:]
		if seen[member] {
			continue
		}
		seen[member] = true
		for _, admission := range links[member] {
			// Keep all branches: pruning may not turn ambiguity into permission.
			r.readmissionBatch(ReadmissionID(admission))
			queue = append(queue, admission.MemberID)
		}
	}
}
