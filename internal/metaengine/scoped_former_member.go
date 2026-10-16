package metaengine

import "github.com/asalimonov/gquorum/topology"

// An approved but never prepared intermediate member is not the former owner.
// Safety permits at most one unretired process owner: every new preparation
// requires the previous owner's complete retirement batch. Refuse a contradictory
// multi-owner graph rather than widening the single-former-member protocol.
func unretiredFormerMembers(state *scopedState, index *scopedIndexes,
	catalog authorityCatalog, admission AdmissionRecord) map[uint64]bool {
	members := make(map[uint64]bool)
	add := func(member uint64) {
		if member != 0 && member < admission.MemberID {
			members[member] = true
		}
	}
	for _, row := range state.Groups {
		if groupNamesNode(row.Declaration, admission.NodeName) {
			doc, _ := checkpointDocument(state, index, row)
			add(doc.MemberID(admission.NodeName))
		}
	}
	for member := range preparedFormerMembers(catalog, admission) {
		add(member)
	}
	for _, debt := range state.Cleanup {
		if debt.Identity.NodeName == admission.NodeName && debt.Retired == nil {
			add(debt.MemberID)
		}
	}
	removeRetiredMembers(catalog, admission.NodeName, members)
	return members
}

func preparedFormerMembers(catalog authorityCatalog, admission AdmissionRecord) map[uint64]bool {
	members := make(map[uint64]bool)
	for _, record := range catalog.records {
		action := record.Grant.Action
		if action == nil || action.Kind != topology.StepPrepareReplica &&
			action.Kind != topology.StepCreateFoundingGroup {
			continue
		}
		for _, target := range record.Grant.Targets {
			if target.NodeName == admission.NodeName && target.MemberID < admission.MemberID {
				members[target.MemberID] = true
			}
		}
	}
	return members
}

func removeRetiredMembers(catalog authorityCatalog, node string, members map[uint64]bool) {
	for _, record := range catalog.records {
		if record.Grant.Purpose != topology.AuthorityReadmit || record.Grant.Key.GroupID != topology.MetaGroupID ||
			!catalog.readmissionBatchConsumed(record.Grant.AdmissionID) {
			continue
		}
		for _, target := range record.Grant.Targets {
			if target.NodeName == node {
				delete(members, target.MemberID)
			}
		}
	}
}
