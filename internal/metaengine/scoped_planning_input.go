package metaengine

import "github.com/asalimonov/gquorum/topology"

// The caller holds the apply/read lock. All returned planning declarations are
// owned copies; the collector only reads validated immutable evidence.
func (s *scopedState) planningCollector() planningCollector {
	evidence := planningEvidence{authority: s.authorityCatalog(), allocations: make(map[string]uint64),
		admissions: s.Admissions, cleanup: s.Cleanup, operations: s.indexes.operations}
	for _, doc := range s.indexes.documents {
		evidence.documents = append(evidence.documents, doc)
	}
	for _, allocation := range s.Allocations {
		evidence.allocations[allocation.GroupID] = allocation.NextReplicaID
	}
	return planningCollector{evidence: evidence, owners: make(map[planningReplicaKey]*planningReplica),
		founding: make(map[string]bool)}
}

// Borrowed under the FSM lock. Mutation preparation must clone this catalog.
func (s *scopedState) authorityCatalog() authorityCatalog {
	catalog := newAuthorityCatalog()
	catalog.deletedGroups = s.completedDeletions()
	for key, record := range s.indexes.grants {
		catalog.records[key] = &record.authorityRecord
		if record.Grant.Purpose == topology.AuthorityEffect {
			catalog.rememberAction(record.Grant)
		}
	}
	for _, floor := range s.AuthorityFloors {
		catalog.floors[floor.Resource] = floor.Key
	}
	return catalog
}

func (s *scopedState) planningDeclarations() topology.PlanInput {
	input := topology.PlanInput{Approved: s.Target.Clone(), Groups: make(map[string]topology.GroupProgress),
		Nodes: make(map[string]topology.NodeProgress)}
	for _, row := range s.Groups {
		input.Groups[row.GroupID] = (topology.GroupProgress{GroupID: row.GroupID,
			ConvergedVersion: row.SourceVersion, Converged: row.Declaration,
			OperationID: row.Owner, Deleting: row.Deleting}).Clone()
	}
	for _, row := range s.Nodes {
		input.Nodes[row.NodeName] = (topology.NodeProgress{NodeName: row.NodeName, MemberID: row.MemberID,
			ConvergedVersion: row.SourceVersion, Converged: row.Declaration}).Clone()
	}
	return input
}
