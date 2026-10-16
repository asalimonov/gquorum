package metaengine

import "github.com/asalimonov/gquorum/topology"

// The caller holds the apply/read lock. All returned planning declarations are
// owned copies; the collector only reads validated immutable evidence.
func (s *scopedState) planningCollector() planningCollector {
	evidence := planningEvidence{authority: newAuthorityCatalog(), allocations: make(map[string]uint64),
		admissions: s.Admissions, cleanup: s.Cleanup}
	for _, doc := range s.indexes.documents {
		evidence.documents = append(evidence.documents, doc)
	}
	for _, allocation := range s.Allocations {
		evidence.allocations[allocation.GroupID] = allocation.NextReplicaID
	}
	for key, record := range s.indexes.grants {
		evidence.authority.records[key] = &record.authorityRecord
	}
	return planningCollector{evidence: evidence, owners: make(map[planningReplicaKey]*planningReplica),
		founding: make(map[string]bool)}
}

func (s *scopedState) planningDeclarations() topology.PlanInput {
	input := topology.PlanInput{Approved: s.Target.Clone(), Groups: make(map[string]topology.GroupProgress),
		Nodes: make(map[string]topology.NodeProgress)}
	for _, row := range s.Groups {
		input.Groups[row.GroupID] = (topology.GroupProgress{GroupID: row.GroupID,
			ConvergedVersion: row.SourceVersion, Converged: row.Declaration, OperationID: row.Owner}).Clone()
	}
	for _, row := range s.Nodes {
		input.Nodes[row.NodeName] = (topology.NodeProgress{NodeName: row.NodeName, MemberID: row.MemberID,
			ConvergedVersion: row.SourceVersion, Converged: row.Declaration}).Clone()
	}
	return input
}
