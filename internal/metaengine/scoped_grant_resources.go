package metaengine

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) authorityDocuments() []topology.Document {
	var documents []topology.Document
	for version, doc := range s.indexes.documents {
		if scopedApprovedVersion(s, version) {
			documents = append(documents, doc)
		}
	}
	slices.SortFunc(documents, func(a, b topology.Document) int { return cmp.Compare(b.Version, a.Version) })
	return documents
}

func (s *scopedState) grantResources(action topology.AuthorizedStep) (grantedResources, error) {
	b := grantResourceBuilder{action: action, resources: make(map[topology.AuthorityResource]bool),
		targets: make(map[topology.AuthorityTarget]bool)}
	approved := s.indexes.documents[action.Version]
	switch action.Kind {
	case topology.StepCreateFoundingGroup:
		b.addGroupResource()
		for _, replica := range approved.VotersOf(action.GroupID) {
			b.addHostResource(replica.NodeName)
			b.targets[topology.AuthorityTarget{NodeName: replica.NodeName,
				MemberID: approved.MemberID(replica.NodeName), ReplicaID: replica.ReplicaID}] = true
		}
	case topology.StepPrepareReplica, topology.StepStopReplica, topology.StepCleanupReplicaState:
		target, err := authorityTargetFrom(s.authorityDocuments(), s.authorityCatalog(), action)
		if err != nil {
			return grantedResources{}, err
		}
		b.addHostResource(action.NodeName)
		b.targets[target] = true
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter,
		topology.StepRemoveReplica, topology.StepTransferLeader:
		if err := s.consensusResources(&b); err != nil {
			return grantedResources{}, err
		}
	case topology.StepDeleteGroupState:
		for _, debt := range s.Cleanup {
			if debt.RetireAction != nil && *debt.RetireAction == action {
				b.addHostResource(debt.Identity.NodeName)
				b.targets[topology.AuthorityTarget{NodeName: debt.Identity.NodeName,
					MemberID: debt.MemberID, ReplicaID: debt.Identity.ReplicaID}] = true
			}
		}
	default:
		return grantedResources{}, fault.ErrUnsupported
	}
	return b.sorted()
}

func (s *scopedState) consensusResources(b *grantResourceBuilder) error {
	b.addGroupResource()
	documents, catalog := s.authorityDocuments(), s.authorityCatalog()
	actions := []topology.AuthorizedStep{b.action}
	if b.action.TransferReplicaID != 0 {
		actions = append(actions, topology.AuthorizedStep{GroupID: b.action.GroupID,
			NodeName: b.action.TransferNodeName, ReplicaID: b.action.TransferReplicaID})
	}
	for _, action := range actions {
		target, err := authorityTargetFrom(documents, catalog, action)
		if err != nil {
			return err
		}
		b.targets[target] = true
	}
	for _, doc := range documents {
		if group := doc.Group(b.action.GroupID); group != nil {
			for _, replica := range group.Replicas {
				b.targets[topology.AuthorityTarget{NodeName: replica.NodeName,
					MemberID: doc.MemberID(replica.NodeName), ReplicaID: replica.ReplicaID}] = true
			}
		}
	}
	for _, record := range catalog.records {
		if record.Grant.Key.GroupID == b.action.GroupID {
			for _, target := range record.Grant.Targets {
				b.targets[target] = true
			}
		}
	}
	return nil
}
