package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/topology"
)

// BeginEffectProposal requests immutable authority for an exact approved
// operator step.
type BeginEffectProposal struct {
	Action     topology.AuthorizedStep `json:"action"`
	ProposalID string                  `json:"proposal_id"`
}

func authorityTargetFrom(documents []topology.Document, catalog authorityCatalog,
	action topology.AuthorizedStep) (topology.AuthorityTarget, error) {
	for _, doc := range documents {
		if r := documentReplica(doc, action.GroupID, action.ReplicaID); r != nil &&
			r.NodeName == action.NodeName && doc.MemberID(action.NodeName) != 0 {
			return topology.AuthorityTarget{NodeName: action.NodeName,
				MemberID: doc.MemberID(action.NodeName), ReplicaID: action.ReplicaID}, nil
		}
	}
	var target topology.AuthorityTarget
	var revision uint64
	for _, record := range catalog.records {
		if record.Grant.Key.GroupID != action.GroupID {
			continue
		}
		for _, candidate := range record.Grant.Targets {
			if candidate.NodeName == action.NodeName && candidate.ReplicaID == action.ReplicaID &&
				record.Grant.Key.Revision > revision {
				target, revision = candidate, record.Grant.Key.Revision
			}
		}
	}
	if revision == 0 {
		return target, fault.ErrHistoryUnavailable
	}
	return target, nil
}

type grantedResources struct {
	Resources []topology.AuthorityResource
	Targets   []topology.AuthorityTarget
}

type grantResourceBuilder struct {
	action    topology.AuthorizedStep
	resources map[topology.AuthorityResource]bool
	targets   map[topology.AuthorityTarget]bool
}

func (b *grantResourceBuilder) addHostResource(node string) {
	b.resources[topology.AuthorityResource{Kind: topology.AuthorityHost,
		GroupID: b.action.GroupID, NodeName: node}] = true
}

func (b *grantResourceBuilder) addGroupResource() {
	b.resources[topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: b.action.GroupID}] = true
}

func (b *grantResourceBuilder) sorted() (grantedResources, error) {
	rr := collection.Slice[topology.AuthorityResource](len(b.resources))
	for r := range b.resources {
		rr = append(rr, r)
	}
	slices.SortFunc(rr, topology.AuthorityResource.Compare)
	tt := collection.Slice[topology.AuthorityTarget](len(b.targets))
	for t := range b.targets {
		tt = append(tt, t)
	}
	slices.SortFunc(tt, topology.AuthorityTarget.Compare)
	if len(rr) == 0 || len(tt) == 0 {
		return grantedResources{}, fault.ErrHistoryUnavailable
	}
	return grantedResources{Resources: rr, Targets: tt}, nil
}
