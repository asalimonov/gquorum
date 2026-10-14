package metaengine

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// BeginEffectProposal requests immutable authority for an exact approved
// operator step.
type BeginEffectProposal struct {
	Action     topology.AuthorizedStep `json:"action"`
	ProposalID string                  `json:"proposal_id"`
}

func (sm *topologySM) applyBeginEffect(index uint64, p BeginEffectProposal) (ApplyResult, bool) {
	refuse := func(code proposalErrorCode, err error) (ApplyResult, bool) {
		return rejectProposal(p.ProposalID, code, "%v", err), false
	}
	if p.Action.StepSeq == 0 {
		return refuse(proposalAuthorization, fault.ErrOperatorAuthorization)
	}
	if err := sm.authorizeLocked(context.Background(), p.Action); err != nil {
		return refuse(proposalAuthorization, err)
	}
	if key, ok := sm.authority.actions[p.Action]; ok {
		r := sm.authority.records[key]
		if r.Closed {
			return refuse(proposalAuthorityFenced, fault.ErrAuthorityFenced)
		}
		grant := r.Grant.Clone()
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &grant}, false
	}
	if !sm.readmissionConsumedLocked(p.Action) {
		return refuse(proposalAuthorityPending, fault.ErrAuthorityPending)
	}
	for _, r := range sm.authority.revocations {
		if !revokedComplete(r) {
			return refuse(proposalAuthorityPending, fault.ErrAuthorityPending)
		}
	}
	resources, err := sm.grantResourcesLocked(p.Action)
	if err != nil {
		return refuse(proposalAuthorization, err)
	}
	for _, resource := range resources.Resources {
		if old := sm.authority.records[sm.authority.floors[resource]]; old != nil && !resourceConsumed(old, resource) &&
			!sm.authority.fenceSettled(old) {
			return refuse(proposalAuthorityPending, fault.ErrAuthorityPending)
		}
	}
	if sm.authority.pendingCost()+newAuthorityGrantCost > sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return refuse(proposalOverloaded, fault.ErrOverloaded)
	}
	a := p.Action
	grant := topology.AuthorityGrant{
		Key:        topology.AuthorityKey{ClusterID: sm.target.ClusterID, GroupID: a.GroupID, Revision: index},
		Purpose:    topology.AuthorityEffect,
		OperatorID: a.OperatorID,
		Action:     &a,
		Resources:  resources.Resources,
		Targets:    resources.Targets,
	}
	if err := grant.Validate(); err != nil {
		return refuse(proposalAuthorization, err)
	}
	sm.insertGrantLocked(grant)
	result := grant.Clone()
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grant: &result}, true
}
func (sm *topologySM) insertGrantLocked(grant topology.AuthorityGrant) {
	sm.authority.records[grant.Key] = &authorityRecord{Grant: grant.Clone()}
	if grant.Purpose == topology.AuthorityEffect {
		sm.authority.actions[*grant.Action] = grant.Key
	}
	for _, resource := range grant.Resources {
		sm.authority.floors[resource] = grant.Key
	}
}

// Only approved documents and retained immutable grants may supply identities.
// Staged Latest does not authorize a host or a consensus participant.
func (sm *topologySM) authorityDocumentsLocked() []topology.Document {
	docs := []topology.Document{sm.target, sm.active}
	for _, op := range sm.operators {
		if doc, ok := sm.documentLocked(op.Version); ok {
			docs = append(docs, doc)
		}
	}
	slices.SortFunc(docs, func(a, b topology.Document) int {
		if a.Version > b.Version {
			return -1
		}
		if a.Version < b.Version {
			return 1
		}
		return 0
	})
	return docs
}

func (sm *topologySM) authorityTargetLocked(group, node string, replica uint64) (topology.AuthorityTarget, error) {
	for _, doc := range sm.authorityDocumentsLocked() {
		if r := documentReplica(doc, group, replica); r != nil && r.NodeName == node && doc.MemberID(node) != 0 {
			return topology.AuthorityTarget{NodeName: node, MemberID: doc.MemberID(node), ReplicaID: replica}, nil
		}
	}
	var target topology.AuthorityTarget
	var revision uint64
	for _, record := range sm.authority.records {
		if record.Grant.Key.GroupID != group {
			continue
		}
		for _, candidate := range record.Grant.Targets {
			if candidate.NodeName == node && candidate.ReplicaID == replica && record.Grant.Key.Revision > revision {
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

func (sm *topologySM) grantResourcesLocked(a topology.AuthorizedStep) (grantedResources, error) {
	b := grantResourceBuilder{sm: sm, action: a, resources: map[topology.AuthorityResource]bool{},
		targets: map[topology.AuthorityTarget]bool{}}
	if err := b.build(); err != nil {
		return grantedResources{}, err
	}
	return b.sorted()
}

type grantResourceBuilder struct {
	sm        *topologySM
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

func (b *grantResourceBuilder) addTarget(node string, replica uint64) error {
	target, err := b.sm.authorityTargetLocked(b.action.GroupID, node, replica)
	if err != nil {
		return err
	}
	b.targets[target] = true
	return nil
}

func (b *grantResourceBuilder) build() error {
	switch b.action.Kind {
	case topology.StepCreateFoundingGroup:
		return b.foundingGroup()
	case topology.StepDeleteGroupState:
		return b.deletedGroup()
	case topology.StepPrepareReplica, topology.StepStopReplica, topology.StepCleanupReplicaState:
		if err := b.addTarget(b.action.NodeName, b.action.ReplicaID); err != nil {
			return err
		}
		b.addHostResource(b.action.NodeName)
		return nil
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter,
		topology.StepRemoveReplica, topology.StepTransferLeader:
		return b.consensusGroup()
	default:
		return fault.ErrUnsupported
	}
}

func (b *grantResourceBuilder) foundingGroup() error {
	b.addGroupResource()
	if g := b.sm.target.Group(b.action.GroupID); g != nil {
		for _, r := range g.Replicas {
			if r.Role != topology.ReplicaVoter {
				continue
			}
			if err := b.addTarget(r.NodeName, r.ReplicaID); err != nil {
				return err
			}
			b.addHostResource(r.NodeName)
		}
	}
	return nil
}

func (b *grantResourceBuilder) deletedGroup() error {
	if b.sm.target.Group(b.action.GroupID) != nil {
		return fault.ErrOperatorAuthorization
	}
	ids, err := b.sm.deletionCatalogLocked(b.action.GroupID)
	if err != nil {
		return err
	}
	for id := range ids {
		if err := b.addTarget(id.NodeName, id.ReplicaID); err != nil {
			return err
		}
		b.addHostResource(id.NodeName)
	}
	if len(ids) == 0 {
		b.reuseDeletionTargets()
	}
	b.addHistoricalDeletionTargets(ids)
	return nil
}

func (b *grantResourceBuilder) reuseDeletionTargets() {
	// A retry after completed group deletion still needs its own exact
	// host receipts. Reuse only the latest deletion grant's target set,
	// which already excluded earlier completed individual retirements.
	var prior *authorityRecord
	for _, record := range b.sm.authority.records {
		if record.Grant.Key.GroupID == b.action.GroupID && record.Grant.Action != nil &&
			record.Grant.Action.Kind == topology.StepDeleteGroupState && (prior == nil ||
			record.Grant.Key.Revision > prior.Grant.Key.Revision) {
			prior = record
		}
	}
	if prior != nil {
		for _, t := range prior.Grant.Targets {
			b.targets[t] = true
			b.addHostResource(t.NodeName)
		}
	}
}

func (b *grantResourceBuilder) addHistoricalDeletionTargets(ids map[storage.Identity]bool) {
	// Preserve exact member identity for unfinished historical targets. A
	// completed retirement must not resurrect an obligation to a removed
	// host.
	// Same-grant retries return the original immutable grant above.
	for _, r := range b.sm.authority.records {
		if r.Grant.Key.GroupID != b.action.GroupID {
			continue
		}
		for _, t := range r.Grant.Targets {
			if !ids[b.sm.loadIdentity(b.action.GroupID, t.NodeName, t.ReplicaID)] {
				continue
			}
			b.targets[t] = true
			b.addHostResource(t.NodeName)
		}
	}
}

func (b *grantResourceBuilder) consensusGroup() error {
	b.addGroupResource()
	if err := b.addTarget(b.action.NodeName, b.action.ReplicaID); err != nil {
		return err
	}
	if b.action.TransferReplicaID != 0 {
		if err := b.addTarget(b.action.TransferNodeName, b.action.TransferReplicaID); err != nil {
			return err
		}
	}
	if err := b.addDocumentTargets(); err != nil {
		return err
	}
	for _, record := range b.sm.authority.records {
		if record.Grant.Key.GroupID == b.action.GroupID {
			for _, target := range record.Grant.Targets {
				b.targets[target] = true
			}
		}
	}
	return nil
}

func (b *grantResourceBuilder) addDocumentTargets() error {
	for _, doc := range []topology.Document{b.sm.active, b.sm.target} {
		if g := doc.Group(b.action.GroupID); g != nil {
			for _, r := range g.Replicas {
				member := doc.MemberID(r.NodeName)
				if member == 0 {
					return fault.ErrIdentityMismatch
				}
				b.targets[topology.AuthorityTarget{NodeName: r.NodeName, MemberID: member,
					ReplicaID: r.ReplicaID}] = true
			}
		}
	}
	return nil
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
