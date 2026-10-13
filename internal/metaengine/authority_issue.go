package metaengine

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

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
	resources, targets, err := sm.grantResourcesLocked(p.Action)
	if err != nil {
		return refuse(proposalAuthorization, err)
	}
	for _, resource := range resources {
		if old := sm.authority.records[sm.authority.floors[resource]]; old != nil && !resourceConsumed(old, resource) && !sm.authority.fenceSettled(old) {
			return refuse(proposalAuthorityPending, fault.ErrAuthorityPending)
		}
	}
	if sm.authority.pendingCost()+3 > sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords {
		return refuse(proposalOverloaded, fault.ErrOverloaded)
	}
	a := p.Action
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: sm.target.ClusterID, GroupID: a.GroupID, Revision: index}, Purpose: topology.AuthorityEffect, OperatorID: a.OperatorID, Action: &a, Resources: resources, Targets: targets}
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
func (sm *topologySM) grantResourcesLocked(a topology.AuthorizedStep) ([]topology.AuthorityResource, []topology.AuthorityTarget, error) {
	resources := map[topology.AuthorityResource]bool{}
	targets := map[topology.AuthorityTarget]bool{}
	group := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: a.GroupID}
	host := func(node string) {
		resources[topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: a.GroupID, NodeName: node}] = true
	}
	add := func(node string, replica uint64) error {
		t, err := sm.authorityTargetLocked(a.GroupID, node, replica)
		if err != nil {
			return err
		}
		targets[t] = true
		return nil
	}
	switch a.Kind {
	case topology.StepCreateFoundingGroup:
		resources[group] = true
		if g := sm.target.Group(a.GroupID); g != nil {
			for _, r := range g.Replicas {
				if r.Role != topology.ReplicaVoter {
					continue
				}
				if err := add(r.NodeName, r.ReplicaID); err != nil {
					return nil, nil, err
				}
				host(r.NodeName)
			}
		}
	case topology.StepDeleteGroupState:
		if sm.target.Group(a.GroupID) != nil {
			return nil, nil, fault.ErrOperatorAuthorization
		}
		ids, err := sm.deletionCatalogLocked(a.GroupID)
		if err != nil {
			return nil, nil, err
		}
		for id := range ids {
			if err := add(id.NodeName, id.ReplicaID); err != nil {
				return nil, nil, err
			}
			host(id.NodeName)
		}
		if len(ids) == 0 {
			// A retry after completed group deletion still needs its own exact
			// host receipts. Reuse only the latest deletion grant's target set,
			// which already excluded earlier completed individual retirements.
			var prior *authorityRecord
			for _, record := range sm.authority.records {
				if record.Grant.Key.GroupID == a.GroupID && record.Grant.Action != nil && record.Grant.Action.Kind == topology.StepDeleteGroupState &&
					(prior == nil || record.Grant.Key.Revision > prior.Grant.Key.Revision) {
					prior = record
				}
			}
			if prior != nil {
				for _, t := range prior.Grant.Targets {
					targets[t] = true
					host(t.NodeName)
				}
			}
		}
		// Preserve exact member identity for unfinished historical targets. A
		// completed retirement must not resurrect an obligation to a removed host.
		// Same-grant retries return the original immutable grant above.
		for _, r := range sm.authority.records {
			if r.Grant.Key.GroupID != a.GroupID {
				continue
			}
			for _, t := range r.Grant.Targets {
				if !ids[sm.loadIdentity(a.GroupID, t.NodeName, t.ReplicaID)] {
					continue
				}
				targets[t] = true
				host(t.NodeName)
			}
		}
	case topology.StepPrepareReplica, topology.StepStopReplica, topology.StepCleanupReplicaState:
		if err := add(a.NodeName, a.ReplicaID); err != nil {
			return nil, nil, err
		}
		host(a.NodeName)
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica, topology.StepTransferLeader:
		resources[group] = true
		if err := add(a.NodeName, a.ReplicaID); err != nil {
			return nil, nil, err
		}
		if a.TransferReplicaID != 0 {
			if err := add(a.TransferNodeName, a.TransferReplicaID); err != nil {
				return nil, nil, err
			}
		}
		for _, doc := range []topology.Document{sm.active, sm.target} {
			if g := doc.Group(a.GroupID); g != nil {
				for _, r := range g.Replicas {
					member := doc.MemberID(r.NodeName)
					if member == 0 {
						return nil, nil, fault.ErrIdentityMismatch
					}
					targets[topology.AuthorityTarget{NodeName: r.NodeName, MemberID: member, ReplicaID: r.ReplicaID}] = true
				}
			}
		}
		for _, record := range sm.authority.records {
			if record.Grant.Key.GroupID == a.GroupID {
				for _, target := range record.Grant.Targets {
					targets[target] = true
				}
			}
		}
	default:
		return nil, nil, fault.ErrUnsupported
	}
	var rr []topology.AuthorityResource
	for r := range resources {
		rr = append(rr, r)
	}
	slices.SortFunc(rr, topology.AuthorityResource.Compare)
	var tt []topology.AuthorityTarget
	for t := range targets {
		tt = append(tt, t)
	}
	slices.SortFunc(tt, topology.AuthorityTarget.Compare)
	if len(rr) == 0 || len(tt) == 0 {
		return nil, nil, fault.ErrHistoryUnavailable
	}
	return rr, tt, nil
}
