package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func (m *Machine) AuthorityFloorsForNode(node string) []topology.AuthorityGrant {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.authorityFloorsLocked(node)
}
func (sm *topologySM) authorityFloorsLocked(node string) []topology.AuthorityGrant {
	var grants []topology.AuthorityGrant
	for resource, key := range sm.authority.floors {
		if resource.Kind == topology.AuthorityHost && resource.NodeName == node {
			grants = append(grants, sm.authority.records[key].Grant.Clone())
		}
	}
	slices.SortFunc(grants, func(a, b topology.AuthorityGrant) int { return compareAuthorityKey(a.Key, b.Key) })
	return grants
}

// AuthorityProof is captured in a single safe FSM query. It exposes only one
// retained grant, its immutable receipts and the requested courier's identity.
type AuthorityProof struct {
	Grant    *topology.AuthorityGrant `json:"grant,omitempty"`
	Courier  topology.Identity        `json:"courier"`
	Receipts []topology.EffectReceipt `json:"receipts,omitempty"`
	// CleanupTargets have exact retained cleanup authority and are absent from
	// approved Target in the same safe query. The receiver still owns its local
	// resource gate and must compare the durable ledger identity before deletion.
	CleanupTargets []topology.AuthorityTarget `json:"cleanup_targets,omitempty"`
	ReleasedHosts  []topology.AuthorityTarget `json:"released_hosts,omitempty"`
}

// Optional removal follows complete approved convergence, including host steps
// whose grants have not yet been issued. Catalog checks additionally cover
// inherited revocations and nonoperator authority outside that operator cursor.
func (sm *topologySM) hostReleasedLocked(record *authorityRecord, target topology.AuthorityTarget) bool {
	grant := record.Grant
	if grant.Key.GroupID != topology.MetaGroupID || grant.Action == nil || grant.Action.Kind != topology.StepStopReplica || target.ReplicaID != target.MemberID ||
		sm.active.Version == 0 || sm.active.Version != sm.target.Version || sm.target.MemberID(target.NodeName) == target.MemberID || sm.active.MemberID(target.NodeName) == target.MemberID {
		return false
	}
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: target.NodeName}
	if !slices.ContainsFunc(record.Effects, func(receipt topology.EffectReceipt) bool { return receipt.Resource == host && receipt.Target == target }) {
		return false
	}
	for _, candidate := range sm.authority.records {
		if !slices.ContainsFunc(candidate.Grant.Targets, func(t topology.AuthorityTarget) bool {
			return t.NodeName == target.NodeName && t.MemberID == target.MemberID
		}) {
			continue
		}
		resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: candidate.Grant.Key.GroupID, NodeName: target.NodeName}
		if !slices.Contains(candidate.Grant.Resources, resource) {
			continue
		}
		if candidate.Grant.Purpose == topology.AuthorityFence && sm.authority.fenceConsumed(candidate.Grant.Key, resource) {
			continue
		}
		if !sm.authority.resourceSettled(candidate, resource) {
			return false
		}
	}
	return true
}
