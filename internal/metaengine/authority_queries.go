package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func queryScopedAuthorityProof(s *scopedState, q QueryRequest) (AuthorityProof, error) {
	if s == nil {
		return AuthorityProof{}, fault.ErrUnsupported
	}
	if q.OperationID != "" || q.AuthorityKey == nil || q.AuthorityKey.ClusterID != s.Target.ClusterID ||
		q.AuthorityKey.Revision == 0 || topology.ValidateID(q.AuthorityKey.GroupID) != nil {
		return AuthorityProof{}, fault.ErrInvalid
	}
	proof := AuthorityProof{}
	if record := s.indexes.grants[*q.AuthorityKey]; record != nil {
		grant := record.Grant.Clone()
		proof.Grant = &grant
		proof.Receipts = slices.Clone(record.Effects)
		catalog := s.authorityCatalog()
		for _, target := range grant.Targets {
			if grant.Key.GroupID == topology.MetaGroupID && grant.Action != nil &&
				grant.Action.Kind == topology.StepStopReplica && target.ReplicaID == target.MemberID &&
				s.Target.MemberID(target.NodeName) != target.MemberID &&
				slices.ContainsFunc(record.Effects, func(receipt topology.EffectReceipt) bool {
					return receipt.Resource.Kind == topology.AuthorityHost && receipt.Target == target
				}) && s.hostCleanupRetired(target) && catalog.hostResourcesSettled(target) {
				proof.ReleasedHosts = append(proof.ReleasedHosts, target)
			}
			if topology.ValidateLoadID(grant.Key.GroupID) == nil &&
				queryScopedLoadCleanup(s, grant, target) == nil {
				proof.CleanupTargets = append(proof.CleanupTargets, target)
			}
		}
	}
	if q.Key != "" {
		proof.Courier, _ = scopedProcessIdentity(s, q.Key)
	}
	return proof, nil
}

// A removed process must remain available to retire its exact local owners even
// before the cleanup worker issues their grants. Collection may follow later;
// neither retained collection debt nor a successor member owns this process.
func (s *scopedState) hostCleanupRetired(target topology.AuthorityTarget) bool {
	return !slices.ContainsFunc(s.Cleanup, func(debt cleanupRecord) bool {
		return debt.Identity.NodeName == target.NodeName && debt.MemberID == target.MemberID && debt.Retired == nil
	})
}

func scopedProcessIdentity(s *scopedState, name string) (topology.Identity, bool) {
	member := scopedMemberID(s, name)
	id := topology.Identity{NodeName: name, ClusterID: s.Target.ClusterID, MemberID: member}
	if member == 0 {
		return id, false
	}
	if admission := scopedLiveAdmission(s, name, ""); admission != nil && admission.MemberID == member {
		id.InstanceID = admission.InstanceID
		return id, id.InstanceID != ""
	}
	// A session predates a replacement approval until its exact member has
	// converged. Never attach the former process to the successor's member ID.
	meta := s.indexes.groups[topology.MetaGroupID]
	if meta == nil || !groupHasMember(meta.Declaration, name, member) {
		return id, false
	}
	id.InstanceID = scopedLockedSessionInstance(s, name)
	return id, id.InstanceID != ""
}

func scopedMemberID(s *scopedState, name string) uint64 {
	if member := s.Target.MemberID(name); member != 0 {
		return member
	}
	for _, row := range s.Nodes {
		if row.NodeName == name {
			return row.MemberID
		}
	}
	return 0
}

func scopedLockedSessionInstance(s *scopedState, name string) string {
	for _, lock := range s.Locks {
		if lock.Key != "node/"+name {
			continue
		}
		for _, session := range s.Sessions {
			if session.SessionID == lock.SessionID && session.NodeName == name {
				return session.InstanceID
			}
		}
	}
	return ""
}

// AuthorityFloorsForNode returns sorted owned grants for a node's current
// host-resource floors.
func (m *Machine) AuthorityFloorsForNode(node string) []topology.AuthorityGrant {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return scopedAuthorityFloorsForNode(m.scoped, node)
}

func scopedAuthorityFloorsForNode(state *scopedState, node string) []topology.AuthorityGrant {
	if state == nil || node == "" {
		return nil
	}
	var grants []topology.AuthorityGrant
	for _, floor := range state.AuthorityFloors {
		if floor.Resource.Kind != topology.AuthorityHost || floor.Resource.NodeName != node {
			continue
		}
		for _, record := range state.AuthorityRecords {
			if record.Grant.Key == floor.Key {
				grants = append(grants, record.Grant.Clone())
				break
			}
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
	// resource gate and must compare the durable ledger identity before
	// deletion.
	CleanupTargets []topology.AuthorityTarget `json:"cleanup_targets,omitempty"`
	ReleasedHosts  []topology.AuthorityTarget `json:"released_hosts,omitempty"`
}

func (a authorityCatalog) hostResourcesSettled(target topology.AuthorityTarget) bool {
	for _, candidate := range a.records {
		if !slices.ContainsFunc(candidate.Grant.Targets, func(t topology.AuthorityTarget) bool {
			return t.NodeName == target.NodeName && t.MemberID == target.MemberID
		}) {
			continue
		}
		resource := topology.AuthorityResource{
			Kind:     topology.AuthorityHost,
			GroupID:  candidate.Grant.Key.GroupID,
			NodeName: target.NodeName,
		}
		if !slices.Contains(candidate.Grant.Resources, resource) {
			continue
		}
		if candidate.Grant.Purpose == topology.AuthorityFence &&
			a.fenceConsumed(candidate.Grant.Key, resource) {
			continue
		}
		if !a.resourceSettled(candidate, resource) {
			return false
		}
	}
	return true
}
