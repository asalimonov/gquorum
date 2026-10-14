package metaservice

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func readmissionGroups(grants []topology.AuthorityGrant, catalog []topology.Group) []topology.Group {
	var groups []topology.Group
	for _, grant := range grants {
		if grant.Key.GroupID == topology.MetaGroupID {
			continue
		}
		group := topology.Group{ID: grant.Key.GroupID}
		for _, old := range catalog {
			if old.ID == group.ID {
				group = (topology.Document{Groups: []topology.Group{old}}).Clone().Groups[0]
				break
			}
		}
		var replicas []topology.Replica
		for _, target := range grant.Targets {
			replica := readmissionReplica(group, target)
			replicas = append(replicas, replica)
		}
		group.Replicas = replicas
		groups = append(groups, group)
	}
	return groups
}

func readmissionReplica(group topology.Group, target topology.AuthorityTarget) topology.Replica {
	replica := topology.Replica{NodeName: target.NodeName, ReplicaID: target.ReplicaID}
	for _, old := range group.Replicas {
		if old.NodeName == target.NodeName && old.ReplicaID == target.ReplicaID {
			replica = old
			break
		}
	}
	return replica
}

func (g *Group) readmissionPayload(previous topology.Identity, result joinResponse,
	applied []topology.Group) Readmission {
	granted := previous
	granted.MemberID = result.MemberID
	if result.PreviousMemberID != 0 {
		previous.MemberID = result.PreviousMemberID
	} else if previous.MemberID == 0 {
		previous.MemberID = result.Previous.MemberID(previous.NodeName)
	}
	g.mu.RLock()
	docs := []topology.Document{result.Previous, {Groups: result.PreviousGroups}, {Groups: g.recoveryGroups},
		{Groups: applied}}
	g.mu.RUnlock()
	groups := previousNodeGroups(docs, result.Granted, previous.NodeName)
	// The previous metadata pin is named by Previous.MemberID. Other historical
	// metadata replicas are archives, not additional live resources to retire.
	groups = slices.DeleteFunc(groups, func(group topology.Group) bool { return group.ID == topology.MetaGroupID })
	return cloneReadmission(Readmission{
		Previous:       previous,
		Granted:        granted,
		AdmissionID:    metaengine.ReadmissionID(result.Admission),
		MetaDBPath:     g.opts.DBPath,
		PreviousGroups: groups,
		GrantedGroups:  nodeGroups(result.Granted, previous.NodeName),
		Grants:         result.Grants,
	})
}

func validateReadmissionResponse(identity topology.Identity, result joinResponse) error {
	if identity.MemberID != 0 && identity.MemberID != result.MemberID && !result.Readmitted {
		return fault.ErrIdentityMismatch
	}
	if len(result.Grants) == 0 {
		if result.Readmitted || result.PreviousMemberID != 0 {
			return fault.ErrOperatorAuthorization
		}
		return nil
	}
	if result.Existing || result.PreviousMemberID == 0 ||
		result.PreviousMemberID >= result.MemberID || result.Readmitted &&
		identity.MemberID != 0 && identity.MemberID != result.PreviousMemberID {
		return fault.ErrIdentityMismatch
	}
	v := readmissionValidation{identity: identity, response: result,
		seen: make(map[string]bool), prior: make(map[storage.Identity]bool)}
	for _, grant := range result.Grants {
		if err := v.validateGrant(grant); err != nil {
			return err
		}
	}
	return v.validatePreviousGroups()
}

type readmissionValidation struct {
	identity topology.Identity
	response joinResponse
	revision uint64
	seen     map[string]bool
	prior    map[storage.Identity]bool
}

func (v *readmissionValidation) validateGrant(grant topology.AuthorityGrant) error {
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: grant.Key.GroupID,
		NodeName: v.identity.NodeName}
	if grant.Key.ClusterID != v.identity.ClusterID || grant.Purpose != topology.AuthorityReadmit ||
		len(grant.Resources) != 1 || grant.Resources[0] != resource || v.seen[grant.Key.GroupID] ||
		v.revision != 0 && v.revision != grant.Key.Revision || grant.Validate() != nil ||
		grant.AdmissionID != metaengine.ReadmissionID(v.response.Admission) {
		return fault.ErrOperatorAuthorization
	}
	v.revision = grant.Key.Revision
	v.seen[grant.Key.GroupID] = true
	for _, target := range grant.Targets {
		if target.NodeName != v.identity.NodeName || target.MemberID != v.response.PreviousMemberID ||
			grant.Key.GroupID == topology.MetaGroupID && (target.ReplicaID != target.MemberID ||
				len(grant.Targets) != 1) {
			return fault.ErrIdentityMismatch
		}
		if group := v.response.Granted.Group(grant.Key.GroupID); group != nil &&
			slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool {
				return r.ReplicaID == target.ReplicaID
			}) {
			return fault.ErrIdentityMismatch
		}
		if grant.Key.GroupID != topology.MetaGroupID {
			v.prior[storage.Identity{ClusterID: v.identity.ClusterID, NodeName: v.identity.NodeName,
				GroupID:   grant.Key.GroupID,
				ReplicaID: target.ReplicaID}] = true
		}
	}
	return nil
}

func (v *readmissionValidation) validatePreviousGroups() error {
	if !v.seen[topology.MetaGroupID] {
		return fault.ErrOperatorAuthorization
	}
	for _, group := range v.response.PreviousGroups {
		for _, replica := range group.Replicas {
			id := storage.Identity{
				ClusterID: v.identity.ClusterID,
				NodeName:  replica.NodeName,
				GroupID:   group.ID,
				ReplicaID: replica.ReplicaID,
			}
			if !v.prior[id] {
				return fault.ErrOperatorAuthorization
			}
			delete(v.prior, id)
		}
	}
	if len(v.prior) != 0 {
		return fault.ErrOperatorAuthorization
	}
	return nil
}

func validateReadmissionReceipts(grants []topology.AuthorityGrant, receipts []topology.EffectReceipt) error {
	type effect struct {
		key    topology.AuthorityKey
		target topology.AuthorityTarget
	}
	pending := make(map[effect]topology.AuthorityGrant)
	for _, grant := range grants {
		for _, target := range grant.Targets {
			pending[effect{grant.Key, target}] = grant
		}
	}
	for _, receipt := range receipts {
		key := effect{receipt.Key, receipt.Target}
		grant, found := pending[key]
		if !found || receipt.Validate(grant) != nil {
			return fault.ErrOperatorAuthorization
		}
		delete(pending, key)
	}
	if len(pending) != 0 {
		return fault.ErrAuthorityPending
	}
	return nil
}
