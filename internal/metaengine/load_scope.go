package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func missingLoadCleanupGrant(grant topology.AuthorityGrant, cluster string, applied uint64) error {
	// A returning receiver may not yet have applied its courier's exact grant.
	if grant.Key.ClusterID == cluster && grant.Key.Revision > applied && grant.Validate() == nil {
		return fault.New(fault.Unavailable, "receiver has not applied required cleanup grant", nil)
	}
	return fault.ErrOperationAuthorization
}

func validateLoadCleanupTarget(approved topology.Document, recorded, grant topology.AuthorityGrant,
	target topology.AuthorityTarget) error {
	if grant.Action == nil || !recorded.Equal(grant) || !slices.Contains(grant.Targets, target) ||
		!slices.Contains(grant.Resources,
			topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: grant.Key.GroupID,
				NodeName: target.NodeName}) {
		return fault.ErrOperationAuthorization
	}
	a := grant.Action
	if a.Kind != topology.StepDeleteGroupState && (a.Kind != topology.StepCleanupReplicaState ||
		a.NodeName != target.NodeName || a.ReplicaID != target.ReplicaID) {
		return fault.ErrOperationAuthorization
	}
	if group := approved.Group(grant.Key.GroupID); group != nil {
		for _, replica := range group.Replicas {
			if replica.NodeName == target.NodeName && replica.ReplicaID == target.ReplicaID {
				return fault.ErrIdentityMismatch
			}
		}
	}
	return nil
}

// LoadScope is an owned view of an approved local load replica. Started is
// durable evidence that this identity has already been created or prepared.
type LoadScope struct {
	Version     uint64
	Group       topology.Group
	Replica     topology.Replica
	Started     bool
	Founding    bool
	Established bool
}

func approvedLoadScope(approved topology.Document, id storage.Identity, member uint64) (LoadScope, error) {
	if member == 0 || id.ReplicaID == 0 || id.ClusterID != approved.ClusterID ||
		approved.MemberID(id.NodeName) != member {
		return LoadScope{}, fault.New(fault.IdentityMismatch, "load owner differs from approved meta identity", nil)
	}
	group := approved.Group(id.GroupID)
	if group == nil {
		return LoadScope{}, fault.New(fault.IdentityMismatch, "load group is not approved", nil)
	}
	scope := LoadScope{Version: approved.Version}
	for _, replica := range group.Replicas {
		if replica.NodeName == id.NodeName && replica.ReplicaID == id.ReplicaID {
			scope.Replica = replica
			break
		}
	}
	if scope.Replica.ReplicaID == 0 {
		return LoadScope{}, fault.New(fault.IdentityMismatch, "load replica is not approved", nil)
	}
	owned := (topology.Document{Groups: []topology.Group{*group}}).Clone()
	scope.Group = owned.Groups[0]
	return scope, nil
}
