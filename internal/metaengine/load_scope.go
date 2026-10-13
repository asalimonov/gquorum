package metaengine

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// WithGrantedLoadCleanup keeps approved assignments stable through an exact
// local ledger mutation. The resource owner has already admitted this work at
// its installed floor; a closed retained grant is still valid proof of identity.
// fn must be bounded and must not call back into this machine.
func (m *Machine) WithGrantedLoadCleanup(ctx context.Context, grant topology.AuthorityGrant, target topology.AuthorityTarget, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := topology.ValidateLoadID(grant.Key.GroupID); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.grantedLoadCleanupLocked(grant, target); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func (sm *topologySM) grantedLoadCleanupLocked(grant topology.AuthorityGrant, target topology.AuthorityTarget) error {
	record := sm.authority.records[grant.Key]
	if record == nil {
		// The courier can deliver a committed grant before this returning
		// participant applies it. Keep the ledger intact and let the controller
		// retry after catch-up; absence at an already applied index is a refusal.
		if grant.Validate() == nil && grant.Key.ClusterID == sm.target.ClusterID && grant.Key.Revision > sm.appliedIndex {
			return fault.New(fault.Unavailable, "receiver has not applied required cleanup grant", nil)
		}
		return fault.ErrOperatorAuthorization
	}
	if !record.Grant.Equal(grant) || grant.Action == nil || !slices.Contains(grant.Targets, target) ||
		!slices.Contains(grant.Resources, topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: grant.Key.GroupID, NodeName: target.NodeName}) {
		return fault.ErrOperatorAuthorization
	}
	a := grant.Action
	if a.Kind != topology.StepDeleteGroupState && (a.Kind != topology.StepCleanupReplicaState || a.NodeName != target.NodeName || a.ReplicaID != target.ReplicaID) {
		return fault.ErrOperatorAuthorization
	}
	if group := sm.target.Group(grant.Key.GroupID); group != nil {
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

// WithLoadScope holds the FSM read boundary through a local ledger operation.
// fn must be bounded and must not call back into this machine.
func (m *Machine) WithLoadScope(id storage.Identity, member uint64, fn func(LoadScope) error) error {
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.retired[id.GroupID] != 0 {
		return fault.ErrGroupRetired
	}
	if member == 0 || id.ReplicaID == 0 || id.ClusterID != m.target.ClusterID || m.target.MemberID(id.NodeName) != member {
		return fault.New(fault.IdentityMismatch, "load owner differs from approved meta identity", nil)
	}
	group := m.target.Group(id.GroupID)
	if group == nil {
		return fault.New(fault.IdentityMismatch, "load group is not approved", nil)
	}
	scope := LoadScope{Version: m.target.Version}
	for _, replica := range group.Replicas {
		if replica.NodeName == id.NodeName && replica.ReplicaID == id.ReplicaID {
			scope.Replica = replica
			break
		}
	}
	if scope.Replica.ReplicaID == 0 {
		return fault.New(fault.IdentityMismatch, "load replica is not approved", nil)
	}
	owned := (topology.Document{Groups: []topology.Group{*group}}).Clone()
	scope.Group = owned.Groups[0]
	if active := m.active.Group(id.GroupID); active != nil {
		for _, replica := range active.Replicas {
			if replica.NodeName == id.NodeName && replica.ReplicaID == id.ReplicaID {
				scope.Started, scope.Established = true, true
			}
		}
	}
	for _, ex := range m.operators {
		for _, step := range ex.Steps {
			if ex.Version == m.target.Version && step.GroupID == id.GroupID && step.Kind == topology.StepCreateFoundingGroup && scope.Replica.Role == topology.ReplicaVoter {
				scope.Founding = true
			}
			completed := step.Seq < ex.CurrentStep || ex.State == topology.OperatorSucceeded && step.Seq == ex.CurrentStep
			if !completed || step.GroupID != id.GroupID {
				continue
			}
			if step.Kind == topology.StepPrepareReplica && step.NodeName == id.NodeName && step.ReplicaID == id.ReplicaID {
				scope.Started = true
			}
			if (step.Kind == topology.StepAddLearner || step.Kind == topology.StepPromoteVoter) && step.NodeName == id.NodeName && step.ReplicaID == id.ReplicaID {
				scope.Started, scope.Established = true, true
			}
			if step.Kind == topology.StepCreateFoundingGroup {
				for _, doc := range m.history {
					if doc.Version != ex.Version {
						continue
					}
					if old := doc.Group(id.GroupID); old != nil {
						for _, replica := range old.Replicas {
							if replica.NodeName == id.NodeName && replica.ReplicaID == id.ReplicaID && replica.Role == topology.ReplicaVoter {
								scope.Started, scope.Established = true, true
							}
						}
					}
				}
			}
		}
	}
	return fn(scope)
}
