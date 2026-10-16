package metaengine

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LegacyWithGrantedLoadCleanup is the schema3 adapter removed at Task78.
// It keeps approved assignments stable through an exact
// local ledger mutation. The resource owner has already admitted this work at
// its installed floor; a closed retained grant is still valid proof of
// identity.
// fn must be bounded and must not call back into this machine.
func (m *Machine) LegacyWithGrantedLoadCleanup(ctx context.Context, grant topology.AuthorityGrant,
	target topology.AuthorityTarget, fn func() error) error {
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
		return missingLoadCleanupGrant(grant, sm.target.ClusterID, sm.appliedIndex)
	}
	return validateLoadCleanupTarget(sm.target, record.Grant, grant, target)
}

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
		return fault.ErrOperatorAuthorization
	}
	a := grant.Action
	if a.Kind != topology.StepDeleteGroupState && (a.Kind != topology.StepCleanupReplicaState ||
		a.NodeName != target.NodeName || a.ReplicaID != target.ReplicaID) {
		return fault.ErrOperatorAuthorization
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

// LegacyWithLoadScope is the schema3 adapter removed at Task78. It holds the
// FSM read boundary through a local ledger operation.
// fn must be bounded and must not call back into this machine.
func (m *Machine) LegacyWithLoadScope(id storage.Identity, member uint64, fn func(LoadScope) error) error {
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	scope, err := m.approvedLoadScopeLocked(id, member)
	if err != nil {
		return err
	}
	m.populateLoadScopeLocked(&scope, id)
	return fn(scope)
}

func (sm *topologySM) approvedLoadScopeLocked(id storage.Identity, member uint64) (LoadScope, error) {
	if sm.retired[id.GroupID] != 0 {
		return LoadScope{}, fault.ErrGroupRetired
	}
	return approvedLoadScope(sm.target, id, member)
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

func (sm *topologySM) populateLoadScopeLocked(scope *LoadScope, id storage.Identity) {
	if active := sm.active.Group(id.GroupID); active != nil {
		for _, replica := range active.Replicas {
			if replica.NodeName == id.NodeName && replica.ReplicaID == id.ReplicaID {
				scope.Started, scope.Established = true, true
			}
		}
	}
	for _, ex := range sm.operators {
		scope.observeOperator(sm, ex, id)
	}
}

func (scope *LoadScope) observeOperator(sm *topologySM, ex *operatorRecord, id storage.Identity) {
	for _, step := range ex.Steps {
		scope.observeStep(sm, ex, step, id)
	}
}

func (scope *LoadScope) observeStep(sm *topologySM, ex *operatorRecord, step topology.Step, id storage.Identity) {
	if ex.Version == sm.target.Version && step.GroupID == id.GroupID &&
		step.Kind == topology.StepCreateFoundingGroup && scope.Replica.Role == topology.ReplicaVoter {
		scope.Founding = true
	}
	completed := completedStep(ex, step)
	if !completed || step.GroupID != id.GroupID {
		return
	}
	if step.Kind == topology.StepPrepareReplica && step.NodeName == id.NodeName &&
		step.ReplicaID == id.ReplicaID {
		scope.Started = true
	}
	if (step.Kind == topology.StepAddLearner || step.Kind == topology.StepPromoteVoter) &&
		step.NodeName == id.NodeName && step.ReplicaID == id.ReplicaID {
		scope.Started, scope.Established = true, true
	}
	if step.Kind == topology.StepCreateFoundingGroup {
		if sm.historicalFoundingVoterLocked(ex.Version, id) {
			scope.Started, scope.Established = true, true
		}
	}
}

func (sm *topologySM) historicalFoundingVoterLocked(version uint64, id storage.Identity) bool {
	found := false
	for _, doc := range sm.history {
		if doc.Version != version {
			continue
		}
		if old := doc.Group(id.GroupID); old != nil {
			for _, replica := range old.Replicas {
				if replica.NodeName == id.NodeName && replica.ReplicaID == id.ReplicaID &&
					replica.Role == topology.ReplicaVoter {
					found = true
				}
			}
		}
	}
	return found
}
