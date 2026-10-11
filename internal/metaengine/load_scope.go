package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LoadScope is an owned view of an approved local load replica. Started is
// durable evidence that this identity has already been created or prepared.
type LoadScope struct {
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
	var scope LoadScope
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
