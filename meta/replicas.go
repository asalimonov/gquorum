package meta

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ReplicaRecord is a node-local boot ledger entry, never replicated through meta.
type ReplicaRecord = metastore.ReplicaRecord
type LoadReplicaScope = metaengine.LoadScope

// Identity returns the current owned meta pin, including readmission changes.
func (g *Group) Identity() Identity { return g.currentIdentity() }

// LookupLoadReplica is available while quiesced, until final database closure.
func (g *Group) LookupLoadReplica(ctx context.Context, group string) (ReplicaRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ReplicaRecord{}, false, err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.store == nil {
		return ReplicaRecord{}, false, fault.ErrReplicaNotReady
	}
	return g.store.LookupReplica(group)
}

// LoadReplicaScope verifies approved Target identity, rather than staged Latest
// intent. Effect receivers must additionally Authorize the complete callback
// identity before invoking runtime operations.
func (g *Group) LoadReplicaScope(ctx context.Context, id storage.Identity) (scope LoadReplicaScope, err error) {
	if err = g.ensureRunning(ctx); err != nil {
		return
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if err = g.ensureRuntimeLocked(false); err != nil {
		return
	}
	if id.NodeName != g.identity.NodeName || id.ClusterID != g.identity.ClusterID {
		return scope, fault.ErrIdentityMismatch
	}
	err = g.machine.WithLoadScope(id, g.identity.MemberID, func(owned metaengine.LoadScope) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		scope = owned
		return nil
	})
	return
}

// RecordLoadReplica records an already opened durable replica. previous is zero
// for first creation, or the exact ledger ID being replaced after approved
// reseating. Repeated calls preserve the first record's audit fields.
func (g *Group) RecordLoadReplica(ctx context.Context, group string, record ReplicaRecord, previous uint64) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if err := g.ensureRuntimeLocked(false); err != nil {
		return err
	}
	if g.store == nil {
		return fault.ErrReplicaNotReady
	}
	if record.NodeName != "" && record.NodeName != g.identity.NodeName || record.ClusterID != "" && record.ClusterID != g.identity.ClusterID {
		return fault.ErrIdentityMismatch
	}
	record.NodeName, record.ClusterID = g.identity.NodeName, g.identity.ClusterID
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	id := storage.Identity{NodeName: record.NodeName, ClusterID: record.ClusterID, GroupID: group, ReplicaID: record.ReplicaID}
	return g.machine.WithLoadScope(id, g.identity.MemberID, func(metaengine.LoadScope) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return g.store.RecordReplicaCAS(group, record, previous)
	})
}

// ForgetLoadReplica erases only an exact, authorized cleanup identity after
// caller-owned durable storage cleanup. Whole-group deletion takes the old
// local ID explicitly because its authorized step has no single replica ID.
// Stopping or parking a handle must not erase its boot ledger.
func (g *Group) ForgetLoadReplica(ctx context.Context, a topology.AuthorizedStep, expected uint64) error {
	if expected == 0 {
		return fault.ErrInvalid
	}
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if err := g.ensureRuntimeLocked(false); err != nil {
		return err
	}
	if g.store == nil {
		return fault.ErrReplicaNotReady
	}
	if a.Kind != topology.StepDeleteGroupState && (a.Kind != topology.StepCleanupReplicaState || a.NodeName != g.identity.NodeName || a.ReplicaID != expected) {
		return fault.ErrExecutionAuthorization
	}
	if err := g.machine.Authorize(ctx, a); err != nil {
		return err
	}
	target := g.machine.Target()
	if group := target.Group(a.GroupID); group != nil {
		for _, replica := range group.Replicas {
			if replica.NodeName == g.identity.NodeName && replica.ReplicaID == expected {
				return fault.ErrIdentityMismatch
			}
		}
	}
	return g.store.ForgetReplica(a.GroupID, expected)
}
