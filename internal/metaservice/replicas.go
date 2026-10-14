package metaservice

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ReplicaRecord is a node-local boot ledger entry, never replicated through
// meta.
type ReplicaRecord = metastore.ReplicaRecord

// LoadReplicaScope aliases exact approved local startup and recovery evidence.
type LoadReplicaScope = metaengine.LoadScope

// LoadCatalog returns exact locally applied load identities, including retained
// cleanup scope.
func (g *Group) LoadCatalog(ctx context.Context, group string) ([]storage.Identity, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return nil, err
	}
	return g.state().LoadCatalog(ctx, group)
}

// Identity returns the current owned meta pin, including readmission changes.
func (g *Group) Identity() topology.Identity { return g.currentIdentity() }

// LookupLoadReplica is available while quiesced, until final database closure.
func (g *Group) LookupLoadReplica(ctx context.Context, group string) (metastore.ReplicaLookup, error) {
	if err := ctx.Err(); err != nil {
		return metastore.ReplicaLookup{}, err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.store == nil {
		return metastore.ReplicaLookup{}, fault.ErrReplicaNotReady
	}
	return g.store.LookupReplica(group)
}

// LoadReplicaScope verifies approved Target identity, rather than staged Latest
// intent. Effect receivers additionally require the exact committed grant and
// installed resource fence before invoking managed operations.
func (g *Group) LoadReplicaScope(ctx context.Context, id storage.Identity) (scope LoadReplicaScope, err error) {
	if err = g.ensureRunning(ctx); err != nil {
		return scope, err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if err = g.ensureRuntimeLocked(ordinaryRuntime); err != nil {
		return scope, err
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
	return scope, err
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
	if err := g.ensureRuntimeLocked(ordinaryRuntime); err != nil {
		return err
	}
	if g.store == nil {
		return fault.ErrReplicaNotReady
	}
	if record.NodeName != "" && record.NodeName != g.identity.NodeName || record.ClusterID != "" &&
		record.ClusterID != g.identity.ClusterID {
		return fault.ErrIdentityMismatch
	}
	record.NodeName, record.ClusterID = g.identity.NodeName, g.identity.ClusterID
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	id := storage.Identity{NodeName: record.NodeName, ClusterID: record.ClusterID, GroupID: group,
		ReplicaID: record.ReplicaID}
	return g.machine.WithLoadScope(id, g.identity.MemberID, func(metaengine.LoadScope) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return g.store.RecordReplicaCAS(group, record, previous)
	})
}

// ForgetGrantedLoadReplica is called only by the owner of an admitted local
// cleanup. It validates retained authority instead of a newer operator cursor;
// the managed host keeps its resource gate until the receipt is durable.
func (g *Group) ForgetGrantedLoadReplica(parent context.Context, grant topology.AuthorityGrant, expected uint64) error {
	if expected == 0 {
		return fault.ErrInvalid
	}
	ctxHandle, err := g.beginRPC(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer release()
	if err := g.ValidateGrant(ctx, grant); err != nil {
		return err
	}
	g.mu.RLock()
	id := g.identity
	target := topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: expected}
	if !g.quiescing && g.selfRemoval == nil {
		defer g.mu.RUnlock()
		if err := g.ensureRuntimeLocked(ordinaryRuntime); err != nil {
			return err
		}
		if g.store == nil {
			return fault.ErrReplicaNotReady
		}
		return g.machine.WithGrantedLoadCleanup(ctx, grant, target, func() error {
			return g.store.ForgetReplica(grant.Key.GroupID, expected)
		})
	}
	g.mu.RUnlock()
	// The fenced catalog may precede this grant and still assign the old
	// replica. Obtain the exact cleanup decision from the live metadata quorum.
	// The Host retains its resource gate until the final receipt; successor
	// resource fencing must join this owner before admitting newer effects.
	proof, err := g.readAuthorityProof(ctx, grant.Key, "")
	if err != nil {
		return err
	}
	if proof.Grant == nil || !proof.Grant.Equal(grant) || !slices.Contains(proof.CleanupTargets, target) {
		return fault.ErrOperatorAuthorization
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.forgetFencedReplicaLocked(ctx, storage.Identity{ClusterID: id.ClusterID,
		NodeName: id.NodeName, GroupID: grant.Key.GroupID, ReplicaID: expected}, id)
}

func (g *Group) forgetFencedReplicaLocked(ctx context.Context, replica storage.Identity, id topology.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.identity != id || g.rpcClosed {
		return fault.ErrIdentityMismatch
	}
	if g.store == nil {
		return fault.ErrReplicaNotReady
	}
	verifyIdentityErr := g.store.VerifyIdentity(metastore.Identity{NodeName: id.NodeName, ClusterID: id.ClusterID,
		MemberID: id.MemberID})
	if verifyIdentityErr != nil {
		return verifyIdentityErr
	}
	return g.store.ForgetReplica(replica.GroupID, replica.ReplicaID)
}
