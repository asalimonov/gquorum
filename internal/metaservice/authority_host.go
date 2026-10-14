package metaservice

import (
	"context"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

// VerifyPreparedMeta checks replay readiness and the durable acquired pin for
// this exact engine. It does not wait for admission/cluster readiness, which
// itself requires the prepare receipt before learner addition.
func (g *Group) VerifyPreparedMeta(parent context.Context, id topology.Identity) error {
	ctxHandle, err := g.acquireEffectContext(parent)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer done()
	engine := g.engine.Load()
	if engine == nil {
		return fault.ErrReplicaNotReady
	}
	if err = engine.WaitReady(ctx); err != nil {
		return err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.identity != id || g.engine.Load() != engine || engine.ReplicaID() != id.MemberID {
		return fault.ErrIdentityMismatch
	}
	if g.quiescing || g.selfRemoval != nil || !engine.Running() {
		return fault.ErrStopped
	}
	if g.store == nil {
		return fault.ErrReplicaNotReady
	}
	return g.store.VerifyIdentity(metastore.Identity{ClusterID: id.ClusterID, NodeName: id.NodeName,
		MemberID: id.MemberID})
}

// ConfirmHostRelease requires the exact metadata stop receipt and completion of
// the approved topology and retained host obligations in one safe query.
func (g *Group) ConfirmHostRelease(ctx context.Context, grant topology.AuthorityGrant,
	receipt topology.EffectReceipt) error {
	if receipt.Validate(grant) != nil {
		return fault.ErrOperatorAuthorization
	}
	proof, err := g.readAuthorityProof(ctx, grant.Key, "")
	if err != nil {
		return err
	}
	if proof.Grant == nil || !proof.Grant.Equal(grant) {
		return fault.ErrOperatorAuthorization
	}
	if !slices.Contains(proof.ReleasedHosts, receipt.Target) {
		return fault.ErrAuthorityPending
	}
	for _, recorded := range proof.Receipts {
		if reflect.DeepEqual(recorded, receipt) {
			return nil
		}
	}
	return fault.ErrAuthorityPending
}
