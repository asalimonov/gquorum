package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Authorize checks the locally applied approval. A receiver must additionally
// verify its own node/replica identity and the identity of any existing effect.
func (sm *topologySM) Authorize(ctx context.Context, a topology.AuthorizedStep) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.authorizeLocked(ctx, a)
}

func (sm *topologySM) authorizeLocked(ctx context.Context, a topology.AuthorizedStep) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	refuse := func() error {
		return fault.New(fault.OperationAuthorization, "effect does not match current approval", nil)
	}
	if a.StepSeq == 0 {
		return sm.authorizeDirectTransferLocked(a)
	}
	if a.Version > sm.target.Version {
		return refuse()
	}
	ex, ok := sm.operators[a.OperatorID]
	if !ok || ex.Version != sm.target.Version || a.StepSeq < 1 || a.StepSeq > len(ex.Steps) ||
		ex.CurrentStep > len(ex.Steps) || ex.State.Terminal() {
		return refuse()
	}
	latest, found := sm.latestAttemptLocked(ex.Version)
	if !found || latest.ID != ex.ID || !a.Matches(ex.ID, ex.Attempt, ex.Version, ex.Steps[a.StepSeq-1]) {
		return refuse()
	}
	// The leader can deliver an effect before this follower applies the
	// already-committed progress transition. Match every immutable approval
	// field first, then refuse execution with a retryable lag result. No effect
	// is authorized until this receiver reaches the exact current step.
	if ex.State == topology.OperatorPending || ex.CurrentStep < a.StepSeq {
		return fault.New(fault.Unavailable, "receiver has not applied required execution progress", nil)
	}
	if ex.CurrentStep != a.StepSeq {
		return refuse()
	}
	return nil
}

func (sm *topologySM) authorizeDirectTransferLocked(a topology.AuthorizedStep) error {
	if a.OperatorID != "" || a.Attempt != 0 || a.Version == 0 || a.Version != sm.active.Version ||
		a.Kind != topology.StepTransferLeader || a.TransferNodeName != "" || a.TransferReplicaID != 0 {
		return fault.New(fault.OperationAuthorization, "effect does not match current approval", nil)
	}
	r := documentReplica(sm.active, a.GroupID, a.ReplicaID)
	if r == nil || r.NodeName != a.NodeName || r.Role != topology.ReplicaVoter {
		return fault.New(fault.OperationAuthorization, "effect does not match current approval", nil)
	}
	return nil
}
