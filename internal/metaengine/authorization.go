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
	refuse := func() error {
		return fault.New(fault.ExecutionAuthorization, "effect does not match current approval", nil)
	}
	if a.StepSeq == 0 {
		if a.ExecutionID != "" || a.Attempt != 0 || a.Version == 0 || a.Version != sm.active.Version || a.Kind != topology.StepTransferLeader {
			return refuse()
		}
		r := documentReplica(sm.active, a.GroupID, a.ReplicaID)
		if r == nil || r.NodeName != a.NodeName || r.Role != topology.ReplicaVoter {
			return refuse()
		}
		return nil
	}
	if a.Version > sm.target.Version {
		return fault.New(fault.Unavailable, "receiver has not applied required approval", nil)
	}
	ex, ok := sm.executions[a.ExecutionID]
	if !ok || ex.Version != sm.target.Version || ex.State.Terminal() || ex.State == topology.ExecutionPending || ex.CurrentStep < 1 || ex.CurrentStep > len(ex.Steps) {
		return refuse()
	}
	latest, found := sm.latestAttemptLocked(ex.Version)
	if !found || latest.ID != ex.ID || ex.CurrentStep != a.StepSeq || !a.Matches(ex.ID, ex.Attempt, ex.Version, ex.Steps[ex.CurrentStep-1]) {
		return refuse()
	}
	return nil
}
