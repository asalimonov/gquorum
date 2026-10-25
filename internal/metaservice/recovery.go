package metaservice

import (
	"context"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

// WaitRecoveryReplay waits for ordinary metadata replay/founding/session startup,
// independently of Node's managed-load/public recovery readiness barrier.
func (g *Group) WaitRecoveryReplay(ctx context.Context) error {
	if err := g.WaitReady(ctx); err != nil {
		return err
	}
	return g.ReadBarrier(ctx)
}

// ImportRecovery submits only the exact locally accepted private startup import.
func (g *Group) ImportRecovery(ctx context.Context, proposal model.ImportProposal) (topology.OperationID, error) {
	if err := g.authorizeRecoveryOwner(); err != nil {
		return "", err
	}
	if !reflect.DeepEqual(model.ImportFromManifest(*g.opts.Recovery), proposal) {
		return "", fault.ErrIdentityMismatch
	}
	if !g.IsLeader() {
		return "", fault.ErrNotLeader
	} // Initial import is a singleton; never generic-RPC forwarded.
	result, err := g.propose(ctx, proposal)
	return result.OperationID, err
}

// ReadRecovery confirms one bounded metadata observation and returns owned data.
func (g *Group) ReadRecovery(ctx context.Context) (*model.ImportState, error) {
	var result *model.ImportState
	err := g.readControl(ctx, metaengine.QueryRequest{Kind: "recovery"}, &result)
	return result, err
}

// RecordRecoveredGroup records exact normalized-checkpoint and creation receipts.
func (g *Group) RecordRecoveredGroup(ctx context.Context, proposal model.GroupConsumedProposal) error {
	if err := g.authorizeRecoveryOwner(); err != nil {
		return err
	}
	if proposal.RecoveryID != g.opts.Recovery.Request.RecoveryID ||
		proposal.PlanDigest != g.opts.Recovery.PlanDigest {
		return fault.ErrIdentityMismatch
	}
	if !g.IsLeader() {
		return fault.ErrNotLeader
	}
	_, err := g.propose(ctx, proposal)
	return err
}

// CompleteRecovery releases the replicated import only after every exact consumption.
func (g *Group) CompleteRecovery(ctx context.Context, proposal model.CompleteImportProposal) error {
	if err := g.authorizeRecoveryOwner(); err != nil {
		return err
	}
	if proposal.RecoveryID != g.opts.Recovery.Request.RecoveryID ||
		proposal.PlanDigest != g.opts.Recovery.PlanDigest {
		return fault.ErrIdentityMismatch
	}
	if !g.IsLeader() {
		return fault.ErrNotLeader
	}
	_, err := g.propose(ctx, proposal)
	return err
}

func (g *Group) authorizeRecoveryOwner() error {
	accepted := g.opts.Recovery
	if accepted == nil || accepted.Request.Mode != model.Rebuild || accepted.SuccessorStartedAt == nil {
		return fault.ErrOperationAuthorization
	}
	if err := model.ValidateManifest(*accepted); err != nil {
		return err
	}
	if !reflect.DeepEqual(g.rules, accepted.Rules) {
		return fault.ErrIdentityMismatch
	}
	identity := g.currentIdentity()
	if identity.ClusterID != accepted.Identity.ClusterID ||
		identity.NodeName != accepted.Identity.NodeName || identity.MemberID != accepted.Identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func (g *Group) recoveryPending() bool {
	state := g.state()
	if state != nil {
		if recovery := state.RecoveryState(); recovery != nil {
			return recovery.Phase != "complete"
		}
	}
	return g.opts.Recovery != nil
}

func (g *Group) guardRecoveryMutation() error {
	if g.recoveryPending() {
		return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
	}
	return nil
}

func (g *Group) validateRecoveryCourier(proposal any) error {
	switch proposal.(type) {
	case *model.ImportProposal, *model.GroupConsumedProposal, *model.CompleteImportProposal:
		return fault.ErrOperationAuthorization // Codec support is never general RPC authorization.
	default:
	}
	if !g.recoveryPending() {
		return nil
	}
	switch p := proposal.(type) {
	case *metaengine.GrantSession, *metaengine.RevokeSession,
		*metaengine.RecordFenceProposal, *metaengine.RecordEffectProposal, *metaengine.BeginEffectProposal,
		*metaengine.TransitionOperationProposal, *metaengine.GrantTransferProposal:
		return nil // Existing courier and exact replicated resource checks still apply.
	case *metaengine.AcquireLock:
		if p.Key == "node/"+g.currentIdentity().NodeName {
			return nil
		}
	case *metaengine.BindAdmissionProposal:
		identity := g.currentIdentity()
		if p.Founding && p.Admission.NodeName == identity.NodeName && p.Admission.MemberID == identity.MemberID {
			return nil
		}
	default:
	}
	return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
}

// The accepted import timestamp precedes B's startup. Its ordinary v1 founding
// uses that same stable instant so normal per-group approval clocks do not
// reject the subsequent deterministic import as older than its founding proof.
func (g *Group) recoveryFoundingTime(ordinary time.Time) time.Time {
	if accepted := g.opts.Recovery; accepted != nil && accepted.Request.Mode == model.Rebuild {
		return accepted.AcceptedAt
	}
	return ordinary
}
