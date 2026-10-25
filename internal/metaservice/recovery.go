package metaservice

import (
	"context"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
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

// RecordRecoveredGroup submits a durable checkpoint; metadata selects its recorded receipts.
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
	scope := metaengine.RecoveryCommandScope{Owner: g.currentIdentity()}
	if state := g.state(); state != nil {
		if recovery := state.RecoveryState(); recovery != nil {
			scope.Version = recovery.Version
		}
		if retry, ok := proposal.(*metaengine.RetryOperationProposal); ok {
			if operation, err := state.Operation(retry.Previous); err == nil {
				scope.RetryVersion = operation.Version
			}
		}
	}
	return metaengine.ValidateRecoveryCommand(proposal, scope)
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

// RecoveryInProgress exposes the local applied import gate to its owning Node.
// It is an authorization refusal check, not a quorum or readiness observation.
func (g *Group) RecoveryInProgress() bool { return g.recoveryPending() }

func recoveryFoundingResume(startup metaStartup) (bool, error) {
	options := startup.options
	if !options.RecoveryResumeFounding {
		return false, nil
	}
	accepted := options.Recovery
	if accepted == nil || !startup.pinFound || options.Join || accepted.Request.Mode != model.Rebuild ||
		accepted.Phase != model.AwaitingStart || accepted.SuccessorStartedAt == nil {
		return false, fault.ErrOperationAuthorization
	}
	identity := options.Identity
	identity.InstanceID = ""
	if identity != accepted.Identity {
		return false, fault.ErrIdentityMismatch
	}
	only, err := metastore.StoreHasOnlyIdentity(options.DBPath, identity)
	if err != nil || !only {
		return false, fault.New(fault.OperationAuthorization, "recovery founding requires pin-only metadata", err)
	}
	return true, nil
}
