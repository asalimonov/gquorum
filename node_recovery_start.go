package gquorum

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoveryexec"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

// prepareRecoveryStart runs after exclusive roots and before any B acquisition.
func (n *node) prepareRecoveryStart(ctx context.Context) error {
	journal, err := n.storageOwner.PendingStartup(ctx)
	if err != nil {
		return err
	}
	if journal == nil {
		return n.adoptHistoricalRecoveryRules(ctx)
	}
	manifest := journal.View()
	switch manifest.Phase {
	case model.Approved, model.Files, model.Cancelling, model.Archiving:
		return fault.New(fault.Unavailable, "recovery_pending", nil)
	default:
	}
	if err := n.validateRecoveryStart(manifest); err != nil {
		return err
	}
	if manifest.Phase == model.Importing || manifest.Phase == model.Validating {
		if err := n.requireRecoverySuccessor(); err != nil {
			return err
		}
	}
	if manifest.SuccessorStartedAt == nil {
		if err := recoveryexec.VerifyArchived(ctx, n.storageOwner, journal); err != nil {
			return err
		}
		if err := n.requireNoRecoveryDestination(ctx); err != nil {
			return err
		}
		if err := journal.MarkSuccessorStarted(ctx, n.startedAt); err != nil {
			return err
		}
		manifest = journal.View()
	}
	n.mu.Lock()
	n.recoveryJournal, n.recoveryManifest = journal, &manifest
	n.mu.Unlock()
	n.metaOpts.Limits = manifest.Rules.Limits
	if manifest.Request.Mode == model.Rebuild {
		n.metaOpts.Recovery = &manifest
		policy := topology.DefaultVoterConfig()
		policy.TargetVoters = 1
		policy.DistinctVoterLabel = ""
		n.metaOpts.VoterConfig = &policy
	}
	return nil
}

func (n *node) validateRecoveryStart(manifest model.LocalManifest) error {
	if manifest.Identity.ClusterID != n.opts.Identity.ClusterID ||
		manifest.Identity.NodeName != n.opts.Identity.NodeName ||
		manifest.Paths != n.storageOwner.Paths() {
		return fault.ErrIdentityMismatch
	}
	if n.limitsExplicit && n.opts.Limits != manifest.Rules.Limits {
		return fault.ErrIdentityMismatch
	}
	if n.opts.Loads.Storage.Store != nil || n.opts.Loads.Storage.Retirement != nil {
		return fault.ErrUnsupported
	}
	switch manifest.Request.Mode {
	case model.Rebuild:
		return n.validateRebuildStart(manifest)
	case model.ResetForJoin:
		if manifest.Phase != model.JoinReady {
			return fault.ErrOperationAuthorization
		}
		if !n.opts.Bootstrap.Join || len(n.opts.Bootstrap.Founders) != 0 || len(n.opts.Bootstrap.JoinSeeds) == 0 ||
			n.opts.Identity.MemberID != 0 {
			return fault.ErrIdentityMismatch
		}
	default:
		return fault.ErrUnsupported
	}
	return nil
}

func (n *node) validateRebuildStart(manifest model.LocalManifest) error {
	if manifest.Phase != model.AwaitingStart && manifest.Phase != model.Importing &&
		manifest.Phase != model.Validating {
		return fault.New(fault.OperationAuthorization, "recovery_pending", nil)
	}
	founders := n.opts.Bootstrap.Founders
	if n.opts.Bootstrap.Join || len(founders) != 1 || founders[0].Node.Name != manifest.Identity.NodeName ||
		founders[0].MemberID != manifest.Identity.MemberID || founders[0].PinnedLearner ||
		n.opts.Identity.MemberID != manifest.Identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	if n.opts.Loads.Factory == nil && len(manifest.Approved.LoadGroups()) > 0 {
		return fault.ErrNoController
	}
	return nil
}

func (n *node) requireNoRecoveryDestination(ctx context.Context) error {
	for _, name := range []string{metaDBFilename, authorityDBFilename} {
		exists, err := n.storageOwner.ArtifactExists("system", name)
		if err != nil {
			return err
		}
		if exists {
			return fault.New(fault.Conflict, "successor exists without durable recovery marker", nil)
		}
	}
	if n.opts.Loads.Storage.WAL != nil {
		exists, err := n.storageOwner.HasLiveLoadEvidence(ctx)
		if err != nil {
			return err
		}
		if exists {
			return fault.New(fault.Conflict, "load successor exists without durable recovery marker", nil)
		}
	}
	return ctx.Err()
}

func (n *node) recoveryMutationAllowed() error {
	n.mu.RLock()
	manifest, meta := n.recoveryManifest, n.meta
	n.mu.RUnlock()
	if manifest != nil && manifest.Request.Mode == model.Rebuild {
		if meta == nil {
			return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
		}
		if meta.RecoveryInProgress() {
			return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
		}
	}
	return nil
}

func (n *node) adoptHistoricalRecoveryRules(ctx context.Context) error {
	records, err := recoverydisk.List(ctx, n.storageOwner.Paths())
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Phase != model.Complete && record.Phase != model.Joined {
			continue
		}
		if record.Identity.ClusterID != n.opts.Identity.ClusterID ||
			record.Identity.NodeName != n.opts.Identity.NodeName {
			continue
		}
		if n.limitsExplicit && n.opts.Limits != record.Rules.Limits {
			return fault.ErrIdentityMismatch
		}
		n.metaOpts.Limits = record.Rules.Limits
		return nil
	}
	return nil
}

func (n *node) requireRecoverySuccessor() error {
	for _, name := range []string{metaDBFilename, authorityDBFilename} {
		exists, err := n.storageOwner.ArtifactExists("system", name)
		if err != nil {
			return err
		}
		if !exists {
			return fault.New(fault.Unreadable, "established recovery successor identity is missing", nil)
		}
	}
	return nil
}

// Resume only the journaled interval before the first metadata acquisition.
// Freshness of unpinned metadata was independently proved by inspectAuthority.
func (n *node) resumeRecoveryAuthority() (bool, error) {
	manifest := n.recoveryManifest
	if manifest == nil || manifest.Request.Mode != model.Rebuild || manifest.Phase != model.AwaitingStart ||
		manifest.SuccessorStartedAt == nil {
		return false, nil
	}
	reader, err := authoritystore.OpenReadOnly(n.ctx, n.opts.authorityDBPath(), manifest.Resources.MaxMetadataBytes)
	if err != nil {
		return true, err
	}
	evidence, readErr := reader.Evidence(n.ctx)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return true, err
	}
	if evidence.Identity != manifest.Identity || evidence.MetadataAcquired || evidence.AdmissionID != "" ||
		len(evidence.Fences) != 0 || len(evidence.Effects) != 0 || len(evidence.Collections) != 0 {
		return true, fault.New(fault.Unreadable, "partial recovery authority is not fresh accepted B evidence", nil)
	}
	if err := n.inspectFreshLoadStorage(); err != nil {
		return true, err
	}
	identity := manifest.Identity
	identity.InstanceID = n.opts.Identity.InstanceID
	n.authorityFreshness = authoritystore.RetainedEvidence
	return true, n.acquireAuthority(identity)
}

func (n *node) resumePinnedRecoveryFounding() (bool, error) {
	manifest := n.recoveryManifest
	if manifest == nil || manifest.Request.Mode != model.Rebuild || manifest.Phase != model.AwaitingStart {
		return false, nil
	}
	onlyPin, err := metastore.StoreHasOnlyIdentity(n.opts.metaDBPath(), manifest.Identity)
	if err != nil || !onlyPin {
		return false, err
	}
	resumed, err := n.resumeRecoveryAuthority()
	if resumed && err == nil {
		n.metaOpts.RecoveryResumeFounding = true
	}
	return resumed, err
}
