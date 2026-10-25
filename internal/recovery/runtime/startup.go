//revive:disable:package-naming Recovery scopes this runtime; callers import it as recoveryruntime.
package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/metastore"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	recoveryexec "github.com/asalimonov/gquorum/internal/recovery/exec"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
)

// StartupConfig binds startup checks to already-resolved Node metadata options.
// Meta is owned by the startup caller and updated before any service is acquired.
type StartupConfig struct {
	Meta                                                        *metaservice.Options
	LimitsExplicit, BorrowedStorage, LoadFactory, WALConfigured bool
	StartedAt                                                   time.Time
}
type startupChecks struct {
	owner  *recoverydisk.Owner
	config StartupConfig
}

// PrepareStartup validates the accepted operation and syncs its marker before B I/O.
func PrepareStartup(ctx context.Context, owner *recoverydisk.Owner,
	config StartupConfig) (*recoverydisk.Journal, error) {
	c := startupChecks{owner: owner, config: config}
	journal, err := owner.PendingStartup(ctx)
	if err != nil {
		return nil, err
	}
	if journal == nil {
		return nil, c.adoptHistoricalRecoveryRules(ctx)
	}
	manifest := journal.View()
	switch manifest.Phase {
	case model.Approved, model.Files, model.Cancelling, model.Archiving:
		return nil, fault.New(fault.Unavailable, "recovery_pending", nil)
	}
	if err := c.validateRecoveryStart(manifest); err != nil {
		return nil, err
	}
	if manifest.Phase == model.Importing || manifest.Phase == model.Validating {
		if err := c.requireRecoverySuccessor(); err != nil {
			return nil, err
		}
	}
	if manifest.SuccessorStartedAt == nil {
		if err := recoveryexec.VerifyArchived(ctx, owner, journal); err != nil {
			return nil, err
		}
		if err := c.requireNoRecoveryDestination(ctx); err != nil {
			return nil, err
		}
		if err := journal.MarkSuccessorStarted(ctx, config.StartedAt); err != nil {
			return nil, err
		}
		manifest = journal.View()
	}
	config.Meta.Limits = manifest.Rules.Limits
	if manifest.Request.Mode == model.Rebuild {
		config.Meta.Recovery = &manifest
		policy := topology.DefaultVoterConfig()
		policy.TargetVoters = 1
		policy.DistinctVoterLabel = ""
		config.Meta.VoterConfig = &policy
	}
	return journal, nil
}
func (c startupChecks) validateRecoveryStart(manifest model.LocalManifest) error {
	if manifest.Identity.ClusterID != c.config.Meta.Identity.ClusterID ||
		manifest.Identity.NodeName != c.config.Meta.Identity.NodeName ||
		manifest.Paths != c.owner.Paths() {
		return fault.ErrIdentityMismatch
	}
	if c.config.LimitsExplicit && c.config.Meta.Limits != manifest.Rules.Limits {
		return fault.ErrIdentityMismatch
	}
	if c.config.BorrowedStorage {
		return fault.ErrUnsupported
	}
	switch manifest.Request.Mode {
	case model.Rebuild:
		return c.validateRebuildStart(manifest)
	case model.ResetForJoin:
		if manifest.Phase != model.JoinReady {
			return fault.ErrOperationAuthorization
		}
		if !c.config.Meta.Join || len(c.config.Meta.Founders) != 0 || len(c.config.Meta.JoinSeeds) == 0 ||
			c.config.Meta.Identity.MemberID != 0 {
			return fault.ErrIdentityMismatch
		}
	default:
		return fault.ErrUnsupported
	}
	return nil
}

func (c startupChecks) validateRebuildStart(manifest model.LocalManifest) error {
	if manifest.Phase != model.AwaitingStart && manifest.Phase != model.Importing &&
		manifest.Phase != model.Validating {
		return fault.New(fault.OperationAuthorization, "recovery_pending", nil)
	}
	founders := c.config.Meta.Founders
	if c.config.Meta.Join || len(founders) != 1 || founders[0].Node.Name != manifest.Identity.NodeName ||
		founders[0].MemberID != manifest.Identity.MemberID || founders[0].PinnedLearner ||
		c.config.Meta.Identity.MemberID != manifest.Identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	if !c.config.LoadFactory && len(manifest.Approved.LoadGroups()) > 0 {
		return fault.ErrNoController
	}
	return nil
}

func (c startupChecks) requireNoRecoveryDestination(ctx context.Context) error {
	for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
		exists, err := c.owner.ArtifactExists("system", name)
		if err != nil {
			return err
		}
		if exists {
			return fault.New(fault.Conflict, "successor exists without durable recovery marker", nil)
		}
	}
	if c.config.WALConfigured {
		exists, err := c.owner.HasLiveLoadEvidence(ctx)
		if err != nil {
			return err
		}
		if exists {
			return fault.New(fault.Conflict, "load successor exists without durable recovery marker", nil)
		}
	}
	return ctx.Err()
}

func (c startupChecks) adoptHistoricalRecoveryRules(ctx context.Context) error {
	records, err := recoverydisk.List(ctx, c.owner.Paths())
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Phase != model.Complete && record.Phase != model.Joined {
			continue
		}
		if record.Identity.ClusterID != c.config.Meta.Identity.ClusterID ||
			record.Identity.NodeName != c.config.Meta.Identity.NodeName {
			continue
		}
		if c.config.LimitsExplicit && c.config.Meta.Limits != record.Rules.Limits {
			return fault.ErrIdentityMismatch
		}
		c.config.Meta.Limits = record.Rules.Limits
		return nil
	}
	return nil
}

func (c startupChecks) requireRecoverySuccessor() error {
	for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
		exists, err := c.owner.ArtifactExists("system", name)
		if err != nil {
			return err
		}
		if !exists {
			return fault.New(fault.Unreadable, "established recovery successor identity is missing", nil)
		}
	}
	return nil
}

// CheckResumeAuthority validates exact pre-acquisition B evidence without adopting it.
func CheckResumeAuthority(ctx context.Context, manifest *model.LocalManifest, path string) (bool, error) {
	if manifest == nil || manifest.Request.Mode != model.Rebuild || manifest.Phase != model.AwaitingStart ||
		manifest.SuccessorStartedAt == nil {
		return false, nil
	}
	reader, err := authoritystore.OpenReadOnly(ctx, path, manifest.Resources.MaxMetadataBytes)
	if err != nil {
		return true, err
	}
	evidence, readErr := reader.Evidence(ctx)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return true, err
	}
	if evidence.Identity != manifest.Identity || evidence.MetadataAcquired || evidence.AdmissionID != "" ||
		len(evidence.Fences) != 0 || len(evidence.Effects) != 0 || len(evidence.Collections) != 0 {
		return true, fault.New(fault.Unreadable, "partial recovery authority is not fresh accepted B evidence", nil)
	}
	return true, nil
}

// CheckPinnedFounding recognizes only the exact accepted pre-acquisition pin.
func CheckPinnedFounding(manifest *model.LocalManifest, path string) (bool, error) {
	if manifest == nil || manifest.Request.Mode != model.Rebuild || manifest.Phase != model.AwaitingStart {
		return false, nil
	}
	return metastore.StoreHasOnlyIdentity(path, manifest.Identity)
}
