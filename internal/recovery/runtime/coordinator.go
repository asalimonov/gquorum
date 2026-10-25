// Package runtime coordinates accepted recovery inside its owning Node.
//
//revive:disable:package-naming Recovery scopes this runtime; callers import it as recoveryruntime.
package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
)

// Config binds one exact accepted startup and borrowed Node-owned ports.
type Config struct {
	Manifest model.LocalManifest
	Journal  *recoverydisk.Journal
	Meta     MetaPort
	Host     HostPort
	OnChange func()
}

// Coordinator owns control work only; applications and stores stay with Node.
type Coordinator struct {
	config                                 Config
	mu                                     sync.Mutex
	ctx                                    context.Context
	cancel                                 context.CancelFunc
	done                                   chan struct{}
	started, stopped, dataReady, published bool
	err                                    error
	gate                                   chan struct{}
	retryPrevious                          topology.OperationID
}

// New validates/copies configuration without callbacks, I/O or goroutines.
func New(config Config) (*Coordinator, error) {
	if config.Journal == nil || config.Meta == nil || config.Manifest.Request.Mode != model.Rebuild ||
		config.Manifest.SuccessorStartedAt == nil {
		return nil, fault.ErrInvalid
	}
	if err := model.ValidateManifest(config.Manifest); err != nil {
		return nil, err
	}
	if config.Host == nil && len(config.Manifest.Approved.LoadGroups()) > 0 {
		return nil, fault.ErrNoController
	}
	config.Manifest = model.CloneManifest(config.Manifest)
	return &Coordinator{config: config, gate: make(chan struct{}, 1)}, nil
}

// Start accepts one control lifetime without waiting for metadata readiness.
func (c *Coordinator) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return fault.ErrStopped
	}
	if c.started {
		return nil
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.done, c.started = make(chan struct{}), true
	go c.run()
	return nil
}

// Stop cancels and joins control work before Node closes metadata or stores.
func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stopped = true
	if c.cancel != nil {
		c.cancel()
	}
	started, done := c.started, c.done
	c.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-done:
		select {
		case c.gate <- struct{}{}:
			<-c.gate
			return nil
		case <-ctx.Done():
			return fault.New(fault.ShutdownTimeout, "recovery publication is still owned", ctx.Err())
		}
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "recovery coordinator is still owned", ctx.Err())
	}
}

// DataReady means replicated import completion was confirmed and journaled.
func (c *Coordinator) DataReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dataReady && !c.stopped
}

// Error reports a retained control failure; it never releases ownership.
func (c *Coordinator) Error() error { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

func (c *Coordinator) run() {
	defer close(c.done)
	err := c.rebuild(c.ctx)
	if err != nil && c.ctx.Err() == nil {
		issue := model.Issue{Code: "recovery_start_failed", Message: err.Error(), Remedy: model.RemedyRetry}
		err = errors.Join(err, c.config.Journal.SetPhase(context.WithoutCancel(c.ctx),
			c.config.Journal.View().Phase, &issue))
	}
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	if c.config.OnChange != nil {
		c.config.OnChange()
	}
}

func (c *Coordinator) rebuild(ctx context.Context) error {
	if err := model.SameAcceptance(c.config.Manifest, c.config.Journal.View()); err != nil {
		return err
	}
	if err := retryControl(ctx, c.config.Meta.WaitRecoveryReplay); err != nil {
		return err
	}
	if err := c.checkIdentity(c.config.Meta.Identity()); err != nil {
		return err
	}
	if c.config.Journal.View().Phase == model.AwaitingStart {
		if err := c.config.Journal.SetPhase(ctx, model.Importing, nil); err != nil {
			return err
		}
	}
	operation, err := fault.Retry(ctx, fault.RetryOptions{}, c.importAccepted)
	if err != nil {
		return err
	}
	record, err := c.readRecovery(ctx)
	if err != nil {
		return err
	}
	if err := c.validateRecord(record); err != nil {
		return err
	}
	if err := c.finishImport(ctx, operation, record); err != nil {
		return err
	}
	c.mu.Lock()
	if !c.stopped && ctx.Err() == nil {
		c.dataReady = true
	}
	c.mu.Unlock()
	return ctx.Err()
}

func (c *Coordinator) importAccepted(ctx context.Context) (topology.OperationID, error) {
	proposal := model.ImportFromManifest(c.config.Manifest)
	operation, err := c.config.Meta.ImportRecovery(ctx, proposal)
	if err == nil {
		return operation, nil
	}
	// Resolve an uncertain response from exact replicated provenance; never approve Latest.
	record, readErr := c.readRecovery(ctx)
	if readErr != nil {
		return "", errors.Join(err, readErr)
	}
	if record == nil {
		return "", err
	}
	if validationErr := c.validateRecord(record); validationErr != nil {
		return "", validationErr
	}
	return topology.MakeOperationID(record.Version, 1), nil
}

func (c *Coordinator) validateRecord(record *model.ImportState) error {
	manifest := c.config.Manifest
	if record == nil || record.Schema != model.SchemaVersion || record.Version != 2 ||
		record.RecoveryID != manifest.Request.RecoveryID || record.PlanDigest != manifest.PlanDigest ||
		record.Source != manifest.Source || record.Survivor != model.ImportFromManifest(manifest).Survivor {
		return fault.ErrIdentityMismatch
	}
	if record.Phase != "importing" && record.Phase != "complete" {
		return fault.ErrOperationAuthorization
	}
	digest, err := model.ImportRequestDigest(model.ImportFromManifest(manifest))
	if err != nil {
		return err
	}
	if digest != record.RequestDigest {
		return fault.ErrConflict
	}
	return validateImportedGroups(record, manifest)
}

func (c *Coordinator) completeMetadataOnly(ctx context.Context, operation topology.OperationID,
	record *model.ImportState) error {
	if record.Phase == "complete" {
		return nil
	}
	if err := c.waitImportOperation(ctx, operation); err != nil {
		return err
	}
	proposal := model.CompleteImportProposal{
		RecoveryID: c.config.Manifest.Request.RecoveryID, PlanDigest: c.config.Manifest.PlanDigest,
		At:         c.config.Manifest.AcceptedAt.Add(time.Nanosecond),
		ProposalID: c.config.Manifest.Request.RecoveryID + ".complete",
	}
	return retryControl(ctx, func(attempt context.Context) error { return c.completeAccepted(attempt, proposal) })
}

func (c *Coordinator) completeAccepted(ctx context.Context, proposal model.CompleteImportProposal) error {
	err := c.config.Meta.CompleteRecovery(ctx, proposal)
	if err == nil {
		return nil
	}
	confirmed, readErr := c.readRecovery(ctx)
	if readErr != nil {
		return errors.Join(err, readErr)
	}
	if validationErr := c.validateRecord(confirmed); validationErr != nil {
		return errors.Join(err, validationErr)
	}
	if confirmed.Phase == "complete" {
		return nil
	}
	return err
}

func (c *Coordinator) checkIdentity(identity topology.Identity) error {
	expected := c.config.Manifest.Identity
	if identity.NodeName != expected.NodeName || identity.ClusterID != expected.ClusterID ||
		identity.MemberID != expected.MemberID || identity.InstanceID == "" {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func (c *Coordinator) finishImport(ctx context.Context, operation topology.OperationID,
	record *model.ImportState) error {
	if c.config.Host != nil {
		if err := c.config.Host.SetRecovery(ctx, record, c.config.Manifest); err != nil {
			return err
		}
	}
	if err := c.replayGroups(ctx, record); err != nil {
		return err
	}
	resumed, err := fault.Retry(ctx, fault.RetryOptions{},
		func(attempt context.Context) (topology.OperationID, error) {
			return c.resumeExistingOperation(attempt, operation)
		})
	if err != nil {
		return err
	}
	if err := c.consumeGroups(ctx, resumed, record); err != nil {
		return err
	}
	if err := c.completeMetadataOnly(ctx, resumed, record); err != nil {
		return err
	}
	confirmed, err := c.readRecovery(ctx)
	if err != nil {
		return err
	}
	if err := c.validateRecord(confirmed); err != nil {
		return err
	}
	if c.config.Host != nil {
		if err := c.config.Host.SetRecovery(ctx, confirmed, c.config.Manifest); err != nil {
			return err
		}
	}
	if c.config.Journal.View().Phase == model.Importing {
		if err := c.config.Journal.SetPhase(ctx, model.Validating, nil); err != nil {
			return err
		}
	}
	return nil
}
