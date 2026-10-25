package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
)

// Journal serializes durable revisions of one immutable accepted operation.
// Its caller retains the Owner until all journal and application work has joined.
type Journal struct {
	owner *Owner
	mu    sync.Mutex
	value model.LocalManifest
}

// Create publishes Approved without moving source files or invoking applications.
func Create(ctx context.Context, owner *Owner, input model.AcceptedPlan,
	acceptance model.Acceptance) (*Journal, error) {
	owner.journalMu.Lock()
	defer owner.journalMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owner.CheckRoots(); err != nil {
		return nil, err
	}
	accepted := model.CanonicalAccepted(input)
	resources, err := model.ResolveResources(accepted.Resources)
	if err != nil {
		return nil, err
	}
	accepted.Resources = resources
	if accepted.Paths != owner.Paths() {
		return nil, fault.ErrIdentityMismatch
	}
	for _, issue := range accepted.Issues {
		if issue.Blocking {
			return nil, fault.ErrInvalid
		}
	}
	records, err := List(ctx, owner.Paths())
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.Request.RecoveryID == accepted.Request.RecoveryID {
			return reopenAccepted(ctx, owner, record, acceptedApproval{plan: accepted, acceptance: acceptance})
		}
		if !terminalPhase(record.Phase) {
			return nil, fault.New(fault.Conflict, "another recovery is pending", nil)
		}
	}
	if err := owner.cleanupApprovals(ctx); err != nil {
		return nil, err
	}
	return publishApproved(ctx, owner, acceptedApproval{plan: accepted, acceptance: acceptance})
}

func publishApproved(ctx context.Context, owner *Owner, approval acceptedApproval) (*Journal, error) {
	accepted, acceptance := approval.plan, approval.acceptance
	at := owner.clock()
	name, err := model.BackupName(at, accepted.Request.RecoveryID)
	if err != nil {
		return nil, err
	}
	r := newManifest(accepted, acceptance, at, name)
	if err := model.ValidateJournalBudget(r); err != nil {
		return nil, err
	}
	data, err := model.EncodeManifest(r)
	if err != nil {
		return nil, err
	}
	relative := manifestRelative(name)
	j := &Journal{owner: owner, value: r}
	if err := owner.publishApproval(name, data); err != nil {
		var actual model.LocalManifest
		readErr := readRecordFrom(owner.system.root, relative, &actual, model.ManifestMaxBytes)
		if readErr == nil {
			j.value = actual
			return j, err
		}
		if errors.Is(readErr, os.ErrNotExist) {
			return nil, err
		}
		return nil, errors.Join(err, readErr)
	}
	return j, j.ensureCurrent(ctx)
}

type acceptedApproval struct {
	plan       model.AcceptedPlan
	acceptance model.Acceptance
}

func reopenAccepted(ctx context.Context, owner *Owner, r model.LocalManifest,
	approval acceptedApproval) (*Journal, error) {
	if r.Phase == model.RollingBack || r.Phase == model.RolledBack {
		return nil, fault.ErrUnsupported
	}
	if r.Phase == model.Cancelled {
		return nil, fault.New(fault.Conflict, "cancelled recovery ID is reserved", nil)
	}
	want := newManifest(approval.plan, approval.acceptance, r.AcceptedAt, r.BackupName)
	if err := model.SameAcceptance(r, want); err != nil {
		return nil, err
	}
	j := &Journal{owner: owner, value: r}
	if !terminalPhase(r.Phase) {
		return j, j.ensureCurrent(ctx)
	}
	return j, SyncParents(owner.system.root, manifestRelative(r.BackupName))
}

// Open resumes an exact operation while refusing invalid or conflicting namespaces.
func Open(ctx context.Context, owner *Owner, id string) (*Journal, error) {
	owner.journalMu.Lock()
	defer owner.journalMu.Unlock()
	if err := owner.CheckRoots(); err != nil {
		return nil, err
	}
	if err := model.ValidateRecoveryID(id); err != nil {
		return nil, err
	}
	records, err := List(ctx, owner.Paths())
	if err != nil {
		return nil, err
	}
	var found *model.LocalManifest
	pending := 0
	for i := range records {
		if !terminalPhase(records[i].Phase) {
			pending++
		}
		if records[i].Request.RecoveryID == id {
			found = &records[i]
		}
	}
	if pending > 1 {
		return nil, fault.New(fault.Conflict, "conflicting pending recoveries", nil)
	}
	if found == nil {
		return nil, fault.ErrHistoryUnavailable
	}
	if found.Phase == model.RollingBack || found.Phase == model.RolledBack {
		return nil, fault.ErrUnsupported
	}
	if found.Paths != owner.Paths() {
		return nil, fault.ErrIdentityMismatch
	}
	if err := SyncParents(owner.system.root, manifestRelative(found.BackupName)); err != nil {
		return nil, err
	}
	j := &Journal{owner: owner, value: *found}
	if !terminalPhase(found.Phase) {
		return j, j.ensureCurrent(ctx)
	}
	return j, nil
}

// View returns an owned observation. It grants no permission to edit source data.
func (j *Journal) View() model.LocalManifest {
	j.mu.Lock()
	defer j.mu.Unlock()
	return model.CloneManifest(j.value)
}

// Paths returns canonical configuration bound to the held root leases.
func (owner *Owner) Paths() model.Paths { return owner.paths }

func (owner *Owner) clock() time.Time {
	if owner.now != nil {
		return owner.now().UTC()
	}
	return time.Now().UTC()
}
func manifestRelative(name string) string { return filepath.Join(Namespace, name, "manifest.json") }
func (j *Journal) write(kind, relative string, data []byte, replace bool) error {
	options := atomicOptions{tempDir: filepath.Join(Namespace, j.value.BackupName), replace: replace,
		boundary: func(point string) error {
			return atBoundary(j.owner.boundary, kind+"."+point)
		}}
	return writeAtomic(j.owner.system.root, relative, data, options)
}

func (j *Journal) ensureCurrent(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pointer := model.Current{Schema: model.SchemaVersion,
		RecoveryID: j.value.Request.RecoveryID, PlanDigest: j.value.PlanDigest, BackupName: j.value.BackupName}
	data, err := model.EncodeCanonical(pointer)
	if err != nil {
		return err
	}
	return j.write("current", filepath.Join(Namespace, "current.json"), data, true)
}
