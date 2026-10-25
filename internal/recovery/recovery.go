// Package recovery orchestrates offline recovery through private storage owners.
package recovery

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	recoveryinspect "github.com/asalimonov/gquorum/internal/recovery/inspect"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/recovery"
)

// Recovery is an immutable configuration handle with no resource lifetime.
// Each operation acquires and releases its own source ownership.
type Recovery struct {
	source  model.SourceOptions
	options recovery.Options
	probe   ProbeOptions
}

// Config supplies source settings without retaining unrelated Node dependencies.
type Config struct {
	Source          model.SourceOptions
	BorrowedStorage bool
	WALConfigured   bool
	Probe           ProbeOptions
}

// New copies configuration without I/O, callbacks or background work.
func New(config Config, options ...recovery.Options) (*Recovery, error) {
	if len(options) > 1 {
		return nil, recoveryRefusal("invalid_request",
			"at most one recovery options value is allowed", fault.ErrInvalid)
	}
	source, err := recoverySourceOptions(config)
	if err != nil {
		return nil, err
	}
	var selected recovery.Options
	if len(options) == 1 {
		selected = options[0]
	}
	if _, err := model.ResolveResources(selected.Resources); err != nil {
		return nil, recoveryRefusal("resource_bound", "invalid recovery resource settings", err)
	}
	return &Recovery{source: source, options: selected, probe: copyRecoveryProbe(config.Probe)}, nil
}

func recoverySourceOptions(config Config) (model.SourceOptions, error) {
	if config.BorrowedStorage {
		// TODO(not planned): recover injected/custom stores; use Node-owned WAL configuration.
		return model.SourceOptions{}, recoveryRefusal("unsupported_source",
			"recovery requires Node-owned WAL configuration", fault.ErrUnsupported)
	}
	valid := func(s string) bool {
		return strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0) && utf8.ValidString(s)
	}
	if !valid(config.Source.Paths.StorageDir) || !valid(config.Source.NodeName) ||
		config.Source.ClusterID != "" && !valid(config.Source.ClusterID) {
		return model.SourceOptions{}, recoveryRefusal("invalid_source",
			"source directory and node name are required; expected cluster must be valid", fault.ErrInvalid)
	}
	source := model.SourceOptions{
		Paths:    model.Paths{StorageDir: config.Source.Paths.StorageDir},
		NodeName: config.Source.NodeName, ClusterID: config.Source.ClusterID,
	}
	if config.WALConfigured {
		walRoot := config.Source.Paths.LoadWALDir
		if !valid(walRoot) || pathWithin(config.Source.Paths.StorageDir, walRoot) {
			return model.SourceOptions{}, recoveryRefusal("invalid_source",
				"system storage must be outside an explicit WAL root", fault.ErrInvalid)
		}
		source.Paths.LoadWALDir = walRoot
	}
	for _, root := range []string{source.Paths.StorageDir, source.Paths.LoadWALDir} {
		for _, part := range strings.Split(root, string(filepath.Separator)) {
			if part == "@gquorum-recovery" {
				return model.SourceOptions{}, recoveryRefusal("invalid_source",
					"private recovery directories cannot be source roots", fault.ErrInvalid)
			}
		}
	}
	return source, nil
}

func recoveryRefusal(code, message string, cause error) error {
	return &recovery.Refusal{Issue: recovery.Issue{
		Code: code, Message: message, Blocking: true, Remedy: recovery.RemedyInvestigate,
	}, Cause: cause}
}

// Inspect reads source evidence and builds a reviewable recovery report.
func (r *Recovery) Inspect(ctx context.Context, input recovery.Request) (plan recovery.Plan, err error) {
	if err := ctx.Err(); err != nil {
		return plan, err
	}
	request, err := recoveryinspect.EffectiveRequest(input)
	if err != nil {
		return plan, recoveryRefusal("invalid_request", "invalid recovery inspection request", err)
	}
	owner, err := recoverydisk.Acquire(ctx, r.source.Paths)
	if err != nil {
		return plan, inspectionRefusal(err, nil)
	}
	defer func() { err = errors.Join(err, releaseRecoveryOwner(owner)) }()
	prior, err := r.priorApproval(ctx, owner, request)
	if err != nil {
		return plan, inspectionRefusal(err, nil)
	}
	if prior != nil {
		accepted := model.AcceptedFromManifest(*prior)
		probeErr := r.probeAccepted(ctx, &accepted)
		report, reportErr := model.PlanFromAccepted(accepted)
		return report, errors.Join(inspectionRefusal(probeErr, accepted.Issues), reportErr)
	}
	accepted, inspectErr := r.inspectOwned(ctx, owner, request)
	plan, err = model.PlanFromAccepted(accepted)
	return plan, errors.Join(inspectionRefusal(inspectErr, accepted.Issues), err)
}

// Approve durably accepts the exact inspected request without moving source data.
func (r *Recovery) Approve(ctx context.Context, request recovery.Request,
	acceptance recovery.Acceptance) (operation recovery.Operation, err error) {
	if err := ctx.Err(); err != nil {
		return operation, err
	}
	if request.RecoveryID == "" || model.ValidateRequest(request) != nil {
		return operation, recoveryRefusal("invalid_request",
			"approve requires the effective inspected request", fault.ErrInvalid)
	}
	if !acceptance.AcknowledgeDataLoss || request.Mode == recovery.Rebuild && !acceptance.OldDeploymentFenced {
		return operation, recoveryRefusal("acceptance_required",
			"potential data loss and old-deployment fencing require explicit acceptance", fault.ErrInvalid)
	}
	owner, err := recoverydisk.Acquire(ctx, r.source.Paths)
	if err != nil {
		return operation, inspectionRefusal(err, nil)
	}
	defer func() { err = errors.Join(err, releaseRecoveryOwner(owner)) }()
	prior, err := r.priorApproval(ctx, owner, request)
	if err != nil {
		return operation, inspectionRefusal(err, nil)
	}
	var accepted model.AcceptedPlan
	if prior != nil {
		accepted = model.AcceptedFromManifest(*prior)
		if err := r.probeAccepted(ctx, &accepted); err != nil {
			return operation, inspectionRefusal(err, accepted.Issues)
		}
	} else {
		accepted, err = r.inspectOwned(ctx, owner, request)
		if err != nil {
			return operation, inspectionRefusal(err, accepted.Issues)
		}
	}
	digest, err := model.DigestAccepted(accepted)
	if err != nil {
		return operation, err
	}
	if digest != acceptance.PlanDigest {
		return operation, recoveryRefusal("source_changed",
			"acceptance does not match exact retained source evidence; inspect again", fault.ErrConflict)
	}
	journal, err := recoverydisk.Create(ctx, owner, accepted, acceptance)
	if journal != nil {
		operation = model.OperationFromManifest(journal.View())
	}
	return operation, inspectionRefusal(err, nil)
}

func (r *Recovery) inspectOwned(ctx context.Context, owner *recoverydisk.Owner,
	request recovery.Request) (model.AcceptedPlan, error) {
	if err := owner.CheckStartup(ctx, owner.Paths()); err != nil {
		return model.AcceptedPlan{Request: request}, err
	}
	accepted, err := recoveryinspect.Inspect(ctx, owner, request, r.options.Resources)
	accepted.FilesRequired = r.options.Files != nil
	if accepted.Source.NodeName != "" && (accepted.Source.NodeName != r.source.NodeName ||
		r.source.ClusterID != "" && accepted.Source.ClusterID != r.source.ClusterID) {
		return accepted, fault.New(fault.IdentityMismatch, "configured source differs from retained identity", nil)
	}
	if err != nil {
		return accepted, err
	}
	if err := recoverydisk.ValidateApprovalShape(accepted); err != nil {
		accepted.Issues = append(accepted.Issues, model.Issue{Code: "invalid_accepted_evidence", Blocking: true,
			Message: "recovery evidence cannot fit or validate its journal and ordinary metadata import",
			Remedy:  model.RemedyInvestigate})
		return accepted, err
	}
	return accepted, r.probeAccepted(ctx, &accepted)
}

func (r *Recovery) priorApproval(ctx context.Context, owner *recoverydisk.Owner,
	request recovery.Request) (*model.LocalManifest, error) {
	records, err := recoverydisk.List(ctx, owner.Paths())
	if err != nil {
		return nil, err
	}
	var prior *model.LocalManifest
	for _, record := range records {
		if record.Request.RecoveryID == request.RecoveryID {
			if err := r.validatePrior(owner, record, request); err != nil {
				return nil, err
			}
			owned := record
			prior = &owned
			continue
		}
		if !recoveryTerminal(record.Phase) {
			return nil, fault.New(fault.Conflict, "another recovery is pending", nil)
		}
	}
	return prior, nil
}

func (r *Recovery) validatePrior(owner *recoverydisk.Owner, prior model.LocalManifest,
	request recovery.Request) error {
	if err := r.validateExecutionOptions(owner, prior); err != nil {
		return err
	}
	if !reflect.DeepEqual(canonicalRecoveryRequest(prior.Request), canonicalRecoveryRequest(request)) {
		return fault.ErrConflict
	}
	if prior.Phase == recovery.Cancelled {
		return fault.New(fault.Conflict, "cancelled recovery ID is reserved", nil)
	}
	if prior.Phase == recovery.RollingBack || prior.Phase == recovery.RolledBack {
		return fault.ErrUnsupported
	}
	return nil
}

func (r *Recovery) validateExecutionOptions(owner *recoverydisk.Owner, prior model.LocalManifest) error {
	if r.source.ClusterID != "" && prior.Source.ClusterID != r.source.ClusterID ||
		prior.Source.NodeName != r.source.NodeName || prior.Paths != owner.Paths() {
		return fault.ErrIdentityMismatch
	}
	if prior.Files.Required != (r.options.Files != nil) {
		return fault.ErrConflict
	}
	for _, pair := range [][2]uint64{
		{r.options.Resources.MaxRecordBytes, prior.Resources.MaxRecordBytes},
		{r.options.Resources.MaxGroupBytes, prior.Resources.MaxGroupBytes},
		{r.options.Resources.MaxMetadataBytes, prior.Resources.MaxMetadataBytes},
	} {
		if pair[0] != 0 && pair[0] != pair[1] {
			return fault.ErrConflict
		}
	}
	return nil
}

func canonicalRecoveryRequest(request recovery.Request) recovery.Request {
	owned := request
	owned.RequiredGroups = slices.Clone(request.RequiredGroups)
	slices.Sort(owned.RequiredGroups)
	if len(owned.RequiredGroups) == 0 {
		owned.RequiredGroups = nil
	}
	return owned
}

func recoveryTerminal(phase recovery.Phase) bool {
	return phase == recovery.Cancelled || phase == recovery.Complete || phase == recovery.Joined
}

func releaseRecoveryOwner(owner *recoverydisk.Owner) error {
	if err := owner.Close(); err != nil {
		return errors.Join(err, owner.Close())
	}
	return nil
}

func inspectionRefusal(err error, issues []recovery.Issue) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, model.ErrResourceBound) {
		for _, issue := range issues {
			if issue.Code == "resource_bound" {
				return &recovery.Refusal{Issue: issue, Cause: err}
			}
		}
		issue := model.InputIssue(model.Issue{Blocking: true, Remedy: model.RemedyInvestigate}, err)
		return &recovery.Refusal{Issue: issue, Cause: err}
	}
	for _, issue := range issues {
		if issue.Blocking {
			return &recovery.Refusal{Issue: issue, Cause: err}
		}
	}
	if errors.Is(err, fault.ErrUnavailable) {
		return &recovery.Refusal{Issue: recovery.Issue{Code: "source_busy", Blocking: true,
			Message: "source is owned by another operation or is not yet available",
			Remedy:  recovery.RemedyRetry}, Cause: err}
	}
	return recoveryRefusal("inspection_refused", "recovery evidence or request is invalid or conflicting", err)
}

func pathWithin(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
