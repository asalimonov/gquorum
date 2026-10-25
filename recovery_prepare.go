package gquorum

import (
	"context"
	"errors"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoveryinspect"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

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
