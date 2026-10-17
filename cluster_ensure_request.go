package gquorum

import (
	"context"
	"errors"
	"fmt"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// This is the existing Cluster mutation surface, narrowed for one call's state.
type ensureMutations interface {
	CreateGroup(context.Context, GroupSpec, MutationOptions) (MutationResult, error)
	Approve(context.Context, uint64) (topology.OperationID, error)
	ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error)
}

type ensureRequest struct {
	Spec            GroupSpec
	Options         EnsureOptions
	ExpectedVersion uint64
	HeadVersion     uint64
}

const ensureProgressPageLimit = 1000

func (r *ensureRequest) step(ctx context.Context, c ensureMutations,
	view topology.ReadView) (ensureResult, error) {
	page, err := c.ReadProgress(ctx, topology.ProgressQuery{GroupID: r.Spec.ID})
	if err != nil {
		return ensureResult{}, err
	}
	row := ensureProgressRow(page.Groups, r.Spec.ID)
	known, err := ensureKnownGroup(r.Spec, row)
	if known.Done || err != nil {
		return known, err
	}
	if err := r.checkFailedOperation(ctx, c, row); err != nil {
		return ensureResult{Done: true}, err
	}
	if row.Approved != nil {
		return ensureResult{}, nil // Approved intent is progressing; keep observing it.
	}
	if r.ExpectedVersion != 0 && r.HeadVersion != view.Latest.Version {
		return ensureResult{}, r.create(ctx, c, view.Latest.Version)
	}
	if view.Latest.Version != view.Target.Version {
		return ensureResult{}, r.approve(ctx, c, view)
	}
	return ensureResult{}, r.create(ctx, c, view.Latest.Version)
}

func ensureProgressRow(groups []topology.GroupProgress, id string) topology.GroupProgress {
	for _, row := range groups {
		if row.GroupID == id {
			return row
		}
	}
	return topology.GroupProgress{GroupID: id}
}

func (r *ensureRequest) checkFailedOperation(ctx context.Context, c ensureMutations,
	row topology.GroupProgress) error {
	if row.OperationID != "" {
		page, err := c.ReadProgress(ctx, topology.ProgressQuery{OperationID: row.OperationID})
		if err != nil {
			return err
		}
		return failedEnsureBlock(page.Operations, r.Spec.ID, row.OperationID)
	}
	if row.Approved == nil || row.ConvergedVersion != 0 {
		return nil
	}
	// Terminal failure releases the owner ID; inspect the retained group history.
	query := topology.ProgressQuery{GroupID: r.Spec.ID, Limit: ensureProgressPageLimit}
	for {
		page, err := c.ReadProgress(ctx, query)
		if err != nil {
			return err
		}
		if failed := failedEnsureBlock(page.Operations, r.Spec.ID, ""); failed != nil {
			return failed
		}
		if page.Next == "" {
			return nil
		}
		query.After = page.Next
	}
}

func failedEnsureBlock(operations []topology.OperationStatus, group string, exact topology.OperationID) error {
	for _, operation := range operations {
		if exact != "" && operation.ID != exact {
			continue
		}
		for _, block := range operation.Blocks {
			if block.GroupID != group || block.State != topology.OperationFailed {
				continue
			}
			code := fault.Code(block.ErrorCode)
			if code == "" {
				code = fault.Unavailable
			}
			return fault.New(code,
				fmt.Sprintf("operation %s failed for group %q: %s", operation.ID, group, block.Error), nil)
		}
	}
	return nil
}

func (r *ensureRequest) create(ctx context.Context, c ensureMutations, version uint64) error {
	if r.ExpectedVersion == 0 {
		r.ExpectedVersion = version
	}
	result, err := c.CreateGroup(ctx, r.Spec, MutationOptions{ExpectedVersion: r.ExpectedVersion,
		Actor: r.Options.Actor, Comment: r.Options.Comment})
	if result.Document.Version != 0 {
		r.HeadVersion = result.Document.Version
		if result.RebasedTo != 0 {
			r.HeadVersion = result.RebasedTo
		}
	}
	if err == nil && r.HeadVersion < version {
		return fault.New(fault.Conflict, "accepted creation is no longer the staged head", nil)
	}
	return err
}

// FSM typed replay proves the original intent and its RebasedTo link. Only the
// retained head delta needs inspection here; older rebase documents may be pruned.
func (r *ensureRequest) approve(ctx context.Context, c ensureMutations, view topology.ReadView) error {
	if view.Target.Version != view.Latest.ParentVersion ||
		!ownCreation(view.Latest, view.Target, r.Spec, r.Options) {
		return fault.New(fault.Conflict,
			fmt.Sprintf("unapproved staged topology at version %d", view.Latest.Version), nil)
	}
	_, err := c.Approve(ctx, view.Latest.Version)
	var conflict *fault.Error
	if !errors.As(err, &conflict) {
		return err
	}
	if conflict.Code == fault.VersionConflict && conflict.RebasedTo > view.Latest.Version {
		r.HeadVersion = conflict.RebasedTo
		return nil // The FSM names the successor; inspect its exact delta on the next read.
	}
	return err
}
