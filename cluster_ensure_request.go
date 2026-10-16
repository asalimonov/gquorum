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
	Approve(context.Context, uint64) (topology.OperatorID, error)
	Get(context.Context, uint64) (topology.Document, error)
}

type ensureRequest struct {
	Spec            GroupSpec
	Options         EnsureOptions
	ExpectedVersion uint64
	HeadVersion     uint64
}

func (r *ensureRequest) step(ctx context.Context, c ensureMutations,
	view topology.ReadView) (ensureResult, error) {
	row := legacyEnsureProgress(view, r.Spec.ID)
	known, err := ensureKnownGroup(r.Spec, row)
	if known.Done || err != nil || row.Approved != nil {
		return known, err
	}
	if r.ExpectedVersion != 0 && r.HeadVersion != view.Latest.Version {
		return ensureResult{}, r.create(ctx, c, view.Latest.Version)
	}
	if view.Latest.Version != view.Target.Version {
		return ensureResult{}, r.approve(ctx, c, view)
	}
	return ensureResult{}, r.create(ctx, c, view.Latest.Version)
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
	parent := view.Target
	if parent.Version != view.Latest.ParentVersion {
		var err error
		parent, err = c.Get(ctx, view.Latest.ParentVersion)
		if err != nil {
			return err
		}
	}
	if parent.Version != view.Target.Version || !ownCreation(view.Latest, parent, r.Spec, r.Options) {
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

// Task78 replaces this schema3 diagnostic with the requested group's operation
// block. A failed attempt is returned, never implicitly retried by EnsureGroup.
func (c *clusterView) legacyEnsureFailure(ctx context.Context, view topology.ReadView,
	group string) (ensureResult, error) {
	service, err := c.node.service(ctx, readService)
	if err != nil {
		return ensureResult{}, err
	}
	status, err := service.CachedStatus(ctx)
	if err != nil {
		return ensureResult{}, err
	}
	op := status.Operator
	if op == nil || op.Version != view.Target.Version || op.GroupID != group || op.State != topology.OperatorFailed {
		return ensureResult{}, nil
	}
	code := fault.Code(op.ErrorCode)
	if code == "" {
		code = fault.Unavailable
	}
	message := fmt.Sprintf("operation %s failed for group %q: %s", op.ID, group, op.Error)
	return ensureResult{Done: true}, fault.New(code, message, nil)
}
