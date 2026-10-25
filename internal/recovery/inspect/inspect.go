package inspect

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
)

// Inspect reconstructs one complete survivor while retaining both root leases.
// Applications, archive moves and destination startup are outside this reader.
func Inspect(ctx context.Context, owner *recoverydisk.Owner, request model.Request,
	resources model.Resources) (accepted model.AcceptedPlan, err error) {
	if err := ctx.Err(); err != nil {
		return model.AcceptedPlan{}, err
	}
	if err := owner.CheckRoots(); err != nil {
		return model.AcceptedPlan{}, err
	}
	resolved, err := model.ResolveResources(resources)
	if err != nil {
		return model.AcceptedPlan{}, err
	}
	if request.RecoveryID == "" || model.ValidateRequest(request) != nil {
		return model.AcceptedPlan{}, fault.ErrInvalid
	}
	state, err := readSource(ctx, owner.Paths(), resolved)
	if err != nil {
		return model.AcceptedPlan{Request: request, Paths: owner.Paths(), Resources: resolved,
			Source: state.identity,
			Issues: []model.Issue{model.InputIssue(model.Issue{Code: "metadata_unreadable", Blocking: true,
				Message: "retained metadata or authority cannot establish a complete source",
				Remedy:  model.RemedyInvestigate}, err)}}, err
	}
	state.owner = owner
	defer func() { err = errors.Join(err, state.close()) }()
	if request.Mode == model.ResetForJoin {
		return Reset(ctx, state, request)
	}
	return Rebuild(ctx, state, request)
}
