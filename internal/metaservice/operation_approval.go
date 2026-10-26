package metaservice

import (
	"context"
	"encoding/json"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// Approve approves staged intent once; it never retries an existing failed attempt.
func (g *Group) Approve(ctx context.Context, version uint64) (topology.OperationID, error) {
	if err := g.guardRecoveryMutation(); err != nil {
		return "", err
	}
	return g.applyOperation(ctx, applyVersionRequest{Kind: "approve", Version: version})
}

// RetryOperation derives one successor to the exact retained failed attempt.
func (g *Group) RetryOperation(ctx context.Context, previous topology.OperationID) (topology.OperationID, error) {
	if g.recoveryPending() {
		state := g.state().RecoveryState()
		operation, err := g.state().Operation(previous)
		if err != nil || state == nil || operation.Version != state.Version {
			return "", fault.New(fault.OperationAuthorization, "recovery_in_progress", err)
		}
	}
	return g.applyOperation(ctx, applyVersionRequest{Kind: "retry", Previous: previous})
}

func (g *Group) applyOperation(ctx context.Context, request applyVersionRequest) (topology.OperationID, error) {
	if err := request.validate(); err != nil {
		return "", err
	}
	if err := g.ensureRunning(ctx); err != nil {
		return "", err
	}
	if !g.IsLeader() {
		return forwardOperation(ctx, request, g.callLeader)
	}
	return g.applyOperationLocal(ctx, request)
}

func forwardOperation(ctx context.Context, request applyVersionRequest,
	call func(context.Context, string, any) ([]byte, error)) (topology.OperationID, error) {
	data, err := call(ctx, "ApplyVersion", request)
	if err != nil {
		return "", err
	}
	var id topology.OperationID
	decodeErr := json.Unmarshal(data, &id)
	if id == "" || decodeErr != nil {
		return "", fault.ErrUnreadable
	}
	return id, nil
}

func (g *Group) applyOperationLocal(ctx context.Context, request applyVersionRequest) (topology.OperationID,
	error) {
	if !g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	query := metaengine.QueryRequest{Kind: "approval_plan", Version: request.Version}
	if request.Kind == "retry" {
		query = metaengine.QueryRequest{Kind: "retry_plan", OperationID: request.Previous}
	}
	read, err := g.capturePlan(ctx, query, g.readPlan)
	if err != nil || read.Accepted != "" {
		return read.Accepted, err
	}
	proposal, err := g.operationProposal(read, request)
	if err != nil {
		return "", err
	}
	if !g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	result, err := g.propose(ctx, proposal)
	if err != nil {
		return "", err
	}
	g.controller.Signal()
	return result.OperationID, nil
}

func (g *Group) operationProposal(read metaengine.PlanRead, request applyVersionRequest) (any, error) {
	if request.Kind == "retry" {
		for _, scope := range read.Scopes {
			if err := g.validateOperationRead(read, scope.GroupID); err != nil {
				return nil, err
			}
		}
		return metaengine.RetryOperationProposal{Previous: request.Previous,
			Scopes: read.Scopes, At: time.Now().UTC(), ProposalID: newProposalID()}, nil
	}
	plan, err := g.planOperation(read)
	if err != nil {
		return nil, err
	}
	return metaengine.BeginOperationProposal{Version: request.Version, Plan: plan, Scopes: read.Scopes,
		Actor: g.currentIdentity().NodeName, Origin: read.Target.Origin, At: g.approvalTime(read.Target),
		ProposalID: newProposalID(), DeletionSemantics: metaengine.DeletionSemanticsDetached}, nil
}

func (g *Group) approvalTime(desired topology.Document) time.Time {
	now := time.Now().UTC()
	if desired.Version == 1 && desired.Origin == topology.OriginFounding {
		return g.recoveryFoundingTime(now)
	}
	return now
}
