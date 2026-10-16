package metaservice

import (
	"context"
	"crypto/rand"
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// TransferLeader initiates one exact group transfer and records its applied intent.
// Cancellation ends the wait; retry joins any unfinished identical grant.
func (g *Group) TransferLeader(ctx context.Context, id, name string) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	request := topology.AuthorizedStep{GroupID: id, NodeName: name, Kind: topology.StepTransferLeader}
	read, err := g.captureTransfer(ctx, request)
	if err != nil {
		return err
	}
	action, err := transferAction(read, request)
	if err != nil {
		return err
	}
	result, err := g.propose(ctx, metaengine.GrantTransferProposal{
		ExpectedActiveVersion: action.Version, GroupID: id, NodeName: name,
		ReplicaID: action.ReplicaID, RequestID: rand.Text()})
	if err != nil {
		return err
	}
	if result.Grant == nil || result.Grant.Action == nil || *result.Grant.Action != action ||
		result.Grant.Validate() != nil {
		return fault.ErrUnreadable
	}
	return g.completeTransfer(ctx, *result.Grant)
}

// The same grant survives recipient elections and fence/intent waits. Once
// issued it needs an applied receipt even if the destination already leads;
// the native engine records the intent but never transfers leadership to itself.
func (g *Group) completeTransfer(ctx context.Context, grant topology.AuthorityGrant) error {
	_, err := fault.Retry(ctx, fault.RetryOptions{}, func(ctx context.Context) (struct{}, error) {
		err := g.dispatchTransfer(ctx, grant)
		if errors.Is(err, fault.ErrAuthorityPending) {
			err = fault.ErrUnavailable
		}
		return struct{}{}, err
	})
	return err
}

func (g *Group) dispatchTransfer(ctx context.Context, grant topology.AuthorityGrant) error {
	read, err := g.captureTransfer(ctx, *grant.Action)
	if err != nil {
		return err
	}
	target, err := transferRecipient(read, grant)
	if err != nil {
		return err
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: grant.Key.GroupID}
	receipt, callErr := (groupControllers{g}).DispatchEffect(ctx, grant, resource, target)
	if receipt.Key == (topology.AuthorityKey{}) && callErr != nil {
		return callErr
	}
	if receipt.Resource != resource || receipt.Validate(grant) != nil {
		return fault.ErrOperationAuthorization
	}
	if err := g.RecordEffect(ctx, receipt); err != nil {
		return err
	}
	return callErr
}

func (g *Group) captureTransfer(ctx context.Context, request topology.AuthorizedStep) (metaengine.PlanRead, error) {
	query := metaengine.QueryRequest{Kind: "transfer_plan", Key: request.GroupID}
	before, err := g.legacyTransferPlan(ctx, query)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	if _, err := g.leaderTransferTarget(ctx, before.Target, request.GroupID, request.NodeName); err != nil {
		return metaengine.PlanRead{}, err
	}
	if _, err := transferAction(before, request); err != nil {
		return metaengine.PlanRead{}, err
	}
	// Recapture after health callbacks as well as native I/O. No callback wait may
	// turn the earlier topology observation into authority for the proposal.
	read, err := g.capturePlan(ctx, query, g.legacyTransferPlan)
	if err != nil {
		return read, err
	}
	action, err := transferAction(read, request)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	return read, validateTransferNative(read, action)
}

// Task78 switches this read to readPlan. This adapter uses real schema3 Active
// evidence; the unchanged schema3 grant handler retains its global guards.
func (g *Group) legacyTransferPlan(ctx context.Context, query metaengine.QueryRequest) (metaengine.PlanRead, error) {
	input, err := g.state().PlanningInput(ctx, g.state().Target(), query.Native)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	row := input.Groups[query.Key]
	return metaengine.PlanRead{Input: input, Target: input.Approved.Clone(),
		Scopes: []metaengine.ScopeExpectation{{GroupID: query.Key, SourceVersion: row.ConvergedVersion,
			Owner: row.OperationID}}}, nil
}

func transferAction(read metaengine.PlanRead, request topology.AuthorizedStep) (topology.AuthorizedStep, error) {
	group, err := unownedProvenGroup(read.Input, request.GroupID)
	if err != nil {
		return topology.AuthorizedStep{}, err
	}
	row := read.Input.Groups[request.GroupID]
	node := read.Target.Node(request.NodeName)
	if node == nil || node.State != topology.NodeActive {
		return topology.AuthorizedStep{}, fault.ErrInvalid
	}
	action := topology.AuthorizedStep{Kind: topology.StepTransferLeader, GroupID: request.GroupID,
		NodeName: request.NodeName, Version: row.ConvergedVersion}
	for _, replica := range group.Replicas {
		if replica.NodeName == request.NodeName && replica.Role == topology.ReplicaVoter {
			action.ReplicaID = replica.ReplicaID
		}
	}
	if action.ReplicaID == 0 {
		return topology.AuthorizedStep{}, fault.ErrInvalid
	}
	if request.Version != 0 && request != action {
		return topology.AuthorizedStep{}, fault.ErrVersionConflict
	}
	return action, nil
}

func validateTransferNative(read metaengine.PlanRead, action topology.AuthorizedStep) error {
	native := read.Input.Native[action.GroupID]
	status := read.Input.Groups[action.GroupID].Observation
	if status == nil || !status.Found || status.LeaderID == 0 || native.Applied == 0 {
		return fault.ErrUnavailable
	}
	cs := native.ConfState
	if len(cs.VotersOutgoing) != 0 || len(cs.LearnersNext) != 0 || cs.AutoLeave ||
		!slices.Contains(cs.Voters, action.ReplicaID) || !slices.Contains(cs.Voters, status.LeaderID) {
		return fault.ErrUnsafeMembership
	}
	if status.HealthKnown && !status.LiveVoters[action.ReplicaID] {
		return fault.ErrUnsafeMembership
	}
	return nil
}

func transferRecipient(read metaengine.PlanRead, grant topology.AuthorityGrant) (topology.AuthorityTarget, error) {
	leader := read.Input.Groups[grant.Key.GroupID].Observation.LeaderID
	var selected topology.AuthorityTarget
	for _, target := range grant.Targets {
		if target.ReplicaID != leader {
			continue
		}
		if selected.ReplicaID != 0 && selected != target {
			return topology.AuthorityTarget{}, fault.ErrIdentityMismatch
		}
		selected = target
	}
	if selected.ReplicaID == 0 {
		return selected, fault.ErrOperationAuthorization
	}
	return selected, nil
}
