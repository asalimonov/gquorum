package metaservice

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	internalscheduling "github.com/asalimonov/gquorum/internal/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

func (h schedulingHost) ReadRepair(ctx context.Context, group string) (metaengine.PlanRead, error) {
	query := metaengine.QueryRequest{Kind: "repair_plan", Key: group}
	if group == "" {
		return h.g.readPlan(ctx, query)
	}
	read, err := h.g.capturePlan(ctx, query, h.g.readPlan)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	row := read.Input.Groups[group]
	if row.Observation != nil {
		// Bind only a freshly read native/status pair after metadata recapture.
		// Keep the observation's timestamp; a cache decoration cannot renew it.
		if row.Observation.SourceVersion != 0 && row.Observation.SourceVersion != row.ConvergedVersion ||
			!reflect.DeepEqual(row.Observation.ConfState, read.Input.Native[group].ConfState) {
			return metaengine.PlanRead{}, fault.ErrUnavailable
		}
		row.Observation.SourceVersion = row.ConvergedVersion
		read.Input.Groups[group] = row
	}
	return read, nil
}

func (h schedulingHost) PlanRepair(ctx context.Context, captured metaengine.PlanRead,
	doc topology.Document) (topology.Plan, error) {
	if len(captured.Scopes) != 1 {
		return topology.Plan{}, fault.ErrInvalid
	}
	query := metaengine.QueryRequest{Kind: "repair_plan", Key: captured.Scopes[0].GroupID, Desired: &doc}
	read, err := h.g.capturePlan(ctx, query, h.g.readPlan)
	if err != nil {
		return topology.Plan{}, fmt.Errorf("capture repair plan query for %q: %w", query.Key, err)
	}
	if read.Latest.Version != captured.Latest.Version || read.Input.Approved.Version != captured.Target.Version ||
		!slices.Equal(read.Scopes, captured.Scopes) || !sameRepairNative(read, captured) {
		return topology.Plan{}, fault.ErrVersionConflict
	}
	plan, err := h.g.planOperation(read)
	if err != nil {
		return topology.Plan{}, fmt.Errorf("build scoped repair plan for %q: %w", captured.Scopes[0].GroupID, err)
	}
	return plan, nil
}

func sameRepairNative(a, b metaengine.PlanRead) bool {
	for _, scope := range a.Scopes {
		left, right := a.Input.Native[scope.GroupID], b.Input.Native[scope.GroupID]
		if !reflect.DeepEqual(left.ConfState, right.ConfState) {
			return false
		}
	}
	return true
}

func (h schedulingHost) CommitRepair(ctx context.Context,
	commit internalscheduling.RepairCommit) (topology.OperationID, error) {
	if !h.g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	result, err := h.g.propose(ctx, metaengine.CommitRepairOperationProposal{Doc: commit.Document,
		ExpectedVersion: commit.ExpectedVersion, ExpectedTarget: commit.ExpectedTarget,
		Scopes: commit.Scopes, Plan: commit.Plan, At: commit.Document.CreatedAt, ProposalID: newProposalID()})
	return result.OperationID, err
}

func (h schedulingHost) CommitVoterChange(ctx context.Context, change topology.VoterChange,
	commit internalscheduling.RepairCommit) (topology.OperationID, error) {
	if !h.g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	result, err := h.g.propose(ctx, metaengine.CommitVoterChangeProposal{Change: change.Clone(), Doc: commit.Document,
		ExpectedVersion: commit.ExpectedVersion, ExpectedTarget: commit.ExpectedTarget,
		Scopes: commit.Scopes, Plan: commit.Plan, At: commit.Document.CreatedAt, ProposalID: newProposalID()})
	return result.OperationID, err
}

func (h schedulingHost) Health(ctx context.Context) (map[string]topology.Health, error) {
	if h.g.opts.Health != nil {
		observed, err := h.g.opts.Health.Snapshot(ctx)
		return maps.Clone(observed), err
	}
	read, err := h.ReadRepair(ctx, "")
	if err != nil {
		return nil, err
	}
	core := h.g.engine.Load()
	if core == nil {
		return nil, fault.ErrUnavailable
	}
	live, err := h.g.metaHealth(ctx, core.Status())
	if err != nil {
		return nil, err
	}
	health := make(map[string]topology.Health, len(read.Target.Nodes))
	for _, node := range read.Target.Nodes {
		row := read.Input.Nodes[node.Name]
		if row.Converged == nil || row.MemberID == 0 || row.MemberID != read.Target.MemberID(node.Name) {
			continue
		}
		health[node.Name] = topology.HealthDead
		if live[row.MemberID] {
			health[node.Name] = topology.HealthAlive
		}
	}
	return health, nil
}
