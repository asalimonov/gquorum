//go:build functional

package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// Restore a previously converged checkpoint, then exercise reads and repair
// through the real Machine. Founding and receipt protocols have their own suites.
func (h *testHost) restoreConverged(doc topology.Document) {
	h.t.Helper()
	at := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	doc.CreatedAt = at
	var groups, nodes, allocations []map[string]any
	for _, group := range doc.Groups {
		groups = append(groups, map[string]any{"group_id": group.ID, "declaration": group,
			"source_version": doc.Version, "completed_at": at})
		allocations = append(allocations, map[string]any{"group_id": group.ID, "next_replica_id": group.NextReplicaID})
	}
	for _, node := range doc.Nodes {
		nodes = append(nodes, map[string]any{"node_name": node.Name, "declaration": node,
			"source_version": doc.Version, "member_id": doc.MemberID(node.Name), "completed_at": at})
	}
	data, err := json.Marshal(map[string]any{"schema_version": 4,
		"rules":  topology.Rules{SchemaVersion: 1, Limits: h.limits.WithDefaults()},
		"latest": doc, "target": doc, "group_convergence": groups, "node_convergence": nodes,
		"allocations": allocations, "fully_converged": topology.FullyConverged{Version: doc.Version, At: at},
		"applied_index": 1, "applied_term": 1})
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.machine.Restore(h.t.Context(), data); err != nil {
		h.t.Fatal(err)
	}
	h.index = 1
}

func (h *testHost) ReadRepair(ctx context.Context, group string) (metaengine.PlanRead, error) {
	if h.blockViews {
		<-ctx.Done()
		return metaengine.PlanRead{}, ctx.Err()
	}
	if h.viewErr != nil {
		return metaengine.PlanRead{}, h.viewErr
	}
	query := metaengine.QueryRequest{Kind: "repair_plan", Key: group}
	if group != "" {
		h.statusCalls++
		if h.statusErr != nil {
			return metaengine.PlanRead{}, h.statusErr
		}
		status := h.status[group]
		query.Native = make(map[string]topology.NativeState)
		if status.Found && len(status.ConfState.Voters) > 0 {
			query.Native[group] = topology.NativeState{GroupID: group, ReplicaID: status.ConfState.Voters[0],
				Applied: max(1, status.Applied), ConfState: status.Clone().ConfState}
		}
	}
	data, err := json.Marshal(query)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	result, err := h.machine.Query(ctx, data)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	var read metaengine.PlanRead
	if err := json.Unmarshal(result.Data, &read); err != nil {
		return metaengine.PlanRead{}, err
	}
	if group != "" {
		row := read.Input.Groups[group]
		status := h.status[group].Clone()
		status.SourceVersion = row.ConvergedVersion
		row.Observation = &status
		read.Input.Groups[group] = row
	}
	return read, nil
}

func (h *testHost) PlanRepair(ctx context.Context, read metaengine.PlanRead, doc topology.Document) (topology.Plan, error) {
	if err := ctx.Err(); err != nil {
		return topology.Plan{}, err
	}
	if h.planErr != nil {
		return topology.Plan{}, h.planErr
	}
	return topology.BuildPlan(read.Input, doc, topology.Rules{SchemaVersion: 1, Limits: h.limits.WithDefaults()})
}

func (h *testHost) CommitRepair(ctx context.Context, c RepairCommit) (topology.OperationID, error) {
	if h.beforeCommit != nil {
		h.beforeCommit()
	}
	result, err := h.apply(ctx, metaengine.CommitRepairOperationProposal{Doc: c.Document,
		ExpectedVersion: c.ExpectedVersion, ExpectedTarget: c.ExpectedTarget, Scopes: c.Scopes,
		Plan: c.Plan, At: c.Document.CreatedAt, ProposalID: fmt.Sprintf("repair-%d", h.index+1)})
	if err == nil {
		h.commits++
	}
	return result.OperationID, err
}

func (h *testHost) CommitVoterChange(ctx context.Context, change topology.VoterChange,
	c RepairCommit) (topology.OperationID, error) {
	if change.GroupID == "" {
		return "", fault.ErrInvalid
	}
	if h.beforeCommit != nil {
		h.beforeCommit()
	}
	result, err := h.apply(ctx, metaengine.CommitVoterChangeProposal{Change: change, Doc: c.Document,
		ExpectedVersion: c.ExpectedVersion, ExpectedTarget: c.ExpectedTarget, Scopes: c.Scopes,
		Plan: c.Plan, At: c.Document.CreatedAt})
	if err == nil {
		h.commits++
	}
	return result.OperationID, err
}
