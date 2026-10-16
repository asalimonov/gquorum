package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
)

// Plan previews safe convergence to a retained topology version without
// authorizing effects.
func (g *Group) Plan(ctx context.Context, version uint64) (topology.Plan, error) {
	doc, err := g.Get(ctx, version)
	if err != nil {
		return topology.Plan{}, err
	}
	return g.PlanFor(ctx, doc)
}

// PlanFor validates desired topology and builds a plan against observed native
// group state.
func (g *Group) PlanFor(ctx context.Context, doc topology.Document) (topology.Plan, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return topology.Plan{}, err
	}
	if doc.ClusterID != g.currentIdentity().ClusterID {
		return topology.Plan{}, fault.ErrIdentityMismatch
	}
	if err := doc.Validate(g.rules); err != nil {
		return topology.Plan{}, err
	}
	active, err := g.state().DeletionBase(ctx, doc)
	if err != nil {
		return topology.Plan{}, err
	}
	approved := g.state().Target()
	if err := topology.ValidatePlacementTransition(approved, active, doc); err != nil {
		return topology.Plan{}, err
	}
	planning, err := g.planningInput(ctx, doc)
	if err != nil {
		return topology.Plan{}, err
	}
	observed := planning.Groups[topology.MetaGroupID].Observation.Clone()
	native, found := planning.Native[topology.MetaGroupID]
	observed.Found, observed.ConfState = found, native.Clone().ConfState
	health := healthFromMeta(active, doc, observed)
	input := topology.MetaDrainInput{Status: observed, Health: health}
	preferred, err := topology.ValidateMetaDrainSafety(active, doc, input)
	if err != nil {
		return topology.Plan{}, err
	}
	plan, err := topology.BuildPlanPreferred(planning, doc,
		topology.PlanOptions{Rules: g.rules, PreferredMetaTransfer: preferred})
	return plan, explainUnsafePlacementPlan(approved, active, doc, err)
}

func (g *Group) observeGroups(ctx context.Context,
	active, desired topology.Document) (map[string]topology.GroupStatus, error) {
	ids := map[string]bool{}
	for _, doc := range []topology.Document{active, desired} {
		for _, group := range doc.Groups {
			ids[group.ID] = true
		}
	}
	sorted := make([]string, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	slices.Sort(sorted)
	observed := make(map[string]topology.GroupStatus, len(ids))
	controllers := groupControllers{g}
	for _, id := range sorted {
		status, err := controllers.Status(ctx, id)
		if err != nil {
			if !errors.Is(err, fault.ErrUnavailable) || active.Group(id) == nil || desired.Group(id) != nil {
				return nil, err
			}
			status = topology.GroupStatus{GroupID: id, Found: true}
		}
		if status.GroupID != id {
			return nil, fault.New(fault.IdentityMismatch, fmt.Sprintf("status for %q belongs to %q", id,
				status.GroupID), nil)
		}
		observed[id] = status
	}
	return observed, nil
}

func healthFromMeta(active, target topology.Document, status topology.GroupStatus) map[string]topology.Health {
	if !status.HealthKnown {
		return nil
	}
	result := map[string]topology.Health{}
	for _, doc := range []topology.Document{active, target} {
		if group := doc.MetaGroup(); group != nil {
			for _, r := range group.Replicas {
				health := topology.HealthDead
				if status.LiveVoters[r.ReplicaID] {
					health = topology.HealthAlive
				}
				result[r.NodeName] = health
			}
		}
	}
	return result
}

func (g *Group) finalTarget(ctx context.Context, active, target topology.Document) error {
	status, err := (groupControllers{g}).Status(ctx, topology.MetaGroupID)
	if err != nil {
		return err
	}
	input := topology.MetaDrainInput{Status: status,
		Health: healthFromMeta(active, target, status), Final: true}
	_, err = topology.ValidateMetaDrainSafety(active, target, input)
	return err
}

// Apply approves a topology version with its guarded immutable execution plan.
func (g *Group) Apply(ctx context.Context, version uint64) (topology.OperatorID, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return "", err
	}
	if !g.IsLeader() {
		reply, err := g.callLeader(ctx, "ApplyVersion", struct {
			Version uint64 `json:"version"`
		}{version})
		if err != nil {
			return "", err
		}
		return approvalOperatorID(reply)
	}
	return g.applyLocal(ctx, version)
}

// approvalOperatorID validates the follower's one-hop approval response.
func approvalOperatorID(reply []byte) (topology.OperatorID, error) {
	var id topology.OperatorID
	if err := json.Unmarshal(reply, &id); err != nil {
		return "", fault.New(fault.Unreadable, "invalid execution response", err)
	}
	if id == "" {
		return "", fault.New(fault.Unreadable, "approval returned no execution ID", nil)
	}
	return id, nil
}

func (g *Group) applyLocal(ctx context.Context, version uint64) (topology.OperatorID, error) {
	if !g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	machine := g.state()
	if first, err := machine.Approval(version); first != "" || err != nil {
		return topology.OperatorID(first), err
	}
	latest := machine.Latest()
	if latest.Version != version {
		return "", &fault.Error{Code: fault.VersionConflict, ExpectedVersion: version, CurrentVersion: latest.Version}
	}
	plan, err := g.PlanFor(ctx, latest)
	if err != nil {
		return "", err
	}
	result, err := g.propose(ctx, metaengine.BeginOperatorProposal{
		Version: version,
		Plan:    plan,
		Actor:   g.currentIdentity().NodeName,
		Origin:  latest.Origin,
		At:      time.Now().UTC(),
	})
	if err != nil {
		return "", err
	}
	g.controller.Signal()
	return result.OperatorID, nil
}

// WaitOperator waits for a retained operator to reach any terminal execution
// state.
func (g *Group) WaitOperator(ctx context.Context, id topology.OperatorID) (topology.OperatorStatus, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := g.Operator(ctx, id)
		if err == nil && status.State.Terminal() {
			return status, nil
		}
		if err != nil && !errors.Is(err, fault.ErrHistoryUnavailable) {
			return status, err
		}
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-g.stopped:
			return status, fault.ErrStopped
		case <-ticker.C:
		}
	}
}

func (g *Group) callLeader(ctx context.Context, method string, request any) ([]byte, error) {
	engine := g.engine.Load()
	if engine == nil || engine.RaftStatus().Lead == 0 {
		return nil, fault.ErrUnavailable
	}
	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	address, err := g.resolveMember(ctx, engine.RaftStatus().Lead)
	if err != nil {
		return nil, err
	}
	reply, err := g.transport.CallAddress(ctx, address, method, data)
	if err != nil {
		return nil, err
	}
	if err = metatransport.ReplyError(reply); err != nil {
		return nil, err
	}
	return reply.Data, nil
}

type operatorCoordinator struct{ g *Group }

type operatorStore struct{ *Group }

func (s operatorStore) Active(ctx context.Context) (topology.Document, error) {
	machine := s.state()
	if ex, ok := machine.CurrentOperator(); ok {
		return machine.OperatorBase(ctx, ex.Status.ID)
	}
	return machine.DeletionBase(ctx, machine.Target())
}

func (c operatorCoordinator) IsMetaLeader() bool { return c.g.IsLeader() }

func (c operatorCoordinator) CurrentOperator(ctx context.Context) (operator.Record, bool) {
	if ctx.Err() != nil {
		return operator.Record{}, false
	}
	record, ok := c.g.state().CurrentOperator()
	return operator.FromPublic(record), ok
}

func (c operatorCoordinator) RecordOperatorTransition(ctx context.Context, t topology.OperatorTransition) error {
	if !c.g.IsLeader() {
		return fault.ErrNotLeader
	}
	if _, err := c.g.propose(ctx, metaengine.TransitionOperatorProposal{Transition: t}); err != nil {
		return err
	}
	if t.State == topology.OperatorSucceeded {
		return c.g.activateCompleted(ctx)
	}
	return nil
}
func (g *Group) activateCompleted(ctx context.Context) error {
	ex, ok := g.state().CurrentOperator()
	if !ok || ex.Status.State != topology.OperatorSucceeded {
		return nil
	}
	views := g.state().Views()
	target, active := views.Target, views.Active
	if target.Version != ex.Status.Version || active.Version >= target.Version {
		return nil
	}
	if err := g.finalTarget(ctx, active, target); err != nil {
		return err
	}
	_, err := g.propose(ctx, metaengine.ActivateTargetProposal{OperatorID: ex.Status.ID, Version: ex.Status.Version,
		Attempt: ex.Status.Attempt})
	return err
}
