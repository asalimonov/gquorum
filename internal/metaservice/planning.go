package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) Plan(ctx context.Context, version uint64) (topology.Plan, error) {
	doc, err := g.Get(ctx, version)
	if err != nil {
		return topology.Plan{}, err
	}
	return g.PlanFor(ctx, doc)
}
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
	active := g.state().Active()
	observed, err := g.observeGroups(ctx, active, doc)
	if err != nil {
		return topology.Plan{}, err
	}
	health := healthFromMeta(active, doc, observed[topology.MetaGroupID])
	preferred, err := topology.ValidateMetaDrainSafety(active, doc, observed[topology.MetaGroupID], health, false)
	if err != nil {
		return topology.Plan{}, err
	}
	return topology.BuildPlanPreferred(active, doc, observed, g.rules, preferred)
}
func (g *Group) observeGroups(ctx context.Context, active, desired topology.Document) (map[string]topology.GroupStatus, error) {
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
	sort.Strings(sorted)
	observed := make(map[string]topology.GroupStatus, len(ids))
	controllers := groupControllers{g}
	for _, id := range sorted {
		status, err := controllers.Status(ctx, id)
		if err != nil {
			if errors.Is(err, fault.ErrUnavailable) && active.Group(id) != nil && desired.Group(id) == nil {
				status = topology.GroupStatus{GroupID: id, Found: true}
			} else {
				return nil, err
			}
		}
		if status.GroupID != id {
			return nil, fault.New(fault.IdentityMismatch, fmt.Sprintf("status for %q belongs to %q", id, status.GroupID), nil)
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
	_, err = topology.ValidateMetaDrainSafety(active, target, status, healthFromMeta(active, target, status), true)
	return err
}
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
		var id topology.OperatorID
		if err = json.Unmarshal(reply, &id); err != nil {
			return "", fault.New(fault.Unreadable, "invalid execution response", err)
		}
		if id == "" {
			return "", fault.New(fault.Unreadable, "approval returned no execution ID", nil)
		}
		return id, nil
	}
	return g.applyLocal(ctx, version)
}
func (g *Group) applyLocal(ctx context.Context, version uint64) (topology.OperatorID, error) {
	if !g.IsLeader() {
		return "", fault.ErrNotLeader
	}
	machine := g.state()
	latest := machine.Latest()
	if latest.Version != version {
		return "", &fault.Error{Code: fault.VersionConflict, ExpectedVersion: version, CurrentVersion: latest.Version, Message: "only current Latest can be approved"}
	}
	if ex, ok := machine.CurrentOperator(); ok && ex.Status.Version == version {
		if ex.Status.State == topology.OperatorSucceeded || !ex.Status.State.Terminal() {
			g.controller.Signal()
			return ex.Status.ID, nil
		}
		if ex.Status.State == topology.OperatorSuperseded {
			return "", fault.ErrVersionConflict
		}
	}
	plan, err := g.PlanFor(ctx, latest)
	if err != nil {
		return "", err
	}
	result, err := g.propose(ctx, metaengine.BeginOperatorProposal{Version: version, Plan: plan, Actor: g.currentIdentity().NodeName, Origin: latest.Origin, At: time.Now().UTC()})
	if err != nil {
		return "", err
	}
	g.controller.Signal()
	return result.OperatorID, nil
}
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
	_, target, active := g.state().Views()
	if target.Version != ex.Status.Version || active.Version >= target.Version {
		return nil
	}
	if err := g.finalTarget(ctx, active, target); err != nil {
		return err
	}
	_, err := g.propose(ctx, metaengine.ActivateTargetProposal{OperatorID: ex.Status.ID, Version: ex.Status.Version, Attempt: ex.Status.Attempt})
	return err
}
