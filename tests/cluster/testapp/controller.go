package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// The application owns its machine data. Node owns Raft recovery, membership,
// effects and the lifetime of every successful factory result.
func (a *application) newReplica(ctx context.Context, spec load.ReplicaSpec) (load.Application, error) {
	if err := ctx.Err(); err != nil {
		return load.Application{}, err
	}
	if spec.Identity.NodeName != a.cfg.NodeName || spec.Identity.ClusterID != a.cfg.ClusterID || spec.Node != a.node.Identity() {
		return load.Application{}, fault.ErrIdentityMismatch
	}
	ref := load.ReplicaRef{GroupID: spec.Identity.GroupID, ReplicaID: spec.Identity.ReplicaID}
	machine := newMachine()
	a.mu.Lock()
	if a.machines[ref] != nil {
		a.mu.Unlock()
		return load.Application{}, fault.ErrIdentityMismatch
	}
	a.machines[ref] = machine
	if a.clusterHooks != nil {
		a.clusterHooks.runtime.factories.Add(1)
	}
	a.mu.Unlock()
	return load.Application{StateMachine: a.clusterMachine(machine, walKey{ref.GroupID, ref.ReplicaID}), Close: func(ctx context.Context) error {
		if err := a.joinProjection(ctx, walKey{ref.GroupID, ref.ReplicaID}); err != nil {
			return err
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		if current := a.machines[ref]; current != nil && current != machine {
			return fault.ErrIdentityMismatch
		}
		delete(a.machines, ref)
		a.forgetReadControl(walKey{ref.GroupID, ref.ReplicaID})
		return nil
	}}, nil
}

func (a *application) local(id string) *localReplica {
	group, found := a.node.LoadGroup(id)
	if !found {
		return nil
	}
	a.mu.RLock()
	machine := a.machines[load.ReplicaRef{GroupID: id, ReplicaID: group.ReplicaID()}]
	a.mu.RUnlock()
	if machine == nil {
		return nil
	}
	return &localReplica{group: group, machine: machine}
}

// Embedding the default hooks lets this application decide only its process
// response after Node has completed mandatory removal fencing.
type fixtureLifecycle struct {
	*lifecycle.DefaultHooks
	app *application
}

func (p *fixtureLifecycle) Removal(ctx context.Context, input lifecycle.RemovalInput) (lifecycle.RemovalDecision, error) {
	decision, err := p.DefaultHooks.Removal(ctx, input)
	if err != nil {
		return decision, err
	}
	if input.Identity != p.app.node.Identity() {
		return lifecycle.RemovalDecision{}, fault.ErrIdentityMismatch
	}
	status := p.app.node.Status()
	if status.State != gquorum.NodeFenced || status.Ready || status.SessionLive || status.Meta.Found {
		return lifecycle.RemovalDecision{}, fault.New(fault.Invalid, "removal hooks ran before mandatory fence", nil)
	}
	for _, replica := range status.Loads {
		if replica.State == "ready" || replica.State == "starting" {
			return lifecycle.RemovalDecision{}, fault.New(fault.Invalid, "load owner remains during removal", nil)
		}
	}
	calls := p.app.clusterHooks.runtime.removalCalls.Add(1)
	if p.app.cfg.RetryRemoval {
		if calls == 1 {
			return lifecycle.RemovalDecision{}, fmt.Errorf("fixture: retry removal response")
		}
		return lifecycle.RemovalDecision{Action: lifecycle.KeepDiagnostics}, nil
	}
	p.app.logger.Info("self-removal identity verified", "node", input.Identity.NodeName, "member_id", input.Identity.MemberID, "instance_id", input.Identity.InstanceID, "reason", input.Removal.Reason)
	p.app.RequestClose()
	return decision, nil
}

// Routing application writes needs only this group's leader hint. It does not
// probe every other group or supply membership-health authority to Node.
func (a *application) groupStatus(ctx context.Context, id string) (topology.GroupStatus, error) {
	if group, found := a.node.LoadGroup(id); found {
		status := group.Status()
		if status.Found && status.LeaderID != 0 {
			return status, nil
		}
	}
	active, err := a.cluster.Target(ctx)
	if err != nil {
		return topology.GroupStatus{}, err
	}
	group := active.Group(id)
	if group == nil {
		return topology.GroupStatus{GroupID: id}, fault.ErrReplicaNotReady
	}
	for _, replica := range group.Replicas {
		probe, cancel := context.WithTimeout(ctx, 350*time.Millisecond)
		reply, err := a.callNode(probe, replica.NodeName, "Status", id, replica.ReplicaID, nil)
		cancel()
		if err != nil {
			continue
		}
		var status topology.GroupStatus
		if err := json.Unmarshal(reply.Data, &status); err != nil || reply.ReplicaId != replica.ReplicaID || status.GroupID != id || status.HostNode != replica.NodeName {
			return topology.GroupStatus{}, fault.ErrIdentityMismatch
		}
		if status.Found && status.LeaderID != 0 {
			return status, nil
		}
	}
	return topology.GroupStatus{GroupID: id}, fault.ErrUnavailable
}

func (p *fixtureLifecycle) Readiness(ctx context.Context, input lifecycle.ReadinessInput) (lifecycle.ReadinessDecision, error) {
	decision, err := p.DefaultHooks.Readiness(ctx, input)
	if err != nil {
		return decision, err
	}
	missing := make([]string, len(input.MissingFounders))
	for i, node := range input.MissingFounders {
		missing[i] = node.NodeName
	}
	names := strings.Join(missing, ",")
	p.app.mu.Lock()
	previous := p.app.startupMissing
	p.app.startupMissing = names
	p.app.startupObserved = true
	p.app.mu.Unlock()
	if p.app.cfg.GateFounders && decision.State == lifecycle.Waiting && input.FirstBootstrap && names != previous {
		p.app.logger.Info("waiting for configured cluster members to activate", "missing", names)
	}
	if decision.State == lifecycle.Failed {
		decision.Reason = fmt.Sprintf("greenfield founding timed out after %s; missing: %s", p.app.cfg.FoundingTimeout, names)
	}
	return decision, nil
}
func (a *application) foundingMessage() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	missing := a.startupMissing
	if !a.startupObserved {
		// HTTP starts independently of the first hooks observation. Until it
		// arrives, every configured founder is still unverified by this caller.
		names := make([]string, len(a.cfg.Peers))
		for i, peer := range a.cfg.Peers {
			names[i] = peer.Name
		}
		missing = strings.Join(names, ",")
	}
	return "greenfield founding: waiting for configured members " + missing
}

// Materialized projections are the fixture's only durable application resources.
// Raft WAL/quarantine and the meta ledger remain entirely Node-owned.
func (a *application) retireReplica(ctx context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
	if err := ctx.Err(); err != nil {
		return load.RetirementResult{}, err
	}
	key := walKey{req.Identity.GroupID, req.Identity.ReplicaID}
	if req.Identity != a.projectionIdentity(key) || key.replica == 0 {
		return load.RetirementResult{}, fault.ErrIdentityMismatch
	}
	if err := topology.ValidateLoadID(key.group); err != nil {
		return load.RetirementResult{}, err
	}
	if err := a.joinProjection(ctx, key); err != nil {
		return load.RetirementResult{}, err
	}
	path := a.projectionPath(key)
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() {
			return load.RetirementResult{}, fault.ErrIdentityMismatch
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return load.RetirementResult{}, err
		}
		var saved projectionFile
		if err := decodeJSON(raw, &saved); err != nil {
			return load.RetirementResult{}, err
		}
		if saved.Identity != req.Identity {
			return load.RetirementResult{}, fault.ErrIdentityMismatch
		}
		if err := ctx.Err(); err != nil {
			return load.RetirementResult{}, err
		}
		if err := os.Remove(path); err != nil {
			return load.RetirementResult{}, err
		}
	} else if !os.IsNotExist(err) {
		return load.RetirementResult{}, err
	}
	// Sync even after an earlier removal succeeded but its acknowledgement failed.
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if syncErr != nil {
			return load.RetirementResult{}, syncErr
		}
		if closeErr != nil {
			return load.RetirementResult{}, closeErr
		}
	} else if !os.IsNotExist(err) {
		return load.RetirementResult{}, err
	}
	return load.RetirementResult{Grant: req.Grant.Clone(), Identity: req.Identity, Reason: req.Reason, Authorization: req.Authorization, AdmissionID: req.AdmissionID, PreviousMemberID: req.PreviousMemberID, GrantedMemberID: req.GrantedMemberID, Verified: true}, nil
}
