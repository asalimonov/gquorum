package meta

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// groupControllers dispatches load effects through caller code. Only _meta
// control and status use the library protocol.
type groupControllers struct{ g *Group }

func (c groupControllers) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	if id != topology.MetaGroupID {
		if c.g.opts.LoadGroups == nil {
			return topology.GroupStatus{}, fault.ErrNoController
		}
		return c.g.opts.LoadGroups.Status(ctx, id)
	}
	if !c.g.IsLeader() {
		data, err := c.g.callLeader(ctx, "MetaStatus", nil)
		if err != nil {
			return topology.GroupStatus{}, err
		}
		var status topology.GroupStatus
		if err = json.Unmarshal(data, &status); err != nil {
			return status, fault.New(fault.Unreadable, "invalid meta status", err)
		}
		return status, nil
	}
	status := c.g.engine.Load().Status()
	live, err := c.g.metaHealth(ctx, status)
	if err != nil {
		return status, err
	}
	status.LiveVoters = live
	status.HealthKnown = true
	return status, nil
}
func (c groupControllers) effect(ctx context.Context, a topology.AuthorizedStep, kind topology.StepKind, invoke func(topology.GroupController) error) error {
	if a.Kind != kind {
		return fault.ErrExecutionAuthorization
	}
	if err := c.g.Authorize(ctx, a); err != nil {
		return err
	}
	if a.GroupID == topology.MetaGroupID {
		return c.g.metaEffect(ctx, a)
	}
	if c.g.opts.LoadGroups == nil {
		return fault.ErrNoController
	}
	return invoke(c.g.opts.LoadGroups)
}
func (c groupControllers) CreateGroup(ctx context.Context, a topology.AuthorizedStep) error {
	return c.effect(ctx, a, topology.StepCreateFoundingGroup, func(controller topology.GroupController) error { return controller.CreateGroup(ctx, a) })
}
func (c groupControllers) PrepareReplica(ctx context.Context, a topology.AuthorizedStep) error {
	return c.effect(ctx, a, topology.StepPrepareReplica, func(controller topology.GroupController) error { return controller.PrepareReplica(ctx, a) })
}
func (c groupControllers) ApplyMembership(ctx context.Context, a topology.AuthorizedStep) error {
	switch a.Kind {
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
	default:
		return fault.ErrExecutionAuthorization
	}
	return c.effect(ctx, a, a.Kind, func(controller topology.GroupController) error { return controller.ApplyMembership(ctx, a) })
}
func (c groupControllers) TransferLeader(ctx context.Context, a topology.AuthorizedStep) error {
	return c.effect(ctx, a, topology.StepTransferLeader, func(controller topology.GroupController) error { return controller.TransferLeader(ctx, a) })
}
func (c groupControllers) StopReplica(ctx context.Context, a topology.AuthorizedStep) error {
	return c.effect(ctx, a, topology.StepStopReplica, func(controller topology.GroupController) error { return controller.StopReplica(ctx, a) })
}
func (c groupControllers) CleanupReplicaState(ctx context.Context, a topology.AuthorizedStep) error {
	return c.effect(ctx, a, topology.StepCleanupReplicaState, func(controller topology.GroupController) error { return controller.CleanupReplicaState(ctx, a) })
}
func (c groupControllers) DeleteGroupState(ctx context.Context, a topology.AuthorizedStep) error {
	return c.effect(ctx, a, topology.StepDeleteGroupState, func(controller topology.GroupController) error { return controller.DeleteGroupState(ctx, a) })
}

func (g *Group) metaEffect(ctx context.Context, a topology.AuthorizedStep) error {
	if a.GroupID != topology.MetaGroupID {
		return fault.ErrExecutionAuthorization
	}
	if err := g.Authorize(ctx, a); err != nil {
		return err
	}
	if !g.IsLeader() {
		return fault.ErrNotLeader
	}
	engine := g.engine.Load()
	switch a.Kind {
	case topology.StepPrepareReplica:
		admission, ok := g.state().AdmissionForExecution(a.ExecutionID, a.NodeName, a.ReplicaID)
		if !ok || !admission.State.Prepared() {
			return fault.New(fault.ReplicaNotReady, "exact join admission has not acknowledged its runtime", nil)
		}
		// Readiness is durable for this exact member identity. A retained-store
		// restart may have a new process instance without losing that proof.
		if !g.probeMember(ctx, a.NodeName, a.ReplicaID, "") {
			return fault.ErrReplicaNotReady
		}
		return nil
	case topology.StepStopReplica:
		// A removed meta host receives OnSelfRemoved from its own durable membership
		// observer. Stopping it here could discard the approval's remaining progress.
		status := engine.Status()
		if containsMember(status.ConfState, a.ReplicaID) {
			return fault.ErrExecutionAuthorization
		}
		return nil
	case topology.StepTransferLeader:
		return engine.TransferLeader(ctx, a.ReplicaID)
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
		if a.Kind == topology.StepAddLearner {
			admission, ok := g.state().AdmissionForExecution(a.ExecutionID, a.NodeName, a.ReplicaID)
			if !ok || !admission.State.Prepared() {
				return fault.ErrReplicaNotReady
			}
		}
		live, err := g.metaHealth(ctx, engine.Status())
		if err != nil {
			return err
		}
		if err = g.Authorize(ctx, a); err != nil {
			return err
		}
		var change raftpb.ConfChangeV2
		switch a.Kind {
		case topology.StepAddLearner:
			change = membership.AddLearner(a.ReplicaID)
		case topology.StepPromoteVoter:
			change = membership.PromoteLearner(a.ReplicaID)
		case topology.StepDemoteVoter:
			change = membership.DemoteVoter(a.ReplicaID)
		case topology.StepRemoveReplica:
			change = membership.RemoveReplica(a.ReplicaID)
		}
		return g.coordinator.Apply(ctx, engine.Membership(), metaLive{live}, change)
	default:
		return fault.ErrExecutionAuthorization
	}
}

type metaLive struct{ ids map[uint64]bool }

func (h metaLive) Live(id string) (map[uint64]bool, bool) { return h.ids, id == topology.MetaGroupID }
func containsMember(cs raftpb.ConfState, id uint64) bool {
	for _, members := range [][]uint64{cs.Voters, cs.Learners, cs.VotersOutgoing, cs.LearnersNext} {
		for _, member := range members {
			if member == id {
				return true
			}
		}
	}
	return false
}
func (g *Group) probeMember(parent context.Context, name string, id uint64, instance string) bool {
	ctx, cancel := context.WithTimeout(parent, g.opts.Timing.DialTimeout)
	defer cancel()
	if name == g.currentIdentity().NodeName {
		own := g.currentIdentity()
		return own.MemberID == id && (instance == "" || instance == own.InstanceID) && g.ensureRunning(ctx) == nil
	}
	address, err := g.resolveNode(ctx, name)
	if err != nil {
		return false
	}
	reply, err := g.transport.CallAddress(ctx, address, "IdentityPing", nil)
	if err != nil || metatransport.ReplyError(reply) != nil {
		return false
	}
	var observed Identity
	if json.Unmarshal(reply.Data, &observed) != nil {
		return false
	}
	return observed.ClusterID == g.currentIdentity().ClusterID && observed.NodeName == name && observed.MemberID == id && (instance == "" || observed.InstanceID == instance)
}
func (g *Group) metaHealth(ctx context.Context, status topology.GroupStatus) (map[uint64]bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ids := append(append([]uint64(nil), status.ConfState.Voters...), status.ConfState.Learners...)
	result := make(map[uint64]bool, len(ids))
	if g.opts.Health != nil {
		health, err := g.opts.Health.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		if health == nil {
			return nil, fault.New(fault.Unavailable, "node health is unknown", nil)
		}
		for _, id := range ids {
			result[id] = health[g.memberNode(id)] == topology.HealthAlive
		}
		return result, nil
	}
	// A bounded probe pool supplies _meta liveness when the caller does not have
	// a health source. The exact cluster/node/member tuple must answer.
	workers := min(8, len(ids))
	jobs := make(chan uint64, len(ids))
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				name := g.memberNode(id)
				alive := name != "" && g.probeMember(ctx, name, id, "")
				mu.Lock()
				result[id] = alive
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

var _ topology.GroupController = groupControllers{}
var _ membership.HealthSource = metaLive{}
