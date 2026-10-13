package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// groupControllers dispatches load effects through caller code. Only _meta
// control and status use the library protocol.
type groupControllers struct{ g *Group }

func (c groupControllers) ReadNative(ctx context.Context, id string, candidates ...storage.Identity) (topology.NativeState, error) {
	if id != topology.MetaGroupID {
		if c.g.opts.LoadGroups == nil {
			return topology.NativeState{}, fault.ErrNoController
		}
		return c.g.opts.LoadGroups.ReadNative(ctx, id, candidates...)
	}
	ctx, done, err := c.g.beginEffect(ctx)
	if err != nil {
		return topology.NativeState{}, err
	}
	defer done()
	engine := c.g.engine.Load()
	if engine == nil {
		return topology.NativeState{}, fault.ErrReplicaNotReady
	}
	return engine.ReadNative(ctx)
}

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
func (c groupControllers) effect(ctx context.Context, grant topology.AuthorityGrant, kind topology.StepKind, invoke func(context.Context, operator.GroupEffects) ([]topology.EffectReceipt, error)) ([]topology.EffectReceipt, error) {
	if grant.Action == nil || grant.Action.Kind != kind {
		return nil, fault.ErrOperatorAuthorization
	}
	ctx, done, err := c.g.beginEffect(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := c.g.ValidateGrant(ctx, grant); err != nil {
		return nil, err
	}
	if grant.Key.GroupID == topology.MetaGroupID {
		return c.g.metaEffect(ctx, grant)
	}
	if c.g.opts.LoadGroups == nil {
		return nil, fault.ErrNoController
	}
	return invoke(ctx, c.g.opts.LoadGroups)
}
func (c groupControllers) CreateGroup(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.effect(ctx, grant, topology.StepCreateFoundingGroup, func(ctx context.Context, controller operator.GroupEffects) ([]topology.EffectReceipt, error) {
		return controller.CreateGroup(ctx, grant)
	})
}
func (c groupControllers) PrepareReplica(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.effect(ctx, grant, topology.StepPrepareReplica, func(ctx context.Context, controller operator.GroupEffects) ([]topology.EffectReceipt, error) {
		return controller.PrepareReplica(ctx, grant)
	})
}
func (c groupControllers) TransferLeader(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.effect(ctx, grant, topology.StepTransferLeader, func(ctx context.Context, controller operator.GroupEffects) ([]topology.EffectReceipt, error) {
		return controller.TransferLeader(ctx, grant)
	})
}
func (c groupControllers) StopReplica(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.effect(ctx, grant, topology.StepStopReplica, func(ctx context.Context, controller operator.GroupEffects) ([]topology.EffectReceipt, error) {
		return controller.StopReplica(ctx, grant)
	})
}
func (c groupControllers) CleanupReplicaState(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.effect(ctx, grant, topology.StepCleanupReplicaState, func(ctx context.Context, controller operator.GroupEffects) ([]topology.EffectReceipt, error) {
		return controller.CleanupReplicaState(ctx, grant)
	})
}
func (c groupControllers) DeleteGroupState(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.effect(ctx, grant, topology.StepDeleteGroupState, func(ctx context.Context, controller operator.GroupEffects) ([]topology.EffectReceipt, error) {
		return controller.DeleteGroupState(ctx, grant)
	})
}
func (c groupControllers) ApplyMembership(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	if grant.Action == nil {
		return nil, fault.ErrOperatorAuthorization
	}
	switch grant.Action.Kind {
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
	default:
		return nil, fault.ErrOperatorAuthorization
	}
	return c.effect(ctx, grant, grant.Action.Kind, func(ctx context.Context, controller operator.GroupEffects) ([]topology.EffectReceipt, error) {
		return controller.ApplyMembership(ctx, grant)
	})
}

func (g *Group) metaEffect(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	if grant.Action == nil || grant.Key.GroupID != topology.MetaGroupID {
		return nil, fault.ErrOperatorAuthorization
	}
	if err := g.ValidateGrant(ctx, grant); err != nil {
		return nil, err
	}
	a := *grant.Action
	if a.Kind == topology.StepPrepareReplica || a.Kind == topology.StepStopReplica {
		return g.metaHostEffect(ctx, grant)
	}
	if !g.IsLeader() {
		return g.forwardMetaEffect(ctx, grant)
	}
	engine := g.engine.Load()
	var proof topology.FenceEvidence
	if err := waitMetaAuthority(ctx, func() error {
		var err error
		proof, err = engine.InstallAuthority(ctx, grant)
		return err
	}); err != nil {
		return nil, err
	}
	if err := g.RecordFence(ctx, proof); err != nil {
		return nil, err
	}
	var receipt topology.EffectReceipt
	if a.Kind == topology.StepTransferLeader {
		if err := waitMetaAuthority(ctx, func() error { return engine.TransferAuthorized(ctx, grant) }); err != nil {
			return nil, err
		}
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			var found bool
			var err error
			receipt, found, err = engine.AuthorityReceipt(ctx, grant.Key)
			if err != nil {
				return nil, err
			}
			if found && engine.Status().LeaderID == a.ReplicaID {
				break
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick.C:
			}
		}
	} else {
		var change raftpb.ConfChangeV2
		switch a.Kind {
		case topology.StepAddLearner:
			admission, ok := g.state().AdmissionForOperator(a.OperatorID, a.NodeName, a.ReplicaID)
			if !ok || !admission.State.Prepared() {
				return nil, fault.ErrReplicaNotReady
			}
			change = membership.AddLearner(a.ReplicaID)
		case topology.StepPromoteVoter:
			change = membership.PromoteLearner(a.ReplicaID)
		case topology.StepDemoteVoter:
			change = membership.DemoteVoter(a.ReplicaID)
		case topology.StepRemoveReplica:
			change = membership.RemoveReplica(a.ReplicaID)
		default:
			return nil, fault.ErrOperatorAuthorization
		}
		if err := waitMetaAuthority(ctx, func() error {
			var err error
			receipt, err = engine.ProposeAuthorizedConfChange(ctx, grant, change, engine.Status().ConfState)
			return err
		}); err != nil {
			return nil, err
		}
	}
	return []topology.EffectReceipt{receipt}, nil
}

// Metadata proposals can publish their result while the shared engine is still
// finishing persistence/Advance. Retry the native phase without inserting more
// metadata proposals between attempts. The owner rechecks the exact installed
// grant and native safety every time; fencing or lifecycle errors end the wait.
func waitMetaAuthority(ctx context.Context, attempt func() error) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := attempt()
		if !errors.Is(err, fault.ErrAuthorityPending) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func containsMember(cs raftpb.ConfState, id uint64) bool {
	if id == 0 {
		return false
	}
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
	var observed topology.Identity
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

var _ operator.GroupEffects = groupControllers{}
