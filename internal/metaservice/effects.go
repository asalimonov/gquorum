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
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

const metaHealthProbeWorkers = 8

// groupControllers dispatches load effects through caller code. Only _meta
// control and status use the library protocol.
type groupControllers struct{ g *Group }

func (c groupControllers) ReadNative(parent context.Context, id string,
	candidates ...storage.Identity) (topology.NativeState, error) {
	if id != topology.MetaGroupID {
		if c.g.opts.LoadGroups == nil {
			return topology.NativeState{}, fault.ErrNoController
		}
		return c.g.opts.LoadGroups.ReadNative(parent, id, candidates...)
	}
	ctxHandle, err := c.g.acquireEffectContext(parent)
	ctx, done := ctxHandle.Context, ctxHandle.Release
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
	status := c.g.LocalStatus()
	live, err := c.g.metaHealth(ctx, status)
	if err != nil {
		return status, err
	}
	status.LiveVoters = live
	status.HealthKnown = true
	return status, nil
}

func (g *Group) applyNativeMetaEffect(ctx context.Context,
	grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	if !g.IsLeader() {
		return nil, fault.ErrNotLeader
	}
	if err := g.ValidateGrant(ctx, grant); err != nil {
		return nil, err
	}
	a := *grant.Action
	engine := g.engine.Load()
	var proof topology.FenceEvidence
	checkErr := waitMetaAuthority(ctx, func() error {
		var err error
		proof, err = engine.InstallAuthority(ctx, grant)
		return err
	})
	if checkErr != nil {
		return nil, checkErr
	}
	if err := g.RecordFence(ctx, proof); err != nil {
		return nil, err
	}
	var receipt topology.EffectReceipt
	var err error
	if a.Kind == topology.StepTransferLeader {
		receipt, err = transferMetaReceipt(ctx, engine, grant)
	} else {
		receipt, err = g.changeMetaReceipt(ctx, engine, grant)
	}
	if err != nil {
		return nil, err
	}
	return []topology.EffectReceipt{receipt}, nil
}

func transferMetaReceipt(ctx context.Context, engine *raftengine.Group,
	grant topology.AuthorityGrant) (topology.EffectReceipt, error) {
	a := *grant.Action
	if err := waitMetaAuthority(ctx, func() error { return engine.TransferAuthorized(ctx, grant) }); err != nil {
		return topology.EffectReceipt{}, err
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		lookup, err := engine.AuthorityReceipt(ctx, grant.Key)
		receipt := lookup.Receipt
		if err != nil {
			return topology.EffectReceipt{}, err
		}
		if lookup.Found && engine.Status().LeaderID == a.ReplicaID {
			return receipt, nil
		}
		select {
		case <-ctx.Done():
			return topology.EffectReceipt{}, ctx.Err()
		case <-tick.C:
		}
	}
}

func (g *Group) changeMetaReceipt(ctx context.Context, engine *raftengine.Group,
	grant topology.AuthorityGrant) (topology.EffectReceipt, error) {
	a := *grant.Action
	var receipt topology.EffectReceipt
	var change raftpb.ConfChangeV2
	switch a.Kind {
	case topology.StepAddLearner:
		admission, ok := g.state().AdmissionForOperator(a.OperationID, a.NodeName, a.ReplicaID)
		if !ok || !admission.State.Prepared() {
			return topology.EffectReceipt{}, fault.ErrReplicaNotReady
		}
		change = membership.AddLearner(a.ReplicaID)
	case topology.StepPromoteVoter:
		change = membership.PromoteLearner(a.ReplicaID)
	case topology.StepDemoteVoter:
		change = membership.DemoteVoter(a.ReplicaID)
	case topology.StepRemoveReplica:
		change = membership.RemoveReplica(a.ReplicaID)
	default:
		return topology.EffectReceipt{}, fault.ErrOperationAuthorization
	}
	nextCheckErr := waitMetaAuthority(ctx, func() error {
		var err error
		receipt, err = engine.ProposeAuthorizedConfChange(ctx, grant, change, engine.Status().ConfState)
		return err
	})
	if nextCheckErr != nil {
		return topology.EffectReceipt{}, nextCheckErr
	}
	return receipt, nil
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
	identity := g.currentIdentity()
	return observed.ClusterID == identity.ClusterID && observed.NodeName == name &&
		observed.MemberID == id && (instance == "" || observed.InstanceID == instance)
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
	// A bounded probe pool supplies _meta liveness when the caller does not
	// have
	// a health source. The exact cluster/node/member tuple must answer.
	workers := min(metaHealthProbeWorkers, len(ids))
	jobs := make(chan uint64, len(ids))
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for range workers {
		wg.Go(func() {
			for id := range jobs {
				name := g.memberNode(id)
				alive := name != "" && g.probeMember(ctx, name, id, "")
				mu.Lock()
				result[id] = alive
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

var _ operator.GroupEffects = groupControllers{}
