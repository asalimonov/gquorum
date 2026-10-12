package managed

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func localAction(kind topology.StepKind) bool {
	switch kind {
	case topology.StepCreateFoundingGroup, topology.StepPrepareReplica, topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepDeleteGroupState:
		return true
	default:
		return false
	}
}

func (h *Host) validateAction(ctx context.Context, req load.ActionRequest) error {
	if !completeIdentity(req.From) || !completePeer(req.To) {
		return fault.ErrInvalid
	}
	if req.To.Node != h.cfg.Identity() || req.From.ClusterID != req.To.Node.ClusterID || req.To.Replica.GroupID != req.Authorization.GroupID {
		return fault.ErrIdentityMismatch
	}
	if err := h.cfg.ValidateNode(ctx, req.From); err != nil {
		return err
	}
	if err := h.cfg.Authorize(ctx, req.Authorization); err != nil {
		return err
	}
	targets, err := h.cfg.ActionTargets(ctx, req.Authorization)
	if err != nil {
		return err
	}
	if localAction(req.Authorization.Kind) {
		want := storage.Identity{NodeName: req.To.Node.NodeName, ClusterID: req.To.Node.ClusterID, GroupID: req.To.Replica.GroupID, ReplicaID: req.To.Replica.ReplicaID}
		if req.LocalTarget != want || !slices.Contains(targets, want) {
			return fault.ErrOperatorAuthorization
		}
		return nil
	}
	if req.LocalTarget != (storage.Identity{}) {
		return fault.ErrOperatorAuthorization
	}
	intended := storage.Identity{NodeName: req.Authorization.NodeName, ClusterID: req.From.ClusterID, GroupID: req.Authorization.GroupID, ReplicaID: req.Authorization.ReplicaID}
	if !slices.Contains(targets, intended) {
		return fault.ErrOperatorAuthorization
	}
	return h.cfg.ValidatePeer(ctx, req.To)
}

func (h *Host) ApplyAction(parent context.Context, req load.ActionRequest) (load.ActionResult, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	if err := h.validateAction(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	ctx, release, err := h.beginOperation(ctx, req.Authorization.GroupID)
	if err != nil {
		return load.ActionResult{}, err
	}
	defer release()
	if err := h.validateAction(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	switch req.Authorization.Kind {
	case topology.StepCreateFoundingGroup, topology.StepPrepareReplica:
		slot, startErr := h.acceptGuardedStart(ctx, req.To.Replica, StartOperator, func(ctx context.Context) error { return h.validateAction(ctx, req) })
		if startErr == nil {
			_, startErr = h.waitStarted(ctx, slot)
		}
		err = startErr
	case topology.StepStopReplica:
		err = h.stopRetiringIdentity(ctx, req.LocalTarget, req.Authorization)
	case topology.StepCleanupReplicaState, topology.StepDeleteGroupState:
		a := req.Authorization
		_, err = h.retire(ctx, load.RetirementRequest{Identity: req.LocalTarget, Reason: load.RetirementTopology, Authorization: &a})
	case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica, topology.StepTransferLeader:
		err = h.applyConsensusAction(ctx, req)
	default:
		err = fault.ErrUnsupported
	}
	if err != nil {
		return load.ActionResult{}, err
	}
	if err := h.validateAction(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	if err := h.verifyLocalEffect(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	return load.ActionResult{From: req.To, Authorization: req.Authorization, LocalTarget: req.LocalTarget, Verified: true}, nil
}

func (h *Host) actionCore(req load.ActionRequest) (*raftengine.Group, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	slot := h.slots[req.To.Replica.GroupID]
	if slot == nil || slot.core == nil || slot.released || !slot.core.Running() {
		return nil, fault.ErrReplicaNotReady
	}
	if localPeer(slot) != req.To {
		return nil, fault.ErrIdentityMismatch
	}
	return slot.core, nil
}

func (h *Host) applyConsensusAction(ctx context.Context, req load.ActionRequest) error {
	core, err := h.actionCore(req)
	if err != nil {
		return err
	}
	a := req.Authorization
	if consensusEffectPresent(core.Status(), a) {
		return nil
	}
	if !core.IsLeader() {
		return fault.ErrNotLeader
	}
	if a.Kind == topology.StepTransferLeader {
		if err := core.TransferLeader(ctx, a.ReplicaID); err != nil {
			return err
		}
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for !consensusEffectPresent(core.Status(), a) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
			}
			if err := h.cfg.Authorize(ctx, a); err != nil {
				return err
			}
		}
		return nil
	}
	// Adding a learner must never silently demote an already-voting identity.
	if a.Kind == topology.StepAddLearner && slices.Contains(core.Status().ConfState.Voters, a.ReplicaID) {
		return fault.ErrOperatorAuthorization
	}
	observed, err := h.ObserveGroup(ctx, a.GroupID)
	if err != nil {
		return err
	}
	if err := h.cfg.Authorize(ctx, a); err != nil {
		return err
	}
	if a.Kind == topology.StepDemoteVoter && core.Status().LeaderID == a.ReplicaID && a.TransferReplicaID != 0 {
		if !observed.HealthKnown || !observed.LiveVoters[a.TransferReplicaID] || !slices.Contains(core.Status().ConfState.Voters, a.TransferReplicaID) {
			return fault.New(fault.Unavailable, "transfer_target_unavailable", nil)
		}
		if err := core.TransferLeader(ctx, a.TransferReplicaID); err != nil {
			return err
		}
		// Resolve the new leader and revalidate the current demotion scope
		// on retry; this response does not acknowledge a completed demotion.
		return fault.ErrNotLeader
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
	return h.membership.Apply(ctx, core.Membership(), observedHealth{a.GroupID, observed.LiveVoters, observed.HealthKnown}, change)
}

type observedHealth struct {
	group string
	live  map[uint64]bool
	known bool
}

func (o observedHealth) Live(group string) (map[uint64]bool, bool) {
	return o.live, o.known && group == o.group
}

func consensusEffectPresent(status topology.GroupStatus, a topology.AuthorizedStep) bool {
	cs := status.ConfState
	if len(cs.VotersOutgoing)+len(cs.LearnersNext) != 0 || cs.AutoLeave {
		return false
	}
	voter, learner := slices.Contains(cs.Voters, a.ReplicaID), slices.Contains(cs.Learners, a.ReplicaID)
	switch a.Kind {
	case topology.StepAddLearner, topology.StepDemoteVoter:
		return learner && !voter
	case topology.StepPromoteVoter:
		return voter && !learner
	case topology.StepRemoveReplica:
		return !voter && !learner
	case topology.StepTransferLeader:
		return voter && status.LeaderID == a.ReplicaID
	default:
		return false
	}
}

func (h *Host) verifyLocalEffect(ctx context.Context, req load.ActionRequest) error {
	a := req.Authorization
	switch a.Kind {
	case topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepDeleteGroupState:
		h.mu.Lock()
		slot := h.slots[a.GroupID]
		live := slot != nil && !slot.released
		h.mu.Unlock()
		if live {
			if slot.identity == req.LocalTarget {
				return fault.ErrReplicaNotReady
			}
			if err := h.otherIdentityAllowed(ctx, a, req.LocalTarget, slot.identity); err != nil {
				return err
			}
		}
		if a.Kind == topology.StepStopReplica {
			return nil
		}
		probe, err := h.cfg.Store.Probe(ctx, a.GroupID)
		if err != nil {
			return err
		}
		if probe.Exists {
			if probe.Identity == req.LocalTarget {
				return fault.ErrReplicaNotReady
			}
			if err := h.otherIdentityAllowed(ctx, a, req.LocalTarget, probe.Identity); err != nil {
				return err
			}
		}
		return nil
	case topology.StepCreateFoundingGroup:
		core, err := h.actionCore(req)
		if err != nil {
			return err
		}
		targets, err := h.cfg.ActionTargets(ctx, a)
		if err != nil {
			return err
		}
		want := make([]uint64, 0, len(targets))
		for _, id := range targets {
			want = append(want, id.ReplicaID)
		}
		slices.Sort(want)
		cs := core.Status().ConfState
		got := slices.Clone(cs.Voters)
		slices.Sort(got)
		if !slices.Equal(got, want) || len(cs.Learners)+len(cs.VotersOutgoing)+len(cs.LearnersNext) != 0 || cs.AutoLeave {
			return fault.ErrIdentityMismatch
		}
		return nil
	case topology.StepPrepareReplica:
		_, err := h.actionCore(req)
		return err
	default:
		core, err := h.actionCore(req)
		if err != nil {
			return err
		}
		if !consensusEffectPresent(core.Status(), a) {
			return fault.ErrReplicaNotReady
		}
		return nil
	}
}
