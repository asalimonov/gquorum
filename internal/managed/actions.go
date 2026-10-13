package managed

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func localAction(kind topology.StepKind) bool {
	switch kind {
	case topology.StepCreateFoundingGroup, topology.StepPrepareReplica, topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepDeleteGroupState:
		return true
	default:
		return false
	}
}

func (h *Host) ApplyAction(ctx context.Context, req load.ActionRequest) (load.ActionResult, error) {
	return h.applyGrantedAction(ctx, req)
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
			if err := h.otherIdentityAllowed(ctx, a, req.LocalTarget, slot.identity, req.Grant); err != nil {
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
			if err := h.otherIdentityAllowed(ctx, a, req.LocalTarget, probe.Identity, req.Grant); err != nil {
				return err
			}
		}
		return nil
	case topology.StepCreateFoundingGroup:
		core, err := h.actionCore(req)
		if err != nil {
			return err
		}
		targets, err := actionTargets(ctx, a, req.Grant)
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
