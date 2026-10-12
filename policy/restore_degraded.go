package policy

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type RestoreDegradedOptions struct{ StableFor time.Duration }

// RestoreDegraded converges replicated voter policies using stable learners.
// Legacy groups retain missing-voter repair. Pinned learners and healthy seats
// are preserved; every action is independently checked by the node.
type RestoreDegraded struct{ stableFor time.Duration }

func NewRestoreDegraded(opts RestoreDegradedOptions) (*RestoreDegraded, error) {
	if opts.StableFor < 0 {
		return nil, fault.New(fault.Invalid, "negative stability duration", nil)
	}
	if opts.StableFor == 0 {
		opts.StableFor = time.Minute
	}
	return &RestoreDegraded{stableFor: opts.StableFor}, nil
}

func (p *RestoreDegraded) Schedule(ctx context.Context, in ScheduleInput) (ScheduleDecision, error) {
	if err := ctx.Err(); err != nil {
		return ScheduleDecision{}, err
	}
	if p == nil {
		return ScheduleDecision{}, fault.ErrInvalid
	}
	voterDecision := chooseVoterChange(in)
	if voterDecision.Action != nil {
		return voterDecision, nil
	}
	stableFor := p.stableFor
	if stableFor == 0 {
		stableFor = time.Minute
	}
	sawLost, sawWaiting, sawHeld := false, false, false
	for _, group := range in.Active.Groups {
		if err := ctx.Err(); err != nil {
			return ScheduleDecision{}, err
		}
		if group.ID == topology.MetaGroupID || group.VoterPolicy != nil {
			continue
		}
		status, ok := in.Status[group.ID]
		if !ok || !status.Found {
			continue
		}
		for _, replica := range group.Replicas {
			node := in.Active.Node(replica.NodeName)
			if replica.Role != topology.ReplicaVoter || node == nil || node.State != topology.NodeActive {
				continue
			}
			if slices.Contains(status.ConfState.Voters, replica.ReplicaID) || slices.Contains(status.ConfState.Learners, replica.ReplicaID) {
				continue
			}
			identity := storage.Identity{ClusterID: in.Active.ClusterID, NodeName: replica.NodeName, GroupID: group.ID, ReplicaID: replica.ReplicaID}
			if slices.Contains(in.Held, identity) {
				sawHeld = true
				continue
			}
			if in.Health[group.ID].ObservedHealth.State.QuorumLost() {
				sawLost = true
				continue
			}
			if in.Alive[replica.NodeName] < stableFor {
				sawWaiting = true
				continue
			}
			return ScheduleDecision{Action: &ScheduleAction{Kind: RestoreMissingReplica, Identity: identity}, Reason: "missing_replica"}, nil
		}
	}
	reason := voterDecision.Reason
	switch {
	case sawLost:
		reason = "quorum_lost"
	case sawWaiting:
		reason = "stability_window"
	case sawHeld:
		reason = "local_stop"
	}
	return ScheduleDecision{Reason: reason}, nil
}

var _ Scheduler = (*RestoreDegraded)(nil)
