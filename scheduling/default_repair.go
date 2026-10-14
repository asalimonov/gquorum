package scheduling

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// DefaultRepairOptions configures the stability window for legacy missing-voter
// repair.
type DefaultRepairOptions struct{ StableFor time.Duration }

// DefaultRepairPolicy converges replicated voter configurations using stable
// learners.
// Legacy groups retain missing-voter repair. Pinned learners and healthy seats
// are preserved; every action is independently checked by the node.
type DefaultRepairPolicy struct{ stableFor time.Duration }

// NewDefaultRepairPolicy validates the stability window and applies its
// one-minute default.
func NewDefaultRepairPolicy(opts DefaultRepairOptions) (*DefaultRepairPolicy, error) {
	if opts.StableFor < 0 {
		return nil, fault.New(fault.Invalid, "negative stability duration", nil)
	}
	if opts.StableFor == 0 {
		opts.StableFor = time.Minute
	}
	return &DefaultRepairPolicy{stableFor: opts.StableFor}, nil
}

// Schedule selects a voter-configuration repair or a stable missing legacy
// voter replacement.
func (p *DefaultRepairPolicy) Schedule(ctx context.Context, in ScheduleInput) (ScheduleDecision, error) {
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
	scan := legacyRepairScan{input: in, stableFor: stableFor}
	for _, group := range in.Active.Groups {
		if err := ctx.Err(); err != nil {
			return ScheduleDecision{}, err
		}
		if group.ID == topology.MetaGroupID || group.VoterConfig != nil {
			continue
		}
		status, ok := in.Status[group.ID]
		if !ok || !status.Found {
			continue
		}
		if action := scan.missingReplica(group, status); action != nil {
			return ScheduleDecision{Action: action, Reason: "missing_replica"}, nil
		}
	}
	return ScheduleDecision{Reason: scan.reason(voterDecision.Reason)}, nil
}

type legacyRepairScan struct {
	input                         ScheduleInput
	stableFor                     time.Duration
	lost, waiting, held, reserved bool
}

func (s *legacyRepairScan) missingReplica(group topology.Group, status topology.GroupStatus) *ScheduleAction {
	for _, replica := range group.Replicas {
		node := s.input.Active.Node(replica.NodeName)
		if replica.Role != topology.ReplicaVoter || node == nil || node.State != topology.NodeActive {
			continue
		}
		if slices.Contains(status.ConfState.Voters, replica.ReplicaID) ||
			slices.Contains(status.ConfState.Learners, replica.ReplicaID) {
			continue
		}
		identity := storage.Identity{
			ClusterID: s.input.Active.ClusterID,
			NodeName:  replica.NodeName,
			GroupID:   group.ID,
			ReplicaID: replica.ReplicaID,
		}
		if slices.Contains(s.input.Held, identity) {
			s.held = true
			continue
		}
		if topology.NodeReserved(*node) {
			s.reserved = true // repair allocates a successor load identity
			continue
		}
		if s.input.Health[group.ID].ObservedHealth.State.QuorumLost() {
			s.lost = true
			continue
		}
		if s.input.Alive[replica.NodeName] < s.stableFor {
			s.waiting = true
			continue
		}
		return &ScheduleAction{Kind: RestoreMissingReplica, Identity: identity}
	}
	return nil
}

func (s *legacyRepairScan) reason(initial string) string {
	reason := initial
	switch {
	case s.lost:
		reason = "quorum_lost"
	case s.waiting:
		reason = "stability_window"
	case s.held:
		reason = "local_stop"
	case s.reserved:
		reason = "node_reserved"
	default: // Keep the initial reason when no waiting condition applies.
	}
	return reason
}

var _ RepairPolicy = (*DefaultRepairPolicy)(nil)
