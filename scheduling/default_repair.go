package scheduling

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/votercheck"
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
type DefaultRepairPolicy struct {
	mu        sync.Mutex
	stableFor time.Duration
	lastGroup string
}

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
	p.mu.Lock()
	defer p.mu.Unlock()
	scan := legacyRepairScan{input: in, stableFor: p.stableFor}
	if scan.stableFor == 0 {
		scan.stableFor = time.Minute
	}
	reason := "converged"
	for _, group := range p.repairGroups(in.Target) {
		if err := ctx.Err(); err != nil {
			return ScheduleDecision{}, err
		}
		decision := scan.choose(group)
		if decision.Action != nil {
			p.lastGroup = group.ID
			return decision, nil
		}
		if reason == "converged" && decision.Reason != "" {
			reason = decision.Reason
		}
	}
	return ScheduleDecision{Reason: scan.reason(reason)}, nil
}

// The cursor records selection, not approval, so a repeatedly refused candidate
// cannot monopolize policy calls. Shared policy instances serialize this hint.
func (p *DefaultRepairPolicy) repairGroups(target topology.Document) []topology.Group {
	groups := slices.Clone(target.Groups)
	slices.SortFunc(groups, compareRepairGroups)
	if p.lastGroup == "" {
		return groups
	}
	next := slices.IndexFunc(groups, func(group topology.Group) bool {
		return compareRepairGroups(group, topology.Group{ID: p.lastGroup}) > 0
	})
	if next > 0 {
		groups = append(slices.Clone(groups[next:]), groups[:next]...)
	}
	return groups
}

func (s *legacyRepairScan) choose(group topology.Group) ScheduleDecision {
	_, reason := votercheck.ReadyGroup(votercheck.Input{Target: s.input.Target, Groups: s.input.Groups}, group.ID)
	if reason != "" {
		return ScheduleDecision{Reason: reason}
	}
	if group.VoterConfig != nil {
		return chooseVoterChange(s.input, group)
	}
	if group.ID == topology.MetaGroupID {
		return ScheduleDecision{}
	}
	status, found := s.input.Status[group.ID]
	if !found || !status.Found || status.GroupID != group.ID ||
		status.SourceVersion != s.input.Groups[group.ID].ConvergedVersion {
		return ScheduleDecision{}
	}
	if action := s.missingReplica(group, status); action != nil {
		return ScheduleDecision{Action: action, Reason: "missing_replica"}
	}
	return ScheduleDecision{}
}

type legacyRepairScan struct {
	input                         ScheduleInput
	stableFor                     time.Duration
	lost, waiting, held, reserved bool
}

func (s *legacyRepairScan) missingReplica(group topology.Group, status topology.GroupStatus) *ScheduleAction {
	for _, replica := range group.Replicas {
		node := s.input.Target.Node(replica.NodeName)
		if replica.Role != topology.ReplicaVoter || node == nil || node.State != topology.NodeActive {
			continue
		}
		if slices.Contains(status.ConfState.Voters, replica.ReplicaID) ||
			slices.Contains(status.ConfState.Learners, replica.ReplicaID) {
			continue
		}
		identity := storage.Identity{
			ClusterID: s.input.Target.ClusterID,
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
