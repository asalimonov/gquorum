package scheduling

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/internal/votercheck"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func chooseVoterChange(in ScheduleInput, group topology.Group) ScheduleDecision {
	scan := voterRepairScan{input: votercheck.Input{Target: in.Target, Groups: in.Groups, Metrics: in.Metrics,
		Status: in.Status, Held: in.Held}, reason: "converged"}
	observed, blocker := votercheck.Group(scan.input, group.ID)
	if blocker != "" {
		return ScheduleDecision{Reason: blocker}
	}
	if decision := scan.chooseGroup(group, observed); decision != nil {
		return *decision
	}
	return ScheduleDecision{Reason: scan.reason}
}

func compareRepairGroups(a, b topology.Group) int {
	if a.ID == b.ID {
		return 0
	}
	if a.ID == topology.MetaGroupID {
		return -1
	}
	if b.ID == topology.MetaGroupID {
		return 1
	}
	return cmp.Compare(a.ID, b.ID)
}

type voterRepairScan struct {
	input  votercheck.Input
	reason string
}

type voterCandidates struct{ voters, learners []topology.ReplicaKey }

func repairCandidates(g topology.Group) voterCandidates {
	replicas := slices.Clone(g.Replicas)
	slices.SortFunc(replicas, func(a, b topology.Replica) int { return cmp.Compare(a.NodeName, b.NodeName) })
	candidates := voterCandidates{voters: []topology.ReplicaKey{}, learners: []topology.ReplicaKey{}}
	for _, r := range replicas {
		key := topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}
		if r.Role == topology.ReplicaVoter {
			candidates.voters = append(candidates.voters, key)
		} else {
			candidates.learners = append(candidates.learners, key)
		}
	}
	return candidates
}

func (s *voterRepairScan) try(change topology.VoterChange) *ScheduleDecision {
	if why := votercheck.Check(s.input, change); why != "" {
		if s.reason == "converged" {
			s.reason = why
		}
		return nil
	}
	return &ScheduleDecision{Action: &ScheduleAction{Kind: ReconfigureVoters,
		VoterChange: &change}, Reason: "voter_config"}
}

func (s *voterRepairScan) chooseGroup(g topology.Group, observed metrics.GroupObservation) *ScheduleDecision {
	candidates := repairCandidates(g)
	switch {
	case observed.NativeVoters < g.VoterConfig.TargetVoters:
		if len(candidates.learners) == 0 {
			s.reason = "target_shortfall"
		}
		for _, key := range candidates.learners {
			if d := s.try(topology.VoterChange{GroupID: g.ID, Promote: &key}); d != nil {
				return d
			}
		}
	case observed.NativeVoters > g.VoterConfig.TargetVoters:
		// Keep the leader when another retained voter can be demoted safely.
		preferFollowerDemotion(candidates.voters, observed.LeaderID)
		for _, key := range candidates.voters {
			if d := s.try(topology.VoterChange{GroupID: g.ID, Demote: &key}); d != nil {
				return d
			}
		}
	default:
		return s.replaceFailedVoter(g, observed, candidates)
	}
	return nil
}

func preferFollowerDemotion(voters []topology.ReplicaKey, leaderID uint64) {
	slices.SortStableFunc(voters, func(a, b topology.ReplicaKey) int {
		if a == b {
			return 0
		}
		if a.ReplicaID == leaderID {
			return 1
		}
		if b.ReplicaID == leaderID {
			return -1
		}
		return 0
	})
}

func (s *voterRepairScan) replaceFailedVoter(g topology.Group, observed metrics.GroupObservation,
	candidates voterCandidates) *ScheduleDecision {
	failed := map[topology.ReplicaKey]bool{}
	for _, r := range observed.Replicas {
		if votercheck.ReplacementFailure(*g.VoterConfig, r.Evidence) {
			failed[topology.ReplicaKey{NodeName: r.Sample.Identity.NodeName,
				ReplicaID: r.Sample.Identity.ReplicaID}] = true
		}
	}
	for _, voter := range candidates.voters {
		if !failed[voter] {
			continue
		}
		for _, learner := range candidates.learners {
			if d := s.try(topology.VoterChange{GroupID: g.ID, Promote: &learner, Demote: &voter}); d != nil {
				return d
			}
		}
	}
	return nil
}
