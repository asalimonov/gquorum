package policy

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/internal/votercheck"
	"github.com/asalimonov/gquorum/topology"
)

func chooseVoterChange(in ScheduleInput) ScheduleDecision {
	check := votercheck.Input{Active: in.Active, Metrics: in.Metrics, Status: in.Status, Held: in.Held}
	groups := slices.Clone(in.Active.Groups)
	slices.SortFunc(groups, func(a, b topology.Group) int {
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
	})
	reason := "converged"
	for _, g := range groups {
		if g.VoterPolicy == nil {
			continue
		}
		observed, blocker := votercheck.Group(check, g.ID)
		if blocker != "" {
			if reason == "converged" {
				reason = blocker
			}
			continue
		}
		replicas := slices.Clone(g.Replicas)
		slices.SortFunc(replicas, func(a, b topology.Replica) int { return cmp.Compare(a.NodeName, b.NodeName) })
		voters, learners := []topology.ReplicaKey{}, []topology.ReplicaKey{}
		for _, r := range replicas {
			key := topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}
			if r.Role == topology.ReplicaVoter {
				voters = append(voters, key)
			} else {
				learners = append(learners, key)
			}
		}

		try := func(change topology.VoterChange) *ScheduleDecision {
			if why := votercheck.Check(check, change); why != "" {
				if reason == "converged" {
					reason = why
				}
				return nil
			}
			return &ScheduleDecision{Action: &ScheduleAction{Kind: ReconfigureVoters, VoterChange: &change}, Reason: "voter_policy"}
		}
		switch {
		case observed.NativeVoters < g.VoterPolicy.TargetVoters:
			if len(learners) == 0 {
				reason = "target_shortfall"
			}
			for _, key := range learners {
				if d := try(topology.VoterChange{GroupID: g.ID, Promote: &key}); d != nil {
					return *d
				}
			}
		case observed.NativeVoters > g.VoterPolicy.TargetVoters:
			// Keep the leader when another retained voter can be demoted safely.
			slices.SortStableFunc(voters, func(a, b topology.ReplicaKey) int {
				if a == b {
					return 0
				}
				if a.ReplicaID == observed.LeaderID {
					return 1
				}
				if b.ReplicaID == observed.LeaderID {
					return -1
				}
				return 0
			})
			for _, key := range voters {
				if d := try(topology.VoterChange{GroupID: g.ID, Demote: &key}); d != nil {
					return *d
				}
			}
		default:
			failed := map[topology.ReplicaKey]bool{}
			for _, r := range observed.Replicas {
				if votercheck.ReplacementFailure(*g.VoterPolicy, r.Evidence) {
					failed[topology.ReplicaKey{NodeName: r.Sample.Identity.NodeName, ReplicaID: r.Sample.Identity.ReplicaID}] = true
				}
			}
			for _, voter := range voters {
				if !failed[voter] {
					continue
				}
				for _, learner := range learners {
					if d := try(topology.VoterChange{GroupID: g.ID, Promote: &learner, Demote: &voter}); d != nil {
						return *d
					}
				}
			}
		}

	}
	return ScheduleDecision{Reason: reason}
}
