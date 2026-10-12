// Package votercheck contains mandatory, pure eligibility checks shared by the
// built-in chooser and the scheduler driver. Policy code cannot waive them.
package votercheck

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type Input struct {
	Active  topology.Document
	Metrics metrics.ClusterSnapshot
	Status  map[string]topology.GroupStatus
	Held    []storage.Identity
}

func Group(in Input, id string) (metrics.GroupObservation, string) {
	group := in.Active.Group(id)
	if group == nil || group.VoterPolicy == nil {
		return metrics.GroupObservation{}, "policy_rejected"
	}
	if !in.Metrics.IsCollectorLeader || in.Metrics.ActiveVersion != in.Active.Version {
		return metrics.GroupObservation{}, "unknown_observation"
	}
	var observed metrics.GroupObservation
	for _, g := range in.Metrics.Groups {
		if g.GroupID == id {
			observed = g
			break
		}
	}
	if observed.State != metrics.Current {
		return observed, "stale_observation"
	}
	status, ok := in.Status[id]
	if !ok || !status.Found || !status.HealthKnown || status.LeaderID == 0 || status.Term != observed.Term || status.LeaderID != observed.LeaderID {
		return observed, "unknown_observation"
	}
	if len(status.ConfState.VotersOutgoing) != 0 || len(status.ConfState.LearnersNext) != 0 {
		return observed, "approved_execution"
	}
	voters, learners := []uint64{}, []uint64{}
	for _, r := range group.Replicas {
		if r.Role == topology.ReplicaVoter {
			voters = append(voters, r.ReplicaID)
		} else {
			learners = append(learners, r.ReplicaID)
		}
	}
	same := func(a, b []uint64) bool {
		a, b = slices.Clone(a), slices.Clone(b)
		slices.Sort(a)
		slices.Sort(b)
		return slices.Equal(a, b)
	}
	if !same(voters, status.ConfState.Voters) || !same(learners, status.ConfState.Learners) || observed.NativeVoters != len(voters) {
		return observed, "approved_execution"
	}
	live := 0
	for _, id := range voters {
		if status.LiveVoters[id] {
			live++
		}
	}
	if live < topology.QuorumOf(len(voters)) {
		return observed, "quorum_lost"
	}
	leaderFound := false
	for _, r := range observed.Replicas {
		s := r.Sample
		if s.Identity.ReplicaID == observed.LeaderID && s.LeaderID == observed.LeaderID && s.Term == observed.Term && s.MembershipDigest == observed.MembershipDigest && s.Meter.State == metrics.Current && s.Raft.State == metrics.Current && same(voters, s.Voters) && same(learners, s.Learners) {
			leaderFound = true
		}
	}
	if !leaderFound {
		return observed, "unknown_observation"
	}
	return observed, ""
}
func findReplica(group *topology.Group, key topology.ReplicaKey) (topology.Replica, bool) {
	for _, r := range group.Replicas {
		if r.ReplicaID == key.ReplicaID && r.NodeName == key.NodeName {
			return r, true
		}
	}
	return topology.Replica{}, false
}
func observation(g metrics.GroupObservation, key topology.ReplicaKey, cluster string) (metrics.ReplicaObservation, bool) {
	for _, r := range g.Replicas {
		id := r.Sample.Identity
		if id.ClusterID == cluster && id.GroupID == g.GroupID && id.NodeName == key.NodeName && id.ReplicaID == key.ReplicaID {
			return r, true
		}
	}
	return metrics.ReplicaObservation{}, false
}
func duration(v metrics.DurationValue, w time.Duration) bool {
	return v.State == metrics.Current && v.Value >= w
}
func ReplacementFailure(p topology.VoterPolicy, e metrics.Evidence) bool {
	return duration(e.UnavailableFor, p.FailureWindow) || duration(e.ReplicationLagFor, p.FailureWindow) || p.ReplaceNoProgress && duration(e.NoProgressFor, p.NoProgressWindow)
}
func Check(in Input, change topology.VoterChange) string {
	observed, reason := Group(in, change.GroupID)
	if reason != "" {
		return reason
	}
	group := in.Active.Group(change.GroupID)
	p := *group.VoterPolicy
	if change.Promote == nil && change.Demote == nil {
		return "policy_rejected"
	}
	if change.Promote != nil && change.Demote != nil {
		if *change.Promote == *change.Demote || observed.NativeVoters != p.TargetVoters {
			return "policy_rejected"
		}
	} else if change.Promote != nil && observed.NativeVoters >= p.TargetVoters || change.Demote != nil && change.Promote == nil && observed.NativeVoters <= p.TargetVoters {
		return "policy_rejected"
	}
	if change.Promote != nil {
		r, found := findReplica(group, *change.Promote)
		o, known := observation(observed, *change.Promote, in.Active.ClusterID)
		node := in.Active.Node(change.Promote.NodeName)
		if !found || r.Role != topology.ReplicaLearner || node == nil || node.State != topology.NodeActive {
			return "policy_rejected"
		}
		if r.PinnedLearner {
			return "pinned_learner"
		}
		if slices.Contains(in.Held, storage.Identity{ClusterID: in.Active.ClusterID, GroupID: group.ID, NodeName: r.NodeName, ReplicaID: r.ReplicaID}) {
			return "local_stop"
		}
		if !known || o.State != metrics.Current || o.Reachability != "alive" || o.Sample.Identity.RunID == "" || o.Sample.Identity.InstanceID == "" || !o.Sample.Ready || !o.Sample.Running || o.Sample.Restoring || o.Sample.WALStall > time.Second || o.Sample.Raft.State != metrics.Current || o.Sample.Writer.State != metrics.Current || o.Sample.Meter.State != metrics.Current {
			return "unknown_observation"
		}
		if o.Evidence.DemotionCooldownRemaining > 0 {
			return "replica_cooldown"
		}
		if o.Sample.Term != observed.Term || o.Sample.LeaderID != observed.LeaderID || o.Sample.MembershipDigest != observed.MembershipDigest || !slices.Contains(o.Sample.Learners, r.ReplicaID) || o.ReplicationLag.State != metrics.Current || o.ReplicationLag.Value > p.StandbyLagEntries || o.ApplyBacklog.State != metrics.Current || o.ApplyBacklog.Value > p.StandbyApplyBacklogEntries || !duration(o.Evidence.StandbyReadyFor, p.StandbyWindow) {
			return "standby_window"
		}
		if !in.Status[group.ID].LiveVoters[r.ReplicaID] {
			return "unknown_observation"
		}
	}
	if change.Demote != nil {
		r, found := findReplica(group, *change.Demote)
		if !found || r.Role != topology.ReplicaVoter || r.PinnedLearner {
			return "policy_rejected"
		}
		if change.Promote != nil {
			o, known := observation(observed, *change.Demote, in.Active.ClusterID)
			if !known || !ReplacementFailure(p, o.Evidence) {
				return "failure_window"
			}
			if observed.CooldownRemaining > 0 && (!p.AllowNewFailureDuringCooldown || !slices.Contains(o.Evidence.Reasons, "new_failure")) {
				return "group_cooldown"
			}
		}
	}
	return ""
}
