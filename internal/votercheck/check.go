// Package votercheck contains mandatory, pure eligibility checks shared by the
// built-in chooser and the scheduler driver. Policy code cannot waive them.
package votercheck

import (
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	reasonPolicyRejected     = "policy_rejected"
	reasonUnknownObservation = "unknown_observation"
)

// Input supplies approved/proven topology, collected metrics, native observations and
// exact local holds.
type Input struct {
	Target  topology.Document
	Groups  map[string]topology.GroupProgress
	Metrics metrics.ClusterSnapshot
	Status  map[string]topology.GroupStatus
	Held    []storage.Identity
}

// ReadyGroup selects an exact unowned checkpoint matching current approved intent.
func ReadyGroup(in Input, id string) (*topology.Group, string) {
	row := in.Groups[id]
	if row.OperationID != "" {
		return nil, "group_busy"
	}
	if row.Deleting || row.ConvergedVersion == 0 || row.Converged == nil ||
		!reflect.DeepEqual(row.Converged, in.Target.Group(id)) {
		return nil, "approved_execution"
	}
	return row.Converged, ""
}

// Group checks coherent current group evidence and returns a refusal reason
// when unavailable.
func Group(in Input, id string) (metrics.GroupObservation, string) {
	group, reason := ReadyGroup(in, id)
	if reason != "" {
		return metrics.GroupObservation{}, reason
	}
	if group.VoterConfig == nil {
		return metrics.GroupObservation{}, reasonPolicyRejected
	}
	if !in.Metrics.IsCollectorLeader {
		return metrics.GroupObservation{}, reasonUnknownObservation
	}
	var observed metrics.GroupObservation
	for _, g := range in.Metrics.Groups {
		if g.GroupID == id {
			observed = g
			break
		}
	}
	if observed.SourceVersion != in.Groups[id].ConvergedVersion {
		return observed, reasonUnknownObservation
	}
	if observed.State != metrics.Current {
		return observed, "stale_observation"
	}
	status, ok := in.Status[id]
	if !ok || !status.Found || !status.HealthKnown || status.LeaderID == 0 || status.Term != observed.Term ||
		status.SourceVersion != in.Groups[id].ConvergedVersion ||
		status.LeaderID != observed.LeaderID {
		return observed, reasonUnknownObservation
	}
	return observed, nativeMembership(group, status, observed)
}

func nativeMembership(group *topology.Group, status topology.GroupStatus, observed metrics.GroupObservation) string {
	if len(status.ConfState.VotersOutgoing) != 0 || len(status.ConfState.LearnersNext) != 0 {
		return "approved_execution"
	}
	voters, learners := []uint64{}, []uint64{}
	for _, r := range group.Replicas {
		if r.Role == topology.ReplicaVoter {
			voters = append(voters, r.ReplicaID)
		} else {
			learners = append(learners, r.ReplicaID)
		}
	}
	if observed.NativeVoters != len(voters) ||
		!sameMembers(voters, status.ConfState.Voters) || !sameMembers(learners, status.ConfState.Learners) {
		return "approved_execution"
	}
	live := 0
	for _, id := range voters {
		if status.LiveVoters[id] {
			live++
		}
	}
	if live < topology.QuorumOf(len(voters)) {
		return "quorum_lost"
	}
	if !leaderObservation(observed, voters, learners) {
		return reasonUnknownObservation
	}
	return ""
}

func sameMembers(a, b []uint64) bool {
	left, right := slices.Clone(a), slices.Clone(b)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

func leaderObservation(observed metrics.GroupObservation, voters, learners []uint64) bool {
	for _, r := range observed.Replicas {
		s := r.Sample
		if s.Identity.ReplicaID == observed.LeaderID && s.LeaderID == observed.LeaderID && s.Term == observed.Term &&
			s.MembershipDigest == observed.MembershipDigest && s.Meter.State == metrics.Current &&
			s.Raft.State == metrics.Current && sameMembers(voters, s.Voters) && sameMembers(learners, s.Learners) {
			return true
		}
	}
	return false
}

func findReplica(group *topology.Group, key topology.ReplicaKey) (topology.Replica, bool) {
	for _, r := range group.Replicas {
		if r.ReplicaID == key.ReplicaID && r.NodeName == key.NodeName {
			return r, true
		}
	}
	return topology.Replica{}, false
}

func observation(g metrics.GroupObservation, key topology.ReplicaKey,
	cluster string) (metrics.ReplicaObservation, bool) {
	for _, r := range g.Replicas {
		id := r.Sample.Identity
		if id.ClusterID == cluster && id.GroupID == g.GroupID && id.NodeName == key.NodeName &&
			id.ReplicaID == key.ReplicaID {
			return r, true
		}
	}
	return metrics.ReplicaObservation{}, false
}
func duration(v metrics.DurationValue, w time.Duration) bool {
	return v.State == metrics.Current && v.Value >= w
}

// ReplacementFailure reports whether known failure windows permit voter
// replacement.
func ReplacementFailure(p topology.VoterConfig, e metrics.Evidence) bool {
	return duration(e.UnavailableFor, p.FailureWindow) || duration(e.ReplicationLagFor, p.FailureWindow) ||
		p.ReplaceNoProgress && duration(e.NoProgressFor, p.NoProgressWindow)
}

// Check returns an empty reason only when a requested voter change passes
// mandatory eligibility checks.
func Check(in Input, change topology.VoterChange) string {
	observed, reason := Group(in, change.GroupID)
	if reason != "" {
		return reason
	}
	group := in.Target.Group(change.GroupID)
	p := *group.VoterConfig
	if !validChangeShape(change, observed.NativeVoters, p.TargetVoters) {
		return reasonPolicyRejected
	}
	if change.Promote != nil {
		if reason := promotionEligibility(in, group, observed, *change.Promote); reason != "" {
			return reason
		}
	}
	if change.Demote != nil {
		if reason := demotionEligibility(in, group, observed, change); reason != "" {
			return reason
		}
	}
	return projectedPlacement(in.Target, change)
}

func validChangeShape(change topology.VoterChange, current, target int) bool {
	switch {
	case change.Promote == nil && change.Demote == nil:
		return false
	case change.Promote != nil && change.Demote != nil:
		return *change.Promote != *change.Demote && current == target
	case change.Promote != nil:
		return current < target
	default:
		return current > target
	}
}

func promotionEligibility(in Input, group *topology.Group, observed metrics.GroupObservation,
	key topology.ReplicaKey) string {
	p := *group.VoterConfig
	r, found := findReplica(group, key)
	o, known := observation(observed, key, in.Target.ClusterID)
	node := in.Target.Node(key.NodeName)
	if !found || r.Role != topology.ReplicaLearner || node == nil || node.State != topology.NodeActive {
		return reasonPolicyRejected
	}
	if r.PinnedLearner {
		return "pinned_learner"
	}
	if slices.Contains(in.Held,
		storage.Identity{ClusterID: in.Target.ClusterID, GroupID: group.ID, NodeName: r.NodeName,
			ReplicaID: r.ReplicaID}) {
		return "local_stop"
	}
	if !known || !currentReplicaObservation(o) {
		return reasonUnknownObservation
	}
	if o.Evidence.DemotionCooldownRemaining > 0 {
		return "replica_cooldown"
	}
	if o.Sample.Term != observed.Term || o.Sample.LeaderID != observed.LeaderID ||
		o.Sample.MembershipDigest != observed.MembershipDigest ||
		o.ReplicationLag.State != metrics.Current || o.ReplicationLag.Value > p.StandbyLagEntries ||
		o.ApplyBacklog.State != metrics.Current || o.ApplyBacklog.Value > p.StandbyApplyBacklogEntries ||
		!slices.Contains(o.Sample.Learners, r.ReplicaID) ||
		!duration(o.Evidence.StandbyReadyFor, p.StandbyWindow) {
		return "standby_window"
	}
	if !in.Status[group.ID].LiveVoters[r.ReplicaID] {
		return reasonUnknownObservation
	}
	return ""
}

func currentReplicaObservation(o metrics.ReplicaObservation) bool {
	return o.State == metrics.Current && o.Reachability == "alive" && o.Sample.Identity.RunID != "" &&
		o.Sample.Identity.InstanceID != "" && o.Sample.Ready && o.Sample.Running && !o.Sample.Restoring &&
		o.Sample.WALStall <= time.Second && o.Sample.Raft.State == metrics.Current &&
		o.Sample.Writer.State == metrics.Current && o.Sample.Meter.State == metrics.Current
}

func demotionEligibility(in Input, group *topology.Group, observed metrics.GroupObservation,
	change topology.VoterChange) string {
	p := *group.VoterConfig
	r, found := findReplica(group, *change.Demote)
	if !found || r.Role != topology.ReplicaVoter || r.PinnedLearner {
		return reasonPolicyRejected
	}
	if change.Promote != nil {
		o, known := observation(observed, *change.Demote, in.Target.ClusterID)
		if !known || !ReplacementFailure(p, o.Evidence) {
			return "failure_window"
		}
		if observed.CooldownRemaining > 0 && (!p.AllowNewFailureDuringCooldown ||
			!slices.Contains(o.Evidence.Reasons, "new_failure")) {
			return "group_cooldown"
		}
	}
	return ""
}

func projectedPlacement(active topology.Document, change topology.VoterChange) string {
	group := active.Group(change.GroupID)
	if group == nil {
		return reasonPolicyRejected
	}
	keys := make([]topology.ReplicaKey, 0, len(group.Replicas))
	for _, replica := range group.Replicas {
		if replica.Role == topology.ReplicaVoter && (change.Demote == nil ||
			replica.NodeName != change.Demote.NodeName || replica.ReplicaID != change.Demote.ReplicaID) {
			keys = append(keys, topology.ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID})
		}
	}
	if change.Promote != nil {
		if reason := promotionPlacement(active, change); reason != "" {
			return reason
		}
		keys = append(keys, *change.Promote)
	}
	if err := topology.ValidateVoterPlacement(active, change.GroupID, keys); err != nil {
		if group.VoterConfig != nil && group.VoterConfig.DistinctVoterLabel != "" {
			return "voter_domain_conflict"
		}
		return "placement_unsatisfied"
	}
	return ""
}

func promotionPlacement(active topology.Document, change topology.VoterChange) string {
	report, err := topology.EvaluatePlacement(topology.PlacementInput{Approved: active, Desired: active,
		GroupID: change.GroupID})
	if err != nil {
		return "placement_unsatisfied"
	}
	for _, candidate := range report.Candidates {
		if candidate.NodeName != change.Promote.NodeName {
			continue
		}
		if !candidate.CanVote {
			return candidateRefusal(candidate)
		}
		break
	}
	return ""
}

func candidateRefusal(candidate topology.PlacementCandidate) string {
	for _, reason := range candidate.Reasons {
		if reason.Code == "node_learner_only" || reason.Code == "voter_domain_missing" ||
			reason.Code == "selector_key_missing" || reason.Code == "selector_value_mismatch" ||
			reason.Code == "node_reserved" {
			return reason.Code
		}
	}
	return "placement_unsatisfied"
}
