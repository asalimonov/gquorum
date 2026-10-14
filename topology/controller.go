package topology

import (
	"maps"
	"slices"
)

// Health classifies an observed node's reachability.
type Health string

// HealthUnknown and the following values distinguish unknown, alive and dead
// node observations.
const (
	HealthUnknown Health = "unknown"
	HealthAlive   Health = "alive"
	HealthDead    Health = "dead"
)

// GroupHealth classifies the observed availability of a group's voters.
type GroupHealth string

// GroupUnknown and the following values distinguish unknown health, degradation
// and quorum loss.
const (
	GroupUnknown    GroupHealth = "unknown"
	GroupHealthy    GroupHealth = "healthy"
	GroupDegraded   GroupHealth = "degraded"
	GroupQuorumLost GroupHealth = "quorum_lost"
)

// ReplicaProgress reports an observed replication match index and recent
// activity.
type ReplicaProgress struct {
	Match        uint64 `json:"match"`
	RecentActive bool   `json:"recent_active"`
}

// Clone copies all membership slices and progress and liveness maps.
func (s GroupStatus) Clone() GroupStatus {
	s.ConfState.Voters = slices.Clone(s.ConfState.Voters)
	s.ConfState.Learners = slices.Clone(s.ConfState.Learners)
	s.ConfState.VotersOutgoing = slices.Clone(s.ConfState.VotersOutgoing)
	s.ConfState.LearnersNext = slices.Clone(s.ConfState.LearnersNext)
	s.Progress = maps.Clone(s.Progress)
	s.LiveVoters = maps.Clone(s.LiveVoters)
	return s
}
