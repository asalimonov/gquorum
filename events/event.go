// Package events provides bounded live subscriptions and an independent local journal.
package events

import "time"

type Type string

const (
	LeaderChange       Type = "leader_change"
	QuorumLost         Type = "quorum_lost"
	QuorumAchieved     Type = "quorum_achieved"
	NodeAlive          Type = "node_alive"
	NodeDead           Type = "node_dead"
	OperatorChanged    Type = "operator_changed"
	DrainCompleted     Type = "drain_completed"
	RuntimeError       Type = "runtime_error"
	SchedulingChanged  Type = "scheduling_changed"
	GroupHealthChanged Type = "group_health_changed"
	SelfRemoved        Type = "self_removed"
)

type Event struct {
	Seq         uint64    `json:"seq"`
	At          time.Time `json:"at"`
	Type        Type      `json:"type"`
	NodeName    string    `json:"node_name"`
	InstanceID  string    `json:"instance_id"`
	MemberID    uint64    `json:"member_id,omitempty"`
	ClusterID   string    `json:"cluster_id"`
	GroupID     string    `json:"group_id,omitempty"`
	SubjectNode string    `json:"subject_node,omitempty"`
	// OldLeader and NewLeader are decimal Raft IDs for LeaderChange events:
	// MemberID for _meta, ReplicaID for load groups, never logical node names.
	// "0" means no known leader; an empty field means it was not supplied.
	OldLeader     string `json:"old_leader,omitempty"`
	NewLeader     string `json:"new_leader,omitempty"`
	Term          uint64 `json:"term,omitempty"`
	HealthyVoters *int   `json:"healthy_voters,omitempty"`
	Voters        int    `json:"voters,omitempty"`
	OperatorID    string `json:"operator_id,omitempty"`
	Code          string `json:"code,omitempty"`
	State         string `json:"state,omitempty"`
	Version       uint64 `json:"version,omitempty"`
	Attempt       uint32 `json:"attempt,omitempty"`
	StepSeq       int    `json:"step_seq,omitempty"`
	ReplicaID     uint64 `json:"replica_id,omitempty"`
}
type Filter struct {
	GroupID string
	Types   []Type
}
type Delivery struct {
	Event      Event
	LostBefore uint64
}
type Query struct {
	AfterSeq uint64
	Limit    int
	Filter   Filter
}

func (f Filter) matches(e Event) bool {
	if f.GroupID != "" && f.GroupID != e.GroupID {
		return false
	}
	if len(f.Types) == 0 {
		return true
	}
	for _, typ := range f.Types {
		if typ == e.Type {
			return true
		}
	}
	return false
}
func clone(e Event) Event {
	if e.HealthyVoters != nil {
		n := *e.HealthyVoters
		e.HealthyVoters = &n
	}
	return e
}
