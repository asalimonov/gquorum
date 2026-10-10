// Package events provides bounded live subscriptions and an independent local journal.
package events

import "time"

type Type string

const (
	LeaderChange     Type = "leader_change"
	QuorumLost       Type = "quorum_lost"
	QuorumAchieved   Type = "quorum_achieved"
	NodeAlive        Type = "node_alive"
	NodeDead         Type = "node_dead"
	ExecutionChanged Type = "execution_changed"
	DrainCompleted   Type = "drain_completed"
	RuntimeError     Type = "runtime_error"
)

type Event struct {
	Seq           uint64    `json:"seq"`
	At            time.Time `json:"at"`
	Type          Type      `json:"type"`
	NodeName      string    `json:"node_name"`
	InstanceID    string    `json:"instance_id"`
	ClusterID     string    `json:"cluster_id"`
	GroupID       string    `json:"group_id,omitempty"`
	SubjectNode   string    `json:"subject_node,omitempty"`
	OldLeader     string    `json:"old_leader,omitempty"`
	NewLeader     string    `json:"new_leader,omitempty"`
	Term          uint64    `json:"term,omitempty"`
	HealthyVoters *int      `json:"healthy_voters,omitempty"`
	Voters        int       `json:"voters,omitempty"`
	ExecutionID   string    `json:"execution_id,omitempty"`
	Code          string    `json:"code,omitempty"`
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
