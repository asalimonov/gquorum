package events

import (
	"encoding/json"
	"time"
)

// diskEvent is the original schema-1 journal record. Public API names must not
// silently rewrite historical fields or the stored event-type vocabulary.
type diskEvent struct {
	Seq           uint64    `json:"seq"`
	At            time.Time `json:"at"`
	Type          Type      `json:"type"`
	NodeName      string    `json:"node_name"`
	InstanceID    string    `json:"instance_id"`
	MemberID      uint64    `json:"member_id,omitempty"`
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
	State         string    `json:"state,omitempty"`
	Version       uint64    `json:"version,omitempty"`
	Attempt       uint32    `json:"attempt,omitempty"`
	StepSeq       int       `json:"step_seq,omitempty"`
	ReplicaID     uint64    `json:"replica_id,omitempty"`
}

func encodeEvent(e Event) ([]byte, error) {
	d := diskEvent{
		Seq:           e.Seq,
		At:            e.At,
		Type:          e.Type,
		NodeName:      e.NodeName,
		InstanceID:    e.InstanceID,
		MemberID:      e.MemberID,
		ClusterID:     e.ClusterID,
		GroupID:       e.GroupID,
		SubjectNode:   e.SubjectNode,
		OldLeader:     e.OldLeader,
		NewLeader:     e.NewLeader,
		Term:          e.Term,
		HealthyVoters: e.HealthyVoters,
		Voters:        e.Voters,
		ExecutionID:   e.OperatorID,
		Code:          e.Code,
		State:         e.State,
		Version:       e.Version,
		Attempt:       e.Attempt,
		StepSeq:       e.StepSeq,
		ReplicaID:     e.ReplicaID,
	}
	switch e.Type {
	case OperatorChanged:
		d.Type = "execution_changed"
	case SchedulingChanged:
		d.Type = "reconciliation_changed"
	default: // Retain the event type when no legacy encoding applies.
	}
	return json.Marshal(d)
}

func decodeEvent(data []byte) (Event, error) {
	var d diskEvent
	if err := json.Unmarshal(data, &d); err != nil {
		return Event{}, err
	}
	e := Event{
		Seq:           d.Seq,
		At:            d.At,
		Type:          d.Type,
		NodeName:      d.NodeName,
		InstanceID:    d.InstanceID,
		MemberID:      d.MemberID,
		ClusterID:     d.ClusterID,
		GroupID:       d.GroupID,
		SubjectNode:   d.SubjectNode,
		OldLeader:     d.OldLeader,
		NewLeader:     d.NewLeader,
		Term:          d.Term,
		HealthyVoters: d.HealthyVoters,
		Voters:        d.Voters,
		OperatorID:    d.ExecutionID,
		Code:          d.Code,
		State:         d.State,
		Version:       d.Version,
		Attempt:       d.Attempt,
		StepSeq:       d.StepSeq,
		ReplicaID:     d.ReplicaID,
	}
	switch d.Type {
	case "execution_changed":
		e.Type = OperatorChanged
	case "reconciliation_changed":
		e.Type = SchedulingChanged
	default: // Retain event types without a legacy alias.
	}
	return e, nil
}
