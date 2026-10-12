package topology

import (
	"slices"
	"time"
)

// RoleChangeRecord is replicated only after verified completion of an exact
// approved role step. Index identifies the containing meta application.
type RoleChangeRecord struct {
	OperatorID OperatorID  `json:"operator_id"`
	Version    uint64      `json:"version"`
	Attempt    uint32      `json:"attempt"`
	StepSeq    int         `json:"step_seq"`
	GroupID    string      `json:"group_id"`
	NodeName   string      `json:"node_name"`
	ReplicaID  uint64      `json:"replica_id"`
	From       ReplicaRole `json:"from"`
	To         ReplicaRole `json:"to"`
	At         time.Time   `json:"at"`
	Index      uint64      `json:"index"`
}

type VoterSwitchState struct {
	GroupID  string             `json:"group_id"`
	Last     RoleChangeRecord   `json:"last"`
	Replicas []RoleChangeRecord `json:"replicas"`
}

func (s VoterSwitchState) Clone() VoterSwitchState {
	s.Replicas = slices.Clone(s.Replicas)
	return s
}
