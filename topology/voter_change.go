package topology

// ReplicaKey names one exact assignment; a reused node name is insufficient.
type ReplicaKey struct {
	NodeName  string `json:"node_name"`
	ReplicaID uint64 `json:"replica_id"`
}

// VoterChange changes roles in one group without replacing durable identities.
type VoterChange struct {
	GroupID string      `json:"group_id"`
	Promote *ReplicaKey `json:"promote,omitempty"`
	Demote  *ReplicaKey `json:"demote,omitempty"`
}

// Clone copies the optional promotion and demotion targets in a voter-change
// request.
func (c VoterChange) Clone() VoterChange {
	if c.Promote != nil {
		key := *c.Promote
		c.Promote = &key
	}
	if c.Demote != nil {
		key := *c.Demote
		c.Demote = &key
	}
	return c
}
