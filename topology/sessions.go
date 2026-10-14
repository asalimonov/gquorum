package topology

import "time"

// Session identifies a replicated lease; expiry observations remain local.
type Session struct {
	ID, NodeName, InstanceID string
	TTL                      time.Duration
}

// ParkedJoin is a bounded, expiring leader-local admission observation.
type ParkedJoin struct {
	Name               string    `json:"name"`
	Type               NodeType  `json:"type"`
	InstanceID         string    `json:"instance_id"`
	ClusterID          string    `json:"cluster_id"`
	Address            string    `json:"address"`
	ReportedDocVersion uint64    `json:"reported_doc_version"`
	ReportedMemberID   uint64    `json:"reported_member_id"`
	RefusalKind        string    `json:"refusal_kind"`
	Reason             string    `json:"reason"`
	FirstSeen          time.Time `json:"first_seen"`
	LastSeen           time.Time `json:"last_seen"`
	Attempts           uint64    `json:"attempts"`
}
