package topology

import "time"

// View selects an owned, locally applied document observation.
type View string

const (
	Latest View = "latest"
	Target View = "target"
	Active View = "active"
)

// Page selects history entries in descending version order. Zero Limit is unbounded.
type Page struct{ Offset, Limit int }
type HistoryResult struct {
	Documents     []Document
	OldestVersion uint64
}

// Update is a coalesced document-watch notification, not a complete event log.
type Update struct {
	View     View
	Document Document
}

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
