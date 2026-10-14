package metaengine

// GrantSession creates or renews a session for an exact node incarnation.
type GrantSession struct {
	SessionID  string `json:"session_id"`
	NodeName   string `json:"node_name"`
	InstanceID string `json:"instance_id"`
	TTLSeconds int    `json:"ttl_seconds"`
	ProposalID string `json:"proposal_id"`
}

// RevokeSession removes a session and records its diagnostic reason.
type RevokeSession struct {
	SessionID  string `json:"session_id"`
	Reason     string `json:"reason,omitempty"`
	ProposalID string `json:"proposal_id"`
}

// AcquireLock requests ownership of an application lock for a session.
type AcquireLock struct {
	Key        string `json:"key"`
	SessionID  string `json:"session_id"`
	ProposalID string `json:"proposal_id"`
}
