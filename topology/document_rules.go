package topology

// Limits bounds admitted nodes, load replicas and concurrent execution work.
// Limits are immutable replicated cluster settings: participants must agree, and
// changing them rejects retained-state startup or admission.
type Limits struct {
	MaxPendingAuthorityRecords int `json:"max_pending_authority_records,omitempty"`
	MaxNodes                   int `json:"max_nodes"`
	MaxReplicas                int `json:"max_replicas"`
	MaxConcurrentGroups        int `json:"max_concurrent_groups,omitempty"`
	MaxConcurrentCleanup       int `json:"max_concurrent_cleanup,omitempty"`
}

// Rules contains the replicated schema version and cluster capacity limits.
type Rules struct {
	SchemaVersion uint32 `json:"schema_version"`
	Limits        Limits `json:"limits"`
}

// PlacementRule bounds voters and selects eligible load hosts by node type and
// labels.
type PlacementRule struct {
	// MinReplicas and MaxReplicas bound voters, not hosted learners.
	// Limits.MaxReplicas bounds the total replicas of a load group.
	MinReplicas      int               `json:"min_replicas"`
	MaxReplicas      int               `json:"max_replicas"`
	AllowedNodeTypes []NodeType        `json:"allowed_node_types,omitempty"`
	NodeSelector     map[string]string `json:"node_selector,omitempty"`
}
