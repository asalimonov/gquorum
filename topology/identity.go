package topology

// Identity names one admitted participant and its current process incarnation.
// InstanceID is not part of the durable member pin.
type Identity struct {
	NodeName   string `json:"node_name"`
	ClusterID  string `json:"cluster_id"`
	InstanceID string `json:"instance_id"`
	MemberID   uint64 `json:"member_id"`
}
