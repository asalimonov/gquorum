package topology

// ReplicaRoleChange selects a named replica and its desired voting role.
type ReplicaRoleChange struct {
	NodeName string      `json:"node_name"`
	Role     ReplicaRole `json:"role"`
}
