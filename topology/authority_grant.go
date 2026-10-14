package topology

// AuthorityPurpose identifies the operation authorized by a grant.
type AuthorityPurpose string

// AuthorityTarget identifies a logical node, meta member and exact group
// replica.
type AuthorityTarget struct {
	NodeName  string `json:"node_name"`
	MemberID  uint64 `json:"member_id"`
	ReplicaID uint64 `json:"replica_id"`
}

// AuthorityGrant binds immutable authorization to exact resources, targets and
// an action or admission.
type AuthorityGrant struct {
	Key         AuthorityKey        `json:"key"`
	Purpose     AuthorityPurpose    `json:"purpose"`
	OperatorID  OperatorID          `json:"operator_id,omitempty"`
	Action      *AuthorizedStep     `json:"action,omitempty"`
	AdmissionID string              `json:"admission_id,omitempty"`
	RequestID   string              `json:"request_id,omitempty"`
	Resources   []AuthorityResource `json:"resources"`
	Targets     []AuthorityTarget   `json:"targets"`
}
