package topology

// FenceEvidence records durable installation of a grant revision on an exact
// resource.
type FenceEvidence struct {
	Key           AuthorityKey      `json:"key"`
	Resource      AuthorityResource `json:"resource"`
	Receiver      Identity          `json:"receiver"`
	ReplicaID     uint64            `json:"replica_id,omitempty"`
	AppliedIndex  uint64            `json:"applied_index,omitempty"`
	AppliedTerm   uint64            `json:"applied_term,omitempty"`
	LocalSequence uint64            `json:"local_sequence,omitempty"`
}

// FenceObligation pairs a required resource fence with its optional
// installation evidence.
type FenceObligation struct {
	Key      AuthorityKey      `json:"key"`
	Resource AuthorityResource `json:"resource"`
	Evidence *FenceEvidence    `json:"evidence,omitempty"`
}

// AuthorityStatus reports operator revocation progress and outstanding fence
// obligations.
type AuthorityStatus struct {
	OperationID         OperationID       `json:"operation_id"`
	RevocationRequested bool              `json:"revocation_requested"`
	RevocationComplete  bool              `json:"revocation_complete"`
	Obligations         []FenceObligation `json:"obligations"`
}
