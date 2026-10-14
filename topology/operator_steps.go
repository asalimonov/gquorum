package topology

// StepKind identifies a supported action or wait in an approved plan.
type StepKind string

// Step binds a sequenced plan operation to exact replica and voter-set
// identities.
type Step struct {
	Seq                  int      `json:"seq"`
	Kind                 StepKind `json:"kind"`
	GroupID              string   `json:"group_id"`
	NodeName             string   `json:"node_name,omitempty"`
	ReplicaID            uint64   `json:"replica_id,omitempty"`
	TransferNodeName     string   `json:"transfer_node_name,omitempty"`
	TransferReplicaID    uint64   `json:"transfer_replica_id,omitempty"`
	ExpectedVotersDigest string   `json:"expected_voters_digest,omitempty"`
	ResultVotersDigest   string   `json:"result_voters_digest,omitempty"`
}

// AuthorizedStep binds an operation to its exact operator, version, attempt and
// sequence.
type AuthorizedStep struct {
	OperatorID           OperatorID `json:"operator_id"`
	Version              uint64     `json:"version"`
	Attempt              uint32     `json:"attempt"`
	StepSeq              int        `json:"step_seq"`
	Kind                 StepKind   `json:"kind"`
	GroupID              string     `json:"group_id"`
	NodeName             string     `json:"node_name,omitempty"`
	ReplicaID            uint64     `json:"replica_id,omitempty"`
	TransferNodeName     string     `json:"transfer_node_name,omitempty"`
	TransferReplicaID    uint64     `json:"transfer_replica_id,omitempty"`
	ExpectedVotersDigest string     `json:"expected_voters_digest,omitempty"`
	ResultVotersDigest   string     `json:"result_voters_digest,omitempty"`
}
