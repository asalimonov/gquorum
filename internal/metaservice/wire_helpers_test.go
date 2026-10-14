package metaservice

import "github.com/asalimonov/gquorum/topology"

// wireAuthorization preserves the existing MetaControl payload independently
// of the public AuthorizedStep's operator_id vocabulary.
type wireAuthorization struct {
	ExecutionID          topology.OperatorID `json:"execution_id"`
	Version              uint64              `json:"version"`
	Attempt              uint32              `json:"attempt"`
	StepSeq              int                 `json:"step_seq"`
	Kind                 topology.StepKind   `json:"kind"`
	GroupID              string              `json:"group_id"`
	NodeName             string              `json:"node_name,omitempty"`
	ReplicaID            uint64              `json:"replica_id,omitempty"`
	TransferNodeName     string              `json:"transfer_node_name,omitempty"`
	TransferReplicaID    uint64              `json:"transfer_replica_id,omitempty"`
	ExpectedVotersDigest string              `json:"expected_voters_digest,omitempty"`
	ResultVotersDigest   string              `json:"result_voters_digest,omitempty"`
}

func toWireAuthorization(a topology.AuthorizedStep) wireAuthorization {
	return wireAuthorization{
		ExecutionID:          a.OperatorID,
		Version:              a.Version,
		Attempt:              a.Attempt,
		StepSeq:              a.StepSeq,
		Kind:                 a.Kind,
		GroupID:              a.GroupID,
		NodeName:             a.NodeName,
		ReplicaID:            a.ReplicaID,
		TransferNodeName:     a.TransferNodeName,
		TransferReplicaID:    a.TransferReplicaID,
		ExpectedVotersDigest: a.ExpectedVotersDigest,
		ResultVotersDigest:   a.ResultVotersDigest,
	}
}
func (a wireAuthorization) public() topology.AuthorizedStep {
	return topology.AuthorizedStep{
		OperatorID:           a.ExecutionID,
		Version:              a.Version,
		Attempt:              a.Attempt,
		StepSeq:              a.StepSeq,
		Kind:                 a.Kind,
		GroupID:              a.GroupID,
		NodeName:             a.NodeName,
		ReplicaID:            a.ReplicaID,
		TransferNodeName:     a.TransferNodeName,
		TransferReplicaID:    a.TransferReplicaID,
		ExpectedVotersDigest: a.ExpectedVotersDigest,
		ResultVotersDigest:   a.ResultVotersDigest,
	}
}
