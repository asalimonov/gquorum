package metaengine

import (
	"encoding/json"

	"github.com/asalimonov/gquorum/topology"
)

type diskReadyAdmission struct {
	NodeName    string               `json:"node_name"`
	InstanceID  string               `json:"instance_id"`
	MemberID    uint64               `json:"member_id"`
	ExecutionID topology.OperationID `json:"execution_id"`
	ProposalID  string               `json:"proposal_id"`
}

// MarshalJSON preserves the execution ID field in admission acknowledgements.
func (p ReadyAdmissionProposal) MarshalJSON() ([]byte, error) {
	return json.Marshal(diskReadyAdmission{
		NodeName:    p.NodeName,
		InstanceID:  p.InstanceID,
		MemberID:    p.MemberID,
		ExecutionID: p.OperationID,
		ProposalID:  p.ProposalID,
	})
}

// UnmarshalJSON restores admission acknowledgements from their retained
// encoding.
func (p *ReadyAdmissionProposal) UnmarshalJSON(data []byte) error {
	var d diskReadyAdmission
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	*p = ReadyAdmissionProposal{
		NodeName:    d.NodeName,
		InstanceID:  d.InstanceID,
		MemberID:    d.MemberID,
		OperationID: d.ExecutionID,
		ProposalID:  d.ProposalID,
	}
	return nil
}
