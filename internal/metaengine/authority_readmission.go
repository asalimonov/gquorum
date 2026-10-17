package metaengine

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// BeginReadmissionAuthority is a separate committed boundary from approval.
// Each (group, apply index) can identify only one immutable grant; approval may
// already have used that index to revoke a conflicting group or host effect.
type BeginReadmissionAuthority struct {
	NodeName         string               `json:"node_name"`
	InstanceID       string               `json:"instance_id"`
	MemberID         uint64               `json:"member_id"`
	PreviousMemberID uint64               `json:"previous_member_id"`
	OperationID      topology.OperationID `json:"operation_id"`
	ProposalID       string               `json:"proposal_id"`
}

// ReadmissionID names the original retirement authority. A pending admission
// rebound to another process or operation keeps that proof; readiness still
// uses the current InstanceID and OperationID.
// Instance IDs are scoped to a node, and length-prefixed names avoid
// collisions.
func ReadmissionID(a AdmissionRecord) string {
	instance := a.ReadmissionInstanceID
	if instance == "" {
		instance = a.InstanceID
	}
	return fmt.Sprintf("%s/%d/%d:%s/%s", a.originOperator(), a.MemberID, len(a.NodeName), a.NodeName, instance)
}

func (a AdmissionRecord) originOperator() topology.OperationID {
	if a.ReadmissionOperatorID != "" {
		return a.ReadmissionOperatorID
	}
	return topology.OperationID(a.OperationID)
}

func (a authorityCatalog) admissionRetirementConsumed(admission AdmissionRecord, node string, member uint64) bool {
	return admission.NodeName == node && admission.MemberID == member &&
		a.readmissionBatchConsumed(ReadmissionID(admission))
}

func (a authorityCatalog) readmissionBatchConsumed(id string) bool {
	foundMeta := false
	for _, record := range a.records {
		if record.Grant.Purpose != topology.AuthorityReadmit || record.Grant.AdmissionID != id {
			continue
		}
		if !resourceConsumed(record, record.Grant.Resources[0]) {
			return false
		}
		foundMeta = foundMeta || record.Grant.Key.GroupID == topology.MetaGroupID
	}
	return foundMeta
}

func sortGrants(grants []topology.AuthorityGrant) {
	slices.SortFunc(grants, func(a, b topology.AuthorityGrant) int { return compareAuthorityKey(a.Key, b.Key) })
}

// ReadmissionRequired reports whether preparation requires prior-owner
// retirement. Join classification calls it only after the leader's committed
// current term is applied.
func (m *Machine) ReadmissionRequired(node string, member uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := m.scoped
	row := s.indexes.groups[topology.MetaGroupID]
	unproven := row == nil || !groupHasMember(row.Declaration, node, member)
	return unproven && slices.Contains(s.Activated, node) || previousApprovedMember(s, s.indexes, node, member) != 0
}

// RecordReadmissionReceiptsProposal records verified host-effect receipts for
// readmission cleanup.
type RecordReadmissionReceiptsProposal struct {
	Admission  AdmissionRecord          `json:"admission"`
	Receipts   []topology.EffectReceipt `json:"receipts"`
	At         time.Time                `json:"at,omitzero"`
	ProposalID string                   `json:"proposal_id"`
}

// ReadmissionGrants returns an immutable batch already committed for this
// identity.
func (m *Machine) ReadmissionGrants(ctx context.Context, admission AdmissionRecord) ([]topology.AuthorityGrant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var recorded *AdmissionRecord
	for i := range m.scoped.Admissions {
		a := &m.scoped.Admissions[i]
		if a.NodeName == admission.NodeName && a.InstanceID == admission.InstanceID {
			recorded = a
			break
		}
	}
	if recorded == nil || recorded.MemberID != admission.MemberID || recorded.OperationID != admission.OperationID {
		return nil, fault.ErrOperationAuthorization
	}
	var grants []topology.AuthorityGrant
	for _, r := range m.scoped.AuthorityRecords {
		if r.Grant.Purpose == topology.AuthorityReadmit && r.Grant.AdmissionID == ReadmissionID(*recorded) {
			grants = append(grants, r.Grant.Clone())
		}
	}
	sortGrants(grants)
	return grants, nil
}
