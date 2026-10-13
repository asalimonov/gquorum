package metaengine

import (
	"context"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// BeginReadmissionAuthority is a separate committed boundary from approval.
// Each (group, apply index) can identify only one immutable grant; approval may
// already have used that index to revoke a conflicting group or host effect.
type BeginReadmissionAuthority struct {
	NodeName         string              `json:"node_name"`
	InstanceID       string              `json:"instance_id"`
	MemberID         uint64              `json:"member_id"`
	PreviousMemberID uint64              `json:"previous_member_id"`
	OperatorID       topology.OperatorID `json:"operator_id"`
	ProposalID       string              `json:"proposal_id"`
}

// ReadmissionID names the original retirement authority. A pending admission
// rebound to another process keeps that proof; readiness still uses InstanceID.
// Instance IDs are scoped to a node, and length-prefixed names avoid collisions.
func ReadmissionID(a AdmissionRecord) string {
	instance := a.ReadmissionInstanceID
	if instance == "" {
		instance = a.InstanceID
	}
	return fmt.Sprintf("%s/%d/%d:%s/%s", a.OperatorID, a.MemberID, len(a.NodeName), a.NodeName, instance)
}
func (sm *topologySM) applyBeginReadmissionAuthority(index uint64, p BeginReadmissionAuthority) (ApplyResult, bool) {
	refuse := func(code proposalErrorCode, reason string) (ApplyResult, bool) {
		return rejectProposal(p.ProposalID, code, "%s", reason), false
	}
	admission := sm.admissions[admissionKey(p.NodeName, p.InstanceID)]
	if admission == nil || admission.MemberID != p.MemberID || admission.OperatorID != p.OperatorID {
		return refuse(proposalAuthorization, "readmission authority requires exact approved identity")
	}
	if admission.State == AdmissionSuperseded || admission.State == AdmissionRetired {
		return refuse(proposalAuthorityFenced, "readmission was superseded")
	}
	previous := p.PreviousMemberID
	if previous == 0 || previous >= p.MemberID {
		return refuse(proposalAuthorization, "readmission requires an exact older owner")
	}
	id := ReadmissionID(*admission)
	var grants []topology.AuthorityGrant
	for _, r := range sm.authority.records {
		if r.Grant.Purpose == topology.AuthorityReadmit && r.Grant.AdmissionID == id {
			if r.Closed {
				return refuse(proposalAuthorityFenced, "readmission authority was revoked")
			}
			for _, target := range r.Grant.Targets {
				if target.MemberID != previous {
					return refuse(proposalAuthorization, "readmission retry changed its former owner")
				}
			}
			grants = append(grants, r.Grant.Clone())
		}
	}
	if len(grants) > 0 {
		sortGrants(grants)
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grants: grants}, false
	}
	op := sm.operators[p.OperatorID]
	if op == nil || op.State.Terminal() || op.Version != sm.target.Version || sm.target.MemberID(p.NodeName) != p.MemberID {
		return refuse(proposalAuthorization, "readmission no longer owns its exact target")
	}
	if !sm.approvedMemberLocked(p.NodeName, func(member uint64) bool { return member == previous }) {
		return refuse(proposalAuthorization, "readmission lacks approved former-owner evidence")
	}
	for _, r := range sm.authority.revocations {
		if !revokedComplete(r) {
			return refuse(proposalAuthorityPending, "predecessor revocation is pending")
		}
	}
	targets := map[string]map[uint64]bool{}
	add := func(group string, replica uint64) {
		// Metadata has one live pin. Older metadata archives are not resources
		// owned by that pin, and replacement replicas must never be retired.
		if group == topology.MetaGroupID {
			return
		}
		if current := documentReplica(sm.target, group, replica); current != nil {
			return
		}
		if targets[group] == nil {
			targets[group] = map[uint64]bool{}
		}
		targets[group][replica] = true
	}
	for _, doc := range sm.authorityDocumentsLocked() {
		if doc.Version >= op.Version || doc.MemberID(p.NodeName) == 0 || doc.MemberID(p.NodeName) > previous {
			continue
		}
		for _, group := range doc.Groups {
			for _, replica := range group.Replicas {
				if replica.NodeName == p.NodeName {
					add(group.ID, replica.ReplicaID)
				}
			}
		}
	}
	for _, record := range sm.authority.records {
		for _, target := range record.Grant.Targets {
			if target.NodeName == p.NodeName && target.MemberID <= previous {
				add(record.Grant.Key.GroupID, target.ReplicaID)
			}
		}
	}
	targets[topology.MetaGroupID] = map[uint64]bool{previous: true}
	limit := sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords
	if len(targets) > (limit-sm.authority.pendingCost())/3 {
		return refuse(proposalOverloaded, fault.ErrOverloaded.Error())
	}
	for group, replicas := range targets {
		resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: p.NodeName}
		if old := sm.authority.records[sm.authority.floors[resource]]; old != nil && !resourceConsumed(old, resource) && !sm.authority.fenceSettled(old) {
			return refuse(proposalAuthorityPending, "host has an unfinished effect")
		}
		grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: sm.target.ClusterID, GroupID: group, Revision: index}, Purpose: topology.AuthorityReadmit, AdmissionID: id, Resources: []topology.AuthorityResource{resource}}
		for replica := range replicas {
			grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: p.NodeName, MemberID: previous, ReplicaID: replica})
		}
		slices.SortFunc(grant.Targets, topology.AuthorityTarget.Compare)
		if err := grant.Validate(); err != nil {
			return refuse(proposalAuthorization, err.Error())
		}
		grants = append(grants, grant)
	}
	sortGrants(grants)
	for _, grant := range grants {
		sm.insertGrantLocked(grant)
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grants: grants}, true
}

// approvedMemberLocked deliberately ignores staged Latest and process/session
// observations. Retained grants keep evidence available after document pruning.
func (sm *topologySM) approvedMemberLocked(node string, matches func(uint64) bool) bool {
	for _, doc := range sm.authorityDocumentsLocked() {
		if member := doc.MemberID(node); member != 0 && matches(member) {
			return true
		}
	}
	for _, record := range sm.authority.records {
		for _, target := range record.Grant.Targets {
			if target.NodeName == node && matches(target.MemberID) {
				return true
			}
		}
	}
	return false
}

// Replacement preparation must not occupy the metadata host floor before the
// old owner's readmission grants are issued and consumed. A fence only ends old
// authority; it does not prove that the required retirement actually finished.
func (sm *topologySM) readmissionConsumedLocked(action topology.AuthorizedStep) bool {
	if action.GroupID != topology.MetaGroupID || action.Kind != topology.StepPrepareReplica {
		return true
	}
	if !sm.readmissionRequiredLocked(action.NodeName, action.ReplicaID) {
		return true
	}
	admission, ok := sm.admissionForStepLocked(action.OperatorID, action.NodeName, action.ReplicaID)
	if ok && sm.readmissionBatchConsumedLocked(ReadmissionID(*admission)) {
		return true
	}
	// Completed retirement is an enduring fact about this exact granted member.
	// A successor operator may need to prepare that same already rebound owner;
	// closing the old grant does not undo its actual resource receipts.
	for _, previous := range sm.admissions {
		if previous.NodeName == action.NodeName && previous.MemberID == action.ReplicaID && sm.readmissionBatchConsumedLocked(ReadmissionID(*previous)) {
			return true
		}
	}
	return false
}

func (sm *topologySM) readmissionBatchConsumedLocked(id string) bool {
	foundMeta := false
	for _, record := range sm.authority.records {
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

// ReadmissionRequired reports whether preparation requires prior-owner retirement.
// Join classification calls it only after the leader's committed current term is applied.
func (m *Machine) ReadmissionRequired(node string, member uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.readmissionRequiredLocked(node, member)
}
func (sm *topologySM) readmissionRequiredLocked(node string, member uint64) bool {
	_, activated := sm.activated[node]
	return activated && sm.active.MemberID(node) != member || sm.approvedMemberLocked(node, func(old uint64) bool { return old < member })
}

type RecordReadmissionReceiptsProposal struct {
	Admission  AdmissionRecord          `json:"admission"`
	Receipts   []topology.EffectReceipt `json:"receipts"`
	ProposalID string                   `json:"proposal_id"`
}

// Completion can arrive after supersession. It settles exact historical effects
// without granting readiness or authority to the superseded admission.
func (sm *topologySM) applyRecordReadmissionReceipts(p RecordReadmissionReceiptsProposal) (ApplyResult, bool) {
	admission := sm.admissions[admissionKey(p.Admission.NodeName, p.Admission.InstanceID)]
	if admission == nil || admission.MemberID != p.Admission.MemberID || admission.OperatorID != p.Admission.OperatorID || len(p.Receipts) == 0 {
		return rejectProposal(p.ProposalID, proposalAuthorization, "readmission receipts require exact admitted identity"), false
	}
	for _, receipt := range p.Receipts {
		r := sm.authority.records[receipt.Key]
		if r == nil || r.Grant.Purpose != topology.AuthorityReadmit || r.Grant.AdmissionID != ReadmissionID(*admission) || receipt.Validate(r.Grant) != nil {
			return rejectProposal(p.ProposalID, proposalAuthorization, "receipt is outside exact readmission authority"), false
		}
	}
	previous := sm.authority
	sm.authority = previous.clone()
	changed := false
	for _, receipt := range p.Receipts {
		result, updated := sm.applyRecordEffect(RecordEffectProposal{Receipt: receipt, ProposalID: p.ProposalID})
		if !result.Accepted {
			sm.authority = previous
			return result, false
		}
		changed = changed || updated
	}
	return ApplyResult{Accepted: true, ProposalID: p.ProposalID}, changed
}

// ReadmissionGrants returns an immutable batch already committed for this identity.
func (m *Machine) ReadmissionGrants(ctx context.Context, admission AdmissionRecord) ([]topology.AuthorityGrant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	recorded := m.admissions[admissionKey(admission.NodeName, admission.InstanceID)]
	if recorded == nil || recorded.MemberID != admission.MemberID || recorded.OperatorID != admission.OperatorID {
		return nil, fault.ErrOperatorAuthorization
	}
	var grants []topology.AuthorityGrant
	for _, r := range m.authority.records {
		if r.Grant.Purpose == topology.AuthorityReadmit && r.Grant.AdmissionID == ReadmissionID(*recorded) {
			grants = append(grants, r.Grant.Clone())
		}
	}
	sortGrants(grants)
	return grants, nil
}
