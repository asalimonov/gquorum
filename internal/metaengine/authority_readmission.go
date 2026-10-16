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
	NodeName         string              `json:"node_name"`
	InstanceID       string              `json:"instance_id"`
	MemberID         uint64              `json:"member_id"`
	PreviousMemberID uint64              `json:"previous_member_id"`
	OperatorID       topology.OperatorID `json:"operator_id"`
	ProposalID       string              `json:"proposal_id"`
}

// ReadmissionID names the original retirement authority. A pending admission
// rebound to another process or operation keeps that proof; readiness still
// uses the current InstanceID and OperatorID.
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
	return topology.OperationID(a.OperatorID)
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
	grants, rejected := sm.retainedReadmissionGrantsLocked(p, id)
	if rejected != nil {
		return *rejected, false
	}
	if len(grants) > 0 {
		sortGrants(grants)
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grants: grants}, false
	}
	op := sm.operators[p.OperatorID]
	if op == nil || op.Version != sm.target.Version || op.State.Terminal() ||
		sm.target.MemberID(p.NodeName) != p.MemberID {
		return refuse(proposalAuthorization, "readmission no longer owns its exact target")
	}
	if !sm.approvedMemberLocked(p.NodeName, func(member uint64) bool { return member == previous }) {
		return refuse(proposalAuthorization, "readmission lacks approved former-owner evidence")
	}
	if sm.authority.hasPendingRevocations() {
		return refuse(proposalAuthorityPending, "predecessor revocation is pending")
	}
	targets := sm.readmissionTargetsLocked(p.NodeName, previous, op.Version)
	limit := sm.rules.Limits.WithDefaults().MaxPendingAuthorityRecords
	if len(targets) > (limit-sm.authority.pendingCost())/newAuthorityGrantCost {
		return refuse(proposalOverloaded, fault.ErrOverloaded.Error())
	}
	grants, rejected = sm.buildReadmissionGrantsLocked(index, p, id, targets)
	if rejected != nil {
		return *rejected, false
	}
	sortGrants(grants)
	for _, grant := range grants {
		sm.insertGrantLocked(grant)
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Grants: grants}, true
}

func (sm *topologySM) retainedReadmissionGrantsLocked(p BeginReadmissionAuthority,
	id string) ([]topology.AuthorityGrant, *ApplyResult) {
	var grants []topology.AuthorityGrant
	for _, r := range sm.authority.records {
		if r.Grant.Purpose == topology.AuthorityReadmit && r.Grant.AdmissionID == id {
			if r.Closed {
				return nil, rejectedReadmissionAuthority(p.ProposalID, proposalAuthorityFenced,
					"readmission authority was revoked")
			}
			for _, target := range r.Grant.Targets {
				if target.MemberID != p.PreviousMemberID {
					return nil, rejectedReadmissionAuthority(p.ProposalID, proposalAuthorization,
						"readmission retry changed its former owner")
				}
			}
			grants = append(grants, r.Grant.Clone())
		}
	}
	return grants, nil
}

type readmissionTargets struct {
	sm       *topologySM
	node     string
	previous uint64
	targets  map[string]map[uint64]bool
}

func (sm *topologySM) readmissionTargetsLocked(node string, previous, version uint64) map[string]map[uint64]bool {
	collection := readmissionTargets{sm: sm, node: node, previous: previous,
		targets: map[string]map[uint64]bool{}}
	for _, doc := range collection.sm.authorityDocumentsLocked() {
		if doc.Version >= version || doc.MemberID(collection.node) == 0 ||
			doc.MemberID(collection.node) > collection.previous {
			continue
		}
		for _, group := range doc.Groups {
			for _, replica := range group.Replicas {
				if replica.NodeName == collection.node {
					collection.add(group.ID, replica.ReplicaID)
				}
			}
		}
	}
	collection.addRetainedTargets()
	collection.targets[topology.MetaGroupID] = map[uint64]bool{collection.previous: true}
	return collection.targets
}

func (collection *readmissionTargets) add(group string, replica uint64) {
	// Metadata has one live pin. Older metadata archives are not resources
	// owned by that pin, and replacement replicas must never be retired.
	if group == topology.MetaGroupID {
		return
	}
	if current := documentReplica(collection.sm.target, group, replica); current != nil {
		return
	}
	if collection.targets[group] == nil {
		collection.targets[group] = map[uint64]bool{}
	}
	collection.targets[group][replica] = true
}

func (collection *readmissionTargets) addRetainedTargets() {
	for _, record := range collection.sm.authority.records {
		for _, target := range record.Grant.Targets {
			if target.NodeName == collection.node && target.MemberID <= collection.previous {
				collection.add(record.Grant.Key.GroupID, target.ReplicaID)
			}
		}
	}
}

func (sm *topologySM) buildReadmissionGrantsLocked(index uint64, p BeginReadmissionAuthority, id string,
	targets map[string]map[uint64]bool) ([]topology.AuthorityGrant, *ApplyResult) {
	var grants []topology.AuthorityGrant
	for group, replicas := range targets {
		resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: p.NodeName}
		if old := sm.authority.records[sm.authority.floors[resource]]; old != nil && !resourceConsumed(old, resource) &&
			!sm.authority.fenceSettled(old) {
			return nil, rejectedReadmissionAuthority(p.ProposalID, proposalAuthorityPending,
				"host has an unfinished effect")
		}
		grant := topology.AuthorityGrant{
			Key:         topology.AuthorityKey{ClusterID: sm.target.ClusterID, GroupID: group, Revision: index},
			Purpose:     topology.AuthorityReadmit,
			AdmissionID: id,
			Resources:   []topology.AuthorityResource{resource},
		}
		for replica := range replicas {
			grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: p.NodeName,
				MemberID: p.PreviousMemberID, ReplicaID: replica})
		}
		slices.SortFunc(grant.Targets, topology.AuthorityTarget.Compare)
		if err := grant.Validate(); err != nil {
			return nil, rejectedReadmissionAuthority(p.ProposalID, proposalAuthorization, err.Error())
		}
		grants = append(grants, grant)
	}
	return grants, nil
}

func rejectedReadmissionAuthority(id string, code proposalErrorCode, reason string) *ApplyResult {
	result := rejectProposal(id, code, "%s", reason)
	return &result
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
	// Completed retirement is an enduring fact about this exact granted member.
	// A successor operator may need to prepare that same already rebound owner;
	// closing the old grant does not undo its actual resource receipts.
	for _, previous := range sm.admissions {
		if sm.authority.admissionRetirementConsumed(*previous, action.NodeName, action.ReplicaID) {
			return true
		}
	}
	return false
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
	return m.readmissionRequiredLocked(node, member)
}

func (sm *topologySM) readmissionRequiredLocked(node string, member uint64) bool {
	_, activated := sm.activated[node]
	return activated && sm.active.MemberID(node) != member ||
		sm.approvedMemberLocked(node, func(old uint64) bool { return old < member })
}

// RecordReadmissionReceiptsProposal records verified host-effect receipts for
// readmission cleanup.
type RecordReadmissionReceiptsProposal struct {
	Admission  AdmissionRecord          `json:"admission"`
	Receipts   []topology.EffectReceipt `json:"receipts"`
	At         time.Time                `json:"at,omitzero"`
	ProposalID string                   `json:"proposal_id"`
}

// Completion can arrive after supersession. It settles exact historical effects
// without granting readiness or authority to the superseded admission.
func (sm *topologySM) applyRecordReadmissionReceipts(p RecordReadmissionReceiptsProposal) (ApplyResult, bool) {
	admission := sm.admissions[admissionKey(p.Admission.NodeName, p.Admission.InstanceID)]
	if admission == nil || admission.MemberID != p.Admission.MemberID ||
		admission.OperatorID != p.Admission.OperatorID || len(p.Receipts) == 0 {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"readmission receipts require exact admitted identity"), false
	}
	for _, receipt := range p.Receipts {
		r := sm.authority.records[receipt.Key]
		if r == nil || r.Grant.Purpose != topology.AuthorityReadmit ||
			r.Grant.AdmissionID != ReadmissionID(*admission) ||
			receipt.Validate(r.Grant) != nil {
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"receipt is outside exact readmission authority"), false
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

// ReadmissionGrants returns an immutable batch already committed for this
// identity.
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
