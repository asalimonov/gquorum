package metaengine

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

type AdmissionState string

const (
	AdmissionPending    AdmissionState = "pending"
	AdmissionReady      AdmissionState = "ready"
	AdmissionLearner    AdmissionState = "learner"
	AdmissionPromoted   AdmissionState = "promoted"
	AdmissionCompleted  AdmissionState = "completed"
	AdmissionSuperseded AdmissionState = "superseded"
	AdmissionRetired    AdmissionState = "retired"
)

type AdmissionRecord struct {
	NodeName   string              `json:"node_name"`
	InstanceID string              `json:"instance_id"`
	MemberID   uint64              `json:"member_id"`
	OperatorID topology.OperatorID `json:"execution_id"`
	State      AdmissionState      `json:"state"`
	// ReadmissionInstanceID retains the original retirement proof when a pending
	// admission changes process without changing its durable member or operator.
	// Empty means this record is the original. Only the FSM may assign it.
	ReadmissionInstanceID string `json:"readmission_instance_id,omitempty"`
}

func admissionKey(nodeName, instanceID string) string {
	return nodeName + "\x00" + instanceID
}

func (s AdmissionState) valid() bool {
	return s == AdmissionPending || s == AdmissionReady || s == AdmissionLearner || s == AdmissionPromoted || s.terminal()
}

func (s AdmissionState) terminal() bool {
	return s == AdmissionCompleted || s == AdmissionSuperseded || s == AdmissionRetired
}

func (s AdmissionState) prepared() bool {
	switch s {
	case AdmissionReady, AdmissionLearner, AdmissionPromoted, AdmissionCompleted:
		return true
	default:
		return false
	}
}

func cloneAdmission(in AdmissionRecord) AdmissionRecord { return in }

func (sm *topologySM) Admission(nodeName, instanceID string) (AdmissionRecord, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record, ok := sm.admissions[admissionKey(nodeName, instanceID)]
	if !ok {
		return AdmissionRecord{}, false
	}
	return cloneAdmission(*record), true
}

func (sm *topologySM) liveAdmissionForNode(nodeName, exceptInstance string) (AdmissionRecord, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record := sm.liveAdmissionForNodeLocked(nodeName, exceptInstance)
	if record == nil {
		return AdmissionRecord{}, false
	}
	return cloneAdmission(*record), true
}

func (sm *topologySM) admissionForStepLocked(operatorID topology.OperatorID, nodeName string, memberID uint64) (*AdmissionRecord, bool) {
	for _, record := range sm.admissions {
		if record.OperatorID == operatorID && record.NodeName == nodeName && record.MemberID == memberID &&
			record.State != AdmissionSuperseded && record.State != AdmissionRetired {
			return record, true
		}
	}
	return nil, false
}

func (sm *topologySM) admissionForOperator(operatorID topology.OperatorID, nodeName string, memberID uint64) (AdmissionRecord, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record, ok := sm.admissionForStepLocked(operatorID, nodeName, memberID)
	if !ok {
		return AdmissionRecord{}, false
	}
	return cloneAdmission(*record), true
}

func (sm *topologySM) applyBindAdmission(p BindAdmissionProposal) (ApplyResult, bool) {
	record := p.Admission
	if err := validateAdmissionIdentity(record); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if p.At.IsZero() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "admission timestamp is required"), false
	}
	if record.ReadmissionInstanceID != "" {
		return rejectProposal(p.ProposalID, proposalAuthorization, "readmission origin is assigned by admission binding"), false
	}
	key := admissionKey(record.NodeName, record.InstanceID)
	if existing, ok := sm.admissions[key]; ok {
		if existing.MemberID == record.MemberID && existing.OperatorID == record.OperatorID {
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Admission: cloneAdmission(*existing)}, false
		}
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"admission identity %q/%q is already bound", record.NodeName, record.InstanceID), false
	}
	if conflict := sm.liveAdmissionForNodeLocked(record.NodeName, record.InstanceID); conflict != nil {
		if !p.ReplaceInstance {
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"node %q is already admitted for instance %q", record.NodeName, conflict.InstanceID), false
		}
		if conflict.State != AdmissionPending {
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"instance %q admission is already %s; replace it through a fresh-ID repair",
				conflict.InstanceID, conflict.State), false
		}
		if conflict.MemberID == record.MemberID && conflict.OperatorID == record.OperatorID {
			record.ReadmissionInstanceID = conflict.ReadmissionInstanceID
			if record.ReadmissionInstanceID == "" {
				record.ReadmissionInstanceID = conflict.InstanceID
			}
		}
	}
	if sm.target.MemberID(record.NodeName) != record.MemberID {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"admission member %d is not Target identity for %q", record.MemberID, record.NodeName), false
	}
	operator, ok := sm.operators[record.OperatorID]
	if !ok {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"admission execution %q does not exist", record.OperatorID), false
	}
	if p.Founding {
		if operator.State != topology.OperatorSucceeded || sm.target.Version != sm.active.Version ||
			sm.target.Version != operator.Version || sm.target.Origin != topology.OriginFounding {
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"founding admission is not backed by the active founding execution"), false
		}
		record.State = AdmissionCompleted
	} else {
		if operator.State.Terminal() || operator.Version != sm.target.Version ||
			!hasAdmissionSequence(operator.Steps, sm.target, record.NodeName, record.MemberID) {
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"admission is not backed by an exact current _meta learner-first sequence"), false
		}
		record.State = AdmissionPending
	}
	sm.retireNodeAdmissionsLocked(record.NodeName, record.InstanceID, AdmissionSuperseded)
	sm.admissions[key] = &record
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Admission: cloneAdmission(record)}, true
}

func (sm *topologySM) applyCommitReadmission(index uint64, p CommitReadmissionProposal) (ApplyResult, bool) {
	if err := validateAdmissionIdentity(p.Admission); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if p.At.IsZero() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "readmission timestamp is required"), false
	}
	if p.Admission.ReadmissionInstanceID != "" {
		return rejectProposal(p.ProposalID, proposalAuthorization, "new readmission cannot select an earlier proof"), false
	}
	key := admissionKey(p.Admission.NodeName, p.Admission.InstanceID)
	if existing, ok := sm.admissions[key]; ok {
		if existing.MemberID != p.Admission.MemberID || existing.OperatorID != p.Admission.OperatorID {
			return rejectProposal(p.ProposalID, proposalAuthorization, "readmission retry identity differs"), false
		}
		return ApplyResult{
			ProposalID: p.ProposalID, Accepted: true, Doc: sm.latest.Clone(),
			OperatorID: existing.OperatorID, Admission: cloneAdmission(*existing),
		}, false
	}
	if p.ExpectedVersion != sm.latest.Version {
		return rejectProposal(p.ProposalID, proposalVersionConflict,
			"expected Latest %d, current Latest %d", p.ExpectedVersion, sm.latest.Version), false
	}
	if !reflect.DeepEqual(sm.latest, sm.target) || !reflect.DeepEqual(sm.latest, sm.active) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"readmission requires Latest, Target, and Active to match"), false
	}
	if sm.hasNonterminalOperatorLocked() {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"readmission requires no nonterminal execution"), false
	}
	if _, activated := sm.activated[p.Admission.NodeName]; !activated {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"node %q has never activated", p.Admission.NodeName), false
	}
	expected, err := buildReadmissionDocument(sm.latest, p.Admission.NodeName, p.At)
	if err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	doc := p.Doc.Clone()
	doc.Version = sm.latest.Version + 1
	if !reflect.DeepEqual(expected, doc) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"readmission document does not contain the exact fresh identities"), false
	}
	if p.Plan.Version != doc.Version || !hasAdmissionSequence(p.Plan.Steps, doc, p.Admission.NodeName, p.Admission.MemberID) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"readmission plan does not contain the exact _meta learner-first sequence"), false
	}
	if p.Admission.MemberID != doc.MemberID(p.Admission.NodeName) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"readmission member does not match fresh document identity"), false
	}
	if err := validateExactPlan(p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := topology.ValidatePlanShape(sm.active, doc, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := validateRepairDocument(sm.active, doc); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := validateRepairPlan(sm.active, doc, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := sm.validateDocument(doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}

	oldAuthority := sm.authority
	if err := sm.revokeAuthorityLocked(index, nil, topology.MakeOperatorID(doc.Version, 1)); err != nil {
		return rejectProposal(p.ProposalID, proposalOverloaded, "cannot reserve revocation: %v", err), false
	}
	if err := sm.commitLatestLocked(&doc); err != nil {
		sm.authority = oldAuthority
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}
	sm.target = doc.Clone()
	record := newOperatorRecord(doc.Version, 1, p.Plan.Steps, doc.ChangedBy, doc.Origin, p.At)
	sm.operators[record.ID] = &record
	admission := p.Admission
	admission.OperatorID = record.ID
	admission.State = AdmissionPending
	sm.retireNodeAdmissionsLocked(admission.NodeName, admission.InstanceID, AdmissionSuperseded)
	sm.admissions[key] = &admission
	return ApplyResult{
		ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone(), OperatorID: record.ID,
		Admission: cloneAdmission(admission),
	}, true
}

func (sm *topologySM) applyReadyAdmission(p ReadyAdmissionProposal) (ApplyResult, bool) {
	record, ok := sm.admissions[admissionKey(p.NodeName, p.InstanceID)]
	if !ok || record.MemberID != p.MemberID || record.OperatorID != p.OperatorID {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"join ready tuple does not match a durable admission"), false
	}
	if record.State == AdmissionSuperseded || record.State == AdmissionRetired {
		return rejectProposal(p.ProposalID, proposalSuperseded,
			"admission is %s", record.State), false
	}
	if record.State.prepared() {
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Admission: cloneAdmission(*record)}, false
	}
	operator, ok := sm.operators[record.OperatorID]
	if !ok || operator.State.Terminal() || sm.target.Version != operator.Version ||
		!hasAdmissionSequence(operator.Steps, sm.target, record.NodeName, record.MemberID) {
		return rejectProposal(p.ProposalID, proposalSuperseded,
			"admission execution is no longer current"), false
	}
	if !sm.readmissionConsumedLocked(topology.AuthorizedStep{GroupID: topology.MetaGroupID, Kind: topology.StepPrepareReplica, OperatorID: record.OperatorID, NodeName: record.NodeName, ReplicaID: record.MemberID}) {
		return rejectProposal(p.ProposalID, proposalAuthorityPending, "readmission retirement receipts are pending"), false
	}
	record.State = AdmissionReady
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Admission: cloneAdmission(*record)}, true
}

func validateAdmissionIdentity(record AdmissionRecord) error {
	if strings.ContainsRune(record.NodeName, 0) || strings.ContainsRune(record.InstanceID, 0) || strings.ContainsRune(record.ReadmissionInstanceID, 0) {
		return fmt.Errorf("admission identity contains a separator")
	}
	if record.NodeName == "" || record.InstanceID == "" || record.MemberID == 0 {
		return fmt.Errorf("admission requires node, instance, and non-zero member identity")
	}
	if record.ReadmissionInstanceID == record.InstanceID {
		return fmt.Errorf("readmission origin must name a previous process")
	}
	return nil
}

func (sm *topologySM) liveAdmissionForNodeLocked(nodeName, exceptInstance string) *AdmissionRecord {
	for _, record := range sm.admissions {
		if record.NodeName == nodeName && record.InstanceID != exceptInstance && !record.State.terminal() {
			return record
		}
	}
	return nil
}

func (sm *topologySM) retireNodeAdmissionsLocked(nodeName, exceptInstance string, state AdmissionState) {
	for _, record := range sm.admissions {
		if record.NodeName == nodeName && record.InstanceID != exceptInstance && !record.State.terminal() {
			record.State = state
		}
	}
}

func (sm *topologySM) admissionSnapshotLocked() []AdmissionRecord {
	out := make([]AdmissionRecord, 0, len(sm.admissions))
	for _, record := range sm.admissions {
		out = append(out, cloneAdmission(*record))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeName != out[j].NodeName {
			return out[i].NodeName < out[j].NodeName
		}
		return out[i].InstanceID < out[j].InstanceID
	})
	return out
}

func hasAdmissionSequence(steps []topology.Step, desired topology.Document, nodeName string, memberID uint64) bool {
	if meta := desired.MetaGroup(); meta != nil {
		for _, r := range meta.Replicas {
			if r.NodeName == nodeName && r.ReplicaID == memberID {
				return topology.ValidAdmissionSequence(steps, nodeName, memberID, r.Role)
			}
		}
	}
	return false
}

func buildReadmissionDocument(base topology.Document, nodeName string, at time.Time) (topology.Document, error) {
	doc := base.Clone()
	node := doc.Node(nodeName)
	if node == nil || node.State != topology.NodeActive {
		return topology.Document{}, fmt.Errorf("node %q is not active", nodeName)
	}
	node.UpdatedAt = at
	for i := range doc.Groups {
		changed := false
		for j := range doc.Groups[i].Replicas {
			replica := &doc.Groups[i].Replicas[j]
			if replica.NodeName != nodeName {
				continue
			}
			id, err := doc.Groups[i].AllocateReplicaID()
			if err != nil {
				return topology.Document{}, err
			}
			replica.ReplicaID = id
			replica.UpdatedAt = at
			changed = true
		}
		if changed {
			doc.Groups[i].UpdatedAt = at
		}
	}
	doc.ParentVersion = base.Version
	doc.Version = base.Version + 1
	doc.ChangedBy = "join-reconciler"
	doc.Origin = topology.OriginReconciler
	return doc, nil
}

func (sm *topologySM) updateAdmissionForCompletedStepLocked(ex *operatorRecord, step topology.Step) bool {
	record, ok := sm.admissionForStepLocked(ex.ID, step.NodeName, step.ReplicaID)
	if !ok || record.State.terminal() {
		return false
	}
	before := record.State
	switch step.Kind {
	case topology.StepAddLearner:
		record.State = AdmissionLearner
	case topology.StepPromoteVoter:
		record.State = AdmissionPromoted
	}
	return before != record.State
}

func (sm *topologySM) completeAdmissionsLocked(operatorID topology.OperatorID) bool {
	changed := false
	for _, record := range sm.admissions {
		if record.OperatorID == operatorID && !record.State.terminal() {
			record.State = AdmissionCompleted
			changed = true
		}
	}
	return changed
}

// Prepared reports whether an exact admitted runtime has acknowledged readiness.
func (s AdmissionState) Prepared() bool { return s.prepared() }
