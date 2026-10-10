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
	NodeName    string               `json:"node_name"`
	InstanceID  string               `json:"instance_id"`
	MemberID    uint64               `json:"member_id"`
	ExecutionID topology.ExecutionID `json:"execution_id"`
	State       AdmissionState       `json:"state"`
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

func (sm *topologySM) admissionForStepLocked(executionID topology.ExecutionID, nodeName string, memberID uint64) (*AdmissionRecord, bool) {
	for _, record := range sm.admissions {
		if record.ExecutionID == executionID && record.NodeName == nodeName && record.MemberID == memberID &&
			record.State != AdmissionSuperseded && record.State != AdmissionRetired {
			return record, true
		}
	}
	return nil, false
}

func (sm *topologySM) admissionForExecution(executionID topology.ExecutionID, nodeName string, memberID uint64) (AdmissionRecord, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record, ok := sm.admissionForStepLocked(executionID, nodeName, memberID)
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
	key := admissionKey(record.NodeName, record.InstanceID)
	if existing, ok := sm.admissions[key]; ok {
		if existing.MemberID == record.MemberID && existing.ExecutionID == record.ExecutionID {
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
	}
	if sm.target.MemberID(record.NodeName) != record.MemberID {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"admission member %d is not Target identity for %q", record.MemberID, record.NodeName), false
	}
	execution, ok := sm.executions[record.ExecutionID]
	if !ok {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"admission execution %q does not exist", record.ExecutionID), false
	}
	if p.Founding {
		if execution.State != topology.ExecutionSucceeded || sm.target.Version != sm.active.Version ||
			sm.target.Version != execution.Version || sm.target.Origin != topology.OriginFounding {
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"founding admission is not backed by the active founding execution"), false
		}
		record.State = AdmissionCompleted
	} else {
		if execution.State.Terminal() || execution.Version != sm.target.Version ||
			!hasAdmissionSequence(execution.Steps, record.NodeName, record.MemberID) {
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"admission is not backed by an exact current _meta learner-first sequence"), false
		}
		record.State = AdmissionPending
	}
	sm.retireNodeAdmissionsLocked(record.NodeName, record.InstanceID, AdmissionSuperseded)
	sm.admissions[key] = &record
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Admission: cloneAdmission(record)}, true
}

func (sm *topologySM) applyCommitReadmission(p CommitReadmissionProposal) (ApplyResult, bool) {
	if err := validateAdmissionIdentity(p.Admission); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if p.At.IsZero() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "readmission timestamp is required"), false
	}
	key := admissionKey(p.Admission.NodeName, p.Admission.InstanceID)
	if existing, ok := sm.admissions[key]; ok {
		if existing.MemberID != p.Admission.MemberID || existing.ExecutionID != p.Admission.ExecutionID {
			return rejectProposal(p.ProposalID, proposalAuthorization, "readmission retry identity differs"), false
		}
		return ApplyResult{
			ProposalID: p.ProposalID, Accepted: true, Doc: sm.latest.Clone(),
			ExecutionID: existing.ExecutionID, Admission: cloneAdmission(*existing),
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
	if sm.hasNonterminalExecutionLocked() {
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
	if p.Plan.Version != doc.Version || !hasAdmissionSequence(p.Plan.Steps, p.Admission.NodeName, p.Admission.MemberID) {
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

	sm.commitLatestLocked(&doc)
	sm.target = doc.Clone()
	record := newExecutionRecord(doc.Version, 1, p.Plan.Steps, doc.ChangedBy, doc.Origin, p.At)
	sm.executions[record.ID] = &record
	admission := p.Admission
	admission.ExecutionID = record.ID
	admission.State = AdmissionPending
	sm.retireNodeAdmissionsLocked(admission.NodeName, admission.InstanceID, AdmissionSuperseded)
	sm.admissions[key] = &admission
	return ApplyResult{
		ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone(), ExecutionID: record.ID,
		Admission: cloneAdmission(admission),
	}, true
}

func (sm *topologySM) applyReadyAdmission(p ReadyAdmissionProposal) (ApplyResult, bool) {
	record, ok := sm.admissions[admissionKey(p.NodeName, p.InstanceID)]
	if !ok || record.MemberID != p.MemberID || record.ExecutionID != p.ExecutionID {
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
	execution, ok := sm.executions[record.ExecutionID]
	if !ok || execution.State.Terminal() || sm.target.Version != execution.Version ||
		!hasAdmissionSequence(execution.Steps, record.NodeName, record.MemberID) {
		return rejectProposal(p.ProposalID, proposalSuperseded,
			"admission execution is no longer current"), false
	}
	record.State = AdmissionReady
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Admission: cloneAdmission(*record)}, true
}

func validateAdmissionIdentity(record AdmissionRecord) error {
	if strings.ContainsRune(record.NodeName, 0) || strings.ContainsRune(record.InstanceID, 0) {
		return fmt.Errorf("admission identity contains a separator")
	}
	if record.NodeName == "" || record.InstanceID == "" || record.MemberID == 0 {
		return fmt.Errorf("admission requires node, instance, and non-zero member identity")
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

func hasAdmissionSequence(steps []topology.Step, nodeName string, memberID uint64) bool {
	want := []topology.StepKind{
		topology.StepPrepareReplica, topology.StepAddLearner,
		topology.StepWaitCaughtUp, topology.StepPromoteVoter,
	}
	for i := 0; i+len(want) <= len(steps); i++ {
		matched := true
		for j, kind := range want {
			step := steps[i+j]
			if step.Kind != kind || step.GroupID != topology.MetaGroupID ||
				step.NodeName != nodeName || step.ReplicaID != memberID {
				matched = false
				break
			}
		}
		if matched {
			return true
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

func (sm *topologySM) updateAdmissionForCompletedStepLocked(ex *executionRecord, step topology.Step) bool {
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

func (sm *topologySM) completeAdmissionsLocked(executionID topology.ExecutionID) bool {
	changed := false
	for _, record := range sm.admissions {
		if record.ExecutionID == executionID && !record.State.terminal() {
			record.State = AdmissionCompleted
			changed = true
		}
	}
	return changed
}

// Prepared reports whether an exact admitted runtime has acknowledged readiness.
func (s AdmissionState) Prepared() bool { return s.prepared() }
