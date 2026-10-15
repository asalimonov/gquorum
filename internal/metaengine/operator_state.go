package metaengine

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func (sm *topologySM) applyBeginOperator(index uint64, p BeginOperatorProposal) (ApplyResult, bool) {
	if result := sm.validateBeginOperator(p); !result.Accepted {
		return result, false
	}

	previous, found := sm.latestAttemptLocked(p.Version)
	if found {
		if result := validateOperatorRetry(previous, p); !result.Accepted {
			return result, false
		}
		if previous.State == topology.OperatorSucceeded || !previous.State.Terminal() {
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: previous.ID}, false
		}
	}

	// An accepted plan remains immutable across attempts. Cleanup proofs can
	// remove its old identities from the current planning base; validate shape
	// only on first approval, then validate reachability after inherited
	// fences.
	if !found {
		if result := sm.validateFirstOperatorPlan(p); !result.Accepted {
			return result, false
		}
	}

	attempt, err := nextOperatorAttempt(previous)
	if err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
	}
	if err := sm.validateCleanupCapacity(p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
	}
	revoked := sm.operatorsToRevokeLocked(previous, p.Version)
	report, err := placementResult(sm.target, sm.latest, topology.AffectedPlacementGroups(sm.target, sm.latest))
	if err != nil {
		return rejectProposalError(p.ProposalID, proposalGuardrail, err), false
	}
	if err := sm.revokeAuthorityLocked(index, revoked, topology.MakeOperatorID(p.Version, attempt)); err != nil {
		return rejectProposal(p.ProposalID, proposalOverloaded, "cannot reserve revocation: %v", err), false
	}
	sm.supersedeOlderLocked(p.Version, p.At)
	sm.retireRemovedGroups(sm.latest)
	sm.target = sm.latest.Clone()
	rec := newOperatorRecordWithPlan(p.Plan, attempt, operatorAudit{p.Actor, p.Origin, p.At})
	sm.operators[rec.ID] = &rec
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: rec.ID, Placement: report}, true
}

func (sm *topologySM) applyCommitRepairOperator(index uint64, p CommitRepairOperatorProposal) (ApplyResult, bool) {
	if p.ExpectedVersion != sm.latest.Version {
		return rejectVersionConflict(p.ProposalID, p.ExpectedVersion, sm.latest.Version), false
	}
	if !reflect.DeepEqual(sm.latest, sm.target) || !reflect.DeepEqual(sm.latest, sm.active) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"automatic repair requires Latest, Target, and Active to match"), false
	}
	if sm.hasNonterminalOperatorLocked() {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"automatic repair requires no nonterminal execution"), false
	}
	if p.Doc.Origin != topology.OriginReconciler && p.Doc.Origin != topology.OriginOperator {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"repair requires reconciler or operator origin"), false
	}
	if p.At.IsZero() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "repair timestamp is required"), false
	}

	doc := p.Doc.Clone()
	doc.Version = sm.latest.Version + 1
	if p.Plan.Version != doc.Version {
		return rejectProposal(p.ProposalID, proposalAuthorization, "repair plan version %d must equal %d",
			p.Plan.Version, doc.Version), false
	}
	if err := validateExactPlan(p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, errorFormat, err), false
	}
	if err := sm.validatePlanShapeLocked(doc, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, errorFormat, err), false
	}
	if err := validateSingleReplicaRepair(sm.active, doc); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, errorFormat, err), false
	}
	if err := validateRepairPlan(sm.active, doc, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, errorFormat, err), false
	}
	if err := sm.validateDocument(doc); err != nil {
		return rejectProposalError(p.ProposalID, proposalGuardrail, err), false
	}

	oldAuthority := sm.authority
	if err := sm.revokeAuthorityLocked(index, nil, topology.MakeOperatorID(doc.Version, 1)); err != nil {
		return rejectProposal(p.ProposalID, proposalOverloaded, "cannot reserve revocation: %v", err), false
	}
	if err := sm.commitLatestLocked(&doc); err != nil {
		sm.authority = oldAuthority
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
	}
	sm.retireRemovedGroups(doc)
	sm.target = doc.Clone()
	rec := newOperatorRecordWithPlan(p.Plan, 1, operatorAudit{doc.ChangedBy, doc.Origin, p.At})
	sm.operators[rec.ID] = &rec
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone(), OperatorID: rec.ID}, true
}

func (sm *topologySM) applyTransitionOperator(index uint64, p TransitionOperatorProposal) (ApplyResult, bool) {
	t := p.Transition
	ex, ok := sm.operators[t.OperatorID]
	if !ok {
		return rejectProposal(p.ProposalID, proposalAuthorization, "execution %q does not exist", t.OperatorID), false
	}
	if t.Version != ex.Version || t.Attempt != ex.Attempt ||
		t.OperatorID != topology.MakeOperatorID(t.Version, t.Attempt) {
		return rejectProposal(p.ProposalID, proposalAuthorization, "transition identity does not match execution %q",
			ex.ID), false
	}
	if t.At.IsZero() || t.At.Before(ex.UpdatedAt) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"transition timestamp is missing or older than current state"), false
	}
	if ex.State.Terminal() {
		return terminalOperatorTransition(ex, p)
	}
	if err := validateTransition(*ex, t); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, errorFormat, err), false
	}
	if err := sm.validateTransitionConsumptionLocked(ex, t); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorityPending, errorFormat, err), false
	}
	if t.State == topology.OperatorSuperseded {
		if err := sm.revokeAuthorityLocked(index, []topology.OperatorID{ex.ID}, ""); err != nil {
			return rejectProposal(p.ProposalID, proposalOverloaded, "cannot reserve revocation: %v", err), false
		}
	}
	sm.recordOperatorTransitionEffectsLocked(index, ex, t)

	ex.State = t.State
	ex.CurrentStep = t.StepSeq
	ex.GroupID = t.GroupID
	ex.NodeName = t.NodeName
	ex.WaitingReason = t.WaitingReason
	ex.Error = t.Error
	ex.UpdatedAt = t.At
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: ex.ID}, true
}

func (sm *topologySM) stepConsumedLocked(ex *operatorRecord, step topology.Step) bool {
	if step.Kind == topology.StepWaitCaughtUp {
		return true
	}
	return sm.authority.stepConsumed(ex, step, openCompletion)
}

type completionScope uint8

const (
	openCompletion completionScope = iota
	retainedCompletion
)

func (catalog authorityCatalog) stepConsumed(ex *operatorRecord, step topology.Step, scope completionScope) bool {
	status := topology.OperationStatus{ID: topology.OperationID(ex.ID), Version: ex.Version, Attempt: ex.Attempt}
	return catalog.planStepConsumed(status, step, scope)
}

func (catalog authorityCatalog) planStepConsumed(status topology.OperationStatus,
	step topology.Step, scope completionScope) bool {
	record := catalog.planStepAuthority(status, step)
	if record == nil || scope == openCompletion && record.Closed {
		return false
	}
	for _, resource := range record.Grant.Resources {
		if !resourceConsumed(record, resource) {
			return false
		}
	}
	return true
}

func (catalog authorityCatalog) planStepAuthority(status topology.OperationStatus,
	step topology.Step) *authorityRecord {
	action := topology.AuthorizedStep{
		OperatorID:           topology.OperatorID(status.ID),
		Version:              status.Version,
		Attempt:              status.Attempt,
		StepSeq:              step.Seq,
		Kind:                 step.Kind,
		GroupID:              step.GroupID,
		NodeName:             step.NodeName,
		ReplicaID:            step.ReplicaID,
		TransferNodeName:     step.TransferNodeName,
		TransferReplicaID:    step.TransferReplicaID,
		ExpectedVotersDigest: step.ExpectedVotersDigest,
		ResultVotersDigest:   step.ResultVotersDigest,
	}
	return catalog.records[catalog.actions[action]]
}

func (sm *topologySM) applyActivateTarget(p ActivateTargetProposal) (ApplyResult, bool) {
	ex, ok := sm.operators[p.OperatorID]
	if !ok || ex.Version != p.Version || ex.Attempt != p.Attempt {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"activation does not identify an exact execution"), false
	}
	if ex.State != topology.OperatorSucceeded {
		return rejectProposal(p.ProposalID, proposalAuthorization, "execution %q has not succeeded", ex.ID), false
	}
	if sm.target.Version != ex.Version {
		return rejectProposal(p.ProposalID, proposalSuperseded, "execution %q no longer owns Target version %d", ex.ID,
			sm.target.Version), false
	}
	changed := sm.completeAdmissionsLocked(ex.ID)
	if !reflect.DeepEqual(sm.active, sm.target) {
		sm.active = sm.target.Clone()
		changed = true
	}
	sm.pruneOperatorsLocked()
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: ex.ID}, changed
}

func validateExactPlan(plan topology.Plan) error {
	if err := validateLegacyPlan(plan); err != nil {
		return err
	}
	if err := topology.ValidatePlanGuardChain(plan); err != nil {
		return err
	}
	seenGroups := make(map[string]bool)
	lastGroup := ""
	for i, step := range plan.Steps {
		if err := validatePlanStepPosition(step, i); err != nil {
			return err
		}
		if step.GroupID != lastGroup {
			if seenGroups[step.GroupID] {
				return fmt.Errorf("group %q appears in non-contiguous plan blocks", step.GroupID)
			}
			seenGroups[step.GroupID] = true
			lastGroup = step.GroupID
		}
		if err := validatePlanStepScope(step); err != nil {
			return err
		}
	}
	return nil
}

func validStepKind(kind topology.StepKind) bool {
	switch kind {
	case topology.StepCreateFoundingGroup, topology.StepPrepareReplica,
		topology.StepAddLearner, topology.StepWaitCaughtUp,
		topology.StepPromoteVoter, topology.StepDemoteVoter,
		topology.StepTransferLeader, topology.StepRemoveReplica,
		topology.StepStopReplica, topology.StepCleanupReplicaState,
		topology.StepDeleteGroupState:
		return true
	default:
		return false
	}
}

func validateTransition(ex operatorRecord, t topology.OperatorTransition) error {
	if t.State == topology.OperatorPending || !t.State.Valid() {
		return fmt.Errorf("invalid requested execution state %q", t.State)
	}
	if t.StepSeq < ex.CurrentStep || t.StepSeq > ex.CurrentStep+1 {
		return fmt.Errorf("step %d cannot follow current step %d", t.StepSeq, ex.CurrentStep)
	}
	if err := validateTransitionScope(ex, t); err != nil {
		return err
	}

	if t.State.Waiting() {
		if t.WaitingReason == "" || t.Error != "" {
			return errors.New("waiting transition requires a reason and no terminal error")
		}
	} else if t.WaitingReason != "" {
		return errors.New("non-waiting transition carries a waiting reason")
	}
	if t.State == topology.OperatorFailed {
		if t.Error == "" {
			return errors.New("failed transition requires an error")
		}
	} else if t.Error != "" {
		return errors.New("non-failed transition carries an error")
	}
	if t.State == topology.OperatorSucceeded && t.StepSeq != len(ex.Steps) {
		return fmt.Errorf("success at step %d before exact plan length %d", t.StepSeq, len(ex.Steps))
	}
	return nil
}

func validateRepairDocument(active, repair topology.Document) error {
	if active.ClusterID != repair.ClusterID || !reflect.DeepEqual(active.Placement, repair.Placement) ||
		!reflect.DeepEqual(active.Audit, repair.Audit) {
		return errors.New("repair changes cluster placement or forced audit")
	}
	if len(active.Nodes) != len(repair.Nodes) || len(active.Groups) != len(repair.Groups) {
		return errors.New("repair changes node or group cardinality")
	}
	for i := range active.Nodes {
		a, b := active.Nodes[i], repair.Nodes[i]
		a.UpdatedAt, b.UpdatedAt = time.Time{}, time.Time{}
		if !reflect.DeepEqual(a, b) {
			return fmt.Errorf("repair changes node %q beyond an update timestamp", active.Nodes[i].Name)
		}
	}
	for i := range active.Groups {
		if err := validateRepairGroup(active.Groups[i], repair.Groups[i]); err != nil {
			return err
		}
	}
	return nil
}

// validateSingleReplicaRepair re-derives the sole document edit permitted by
// the atomic repair command. Readmission has a separate validator because it
// replaces the admitted node's meta and load identities together.
func validateSingleReplicaRepair(active, repair topology.Document) error {
	if err := validateRepairDocument(active, repair); err != nil {
		return err
	}
	if repair.ParentVersion != active.Version {
		return errors.New("repair parent is not Active")
	}
	expected := active.Clone()
	expected.Version, expected.ParentVersion = repair.Version, repair.ParentVersion
	expected.Origin, expected.ChangedBy = repair.Origin, repair.ChangedBy
	// Explicit operator mutations may stamp their document audit comment/time.
	// Automatic repair preserves these fields from Active.
	if repair.Origin == topology.OriginOperator {
		expected.Comment, expected.CreatedAt = repair.Comment, repair.CreatedAt
	}
	changed := 0
	for i, group := range active.Groups {
		for j, old := range group.Replicas {
			next := repair.Groups[i].Replicas[j]
			if old.ReplicaID == next.ReplicaID {
				continue
			}
			node := active.Node(old.NodeName)
			if group.ID == topology.MetaGroupID || old.Role != topology.ReplicaVoter || node == nil ||
				node.State != topology.NodeActive || next.ReplicaID != group.NextReplicaID ||
				next.ReplicaID == ^uint64(0) {
				return errors.New("repair must replace one exact Active load voter with its allocator identity")
			}
			changed++
			expected.Groups[i].Replicas[j].ReplicaID = next.ReplicaID
			expected.Groups[i].Replicas[j].UpdatedAt = next.UpdatedAt
			expected.Groups[i].NextReplicaID = next.ReplicaID + 1
			expected.Groups[i].UpdatedAt = repair.Groups[i].UpdatedAt
			expected.Node(old.NodeName).UpdatedAt = repair.Node(old.NodeName).UpdatedAt
		}
	}
	if changed != 1 || !reflect.DeepEqual(expected, repair) {
		return errors.New("repair changes fields outside one exact Active load voter")
	}
	return nil
}

func validateRepairPlan(active, repair topology.Document, plan topology.Plan) error {
	for _, step := range plan.Steps {
		switch step.Kind {
		case topology.StepPrepareReplica, topology.StepAddLearner,
			topology.StepWaitCaughtUp, topology.StepPromoteVoter:
			fresh := repairReplica(repair, step.GroupID, step.ReplicaID)
			if fresh == nil || fresh.NodeName != step.NodeName ||
				activeReplica(active, step.GroupID, step.ReplicaID) != nil {
				return fmt.Errorf("repair step %d does not target a fresh declared replica", step.Seq)
			}
		case topology.StepRemoveReplica,
			topology.StepStopReplica,
			topology.StepCleanupReplicaState:
			old := activeReplica(active, step.GroupID, step.ReplicaID)
			if old == nil || (step.NodeName != "" && old.NodeName != step.NodeName) ||
				repairReplica(repair, step.GroupID, step.ReplicaID) != nil {
				return fmt.Errorf("repair step %d does not target a replaced replica", step.Seq)
			}
		case topology.StepTransferLeader:
			target := repairReplica(repair, step.GroupID, step.ReplicaID)
			retained := activeReplica(active, step.GroupID, step.ReplicaID)
			if target == nil || retained == nil || target.NodeName != step.NodeName ||
				retained.NodeName != step.NodeName || target.Role != topology.ReplicaVoter {
				return fmt.Errorf("repair step %d does not transfer to a retained voter", step.Seq)
			}
		default:
			return fmt.Errorf("repair step %d has disallowed kind %q", step.Seq, step.Kind)
		}
	}
	return nil
}

func activeReplica(doc topology.Document, groupID string, replicaID uint64) *topology.Replica {
	return documentReplica(doc, groupID, replicaID)
}

func repairReplica(doc topology.Document, groupID string, replicaID uint64) *topology.Replica {
	return documentReplica(doc, groupID, replicaID)
}

func documentReplica(doc topology.Document, groupID string, replicaID uint64) *topology.Replica {
	group := doc.Group(groupID)
	if group == nil {
		return nil
	}
	for i := range group.Replicas {
		if group.Replicas[i].ReplicaID == replicaID {
			return &group.Replicas[i]
		}
	}
	return nil
}

type operatorAudit struct {
	actor  string
	origin topology.Origin
	at     time.Time
}

func newOperatorRecordWithPlan(plan topology.Plan, attempt uint32, audit operatorAudit) operatorRecord {
	owned := plan.Clone()
	version := owned.Version
	id := topology.MakeOperatorID(version, attempt)
	return operatorRecord{
		ID: id, Version: version, Attempt: attempt, State: topology.OperatorPending,
		Steps: owned.Steps, VoterBases: owned.VoterBases, Actor: audit.actor, Origin: audit.origin,
		StartedAt: audit.at, UpdatedAt: audit.at,
	}
}

func (sm *topologySM) latestAttemptLocked(version uint64) (*operatorRecord, bool) {
	var latest *operatorRecord
	for _, ex := range sm.operators {
		if ex.Version == version && (latest == nil || ex.Attempt > latest.Attempt) {
			latest = ex
		}
	}
	return latest, latest != nil
}

func (sm *topologySM) supersedeOlderLocked(version uint64, at time.Time) {
	for _, ex := range sm.operators {
		if ex.Version < version && !ex.State.Terminal() {
			ex.State = topology.OperatorSuperseded
			ex.WaitingReason = ""
			ex.Error = ""
			ex.UpdatedAt = at
			for _, admission := range sm.admissions {
				if admission.OperatorID == ex.ID && !admission.State.terminal() {
					admission.State = AdmissionSuperseded
				}
			}
		}
	}
}

func (sm *topologySM) hasNonterminalOperatorLocked() bool {
	for _, ex := range sm.operators {
		if !ex.State.Terminal() {
			return true
		}
	}
	return false
}

func cloneOperatorRecord(in operatorRecord) operatorRecord {
	out := in
	plan := operatorRecordPlan(in).Clone()
	out.Steps, out.VoterBases = plan.Steps, plan.VoterBases
	return out
}

func operatorRecordPlan(in operatorRecord) topology.Plan {
	return topology.Plan{Version: in.Version, Steps: in.Steps, VoterBases: in.VoterBases}
}

func operatorRecordView(in operatorRecord) topology.Operator {
	return topology.Operator{
		Status:    operatorStatus(in),
		Plan:      operatorRecordPlan(in).Clone(),
		Origin:    in.Origin,
		Actor:     in.Actor,
		StartedAt: in.StartedAt,
		UpdatedAt: in.UpdatedAt,
	}
}

func operatorStatus(in operatorRecord) topology.OperatorStatus {
	return topology.OperatorStatus{
		ID: in.ID, Version: in.Version, Attempt: in.Attempt, State: in.State,
		CurrentStep: in.CurrentStep, TotalSteps: len(in.Steps),
		GroupID: in.GroupID, NodeName: in.NodeName, WaitingReason: in.WaitingReason, Error: in.Error,
	}
}

func transitionMatchesRecord(t topology.OperatorTransition, ex operatorRecord) bool {
	return t.OperatorID == ex.ID && t.Version == ex.Version && t.Attempt == ex.Attempt && t.StepSeq == ex.CurrentStep &&
		t.State == ex.State && t.GroupID == ex.GroupID && t.NodeName == ex.NodeName &&
		t.WaitingReason == ex.WaitingReason && t.Error == ex.Error
}

func rejectVersionConflict(id string, expected, current uint64) ApplyResult {
	result := rejectProposal(id, proposalVersionConflict, "expected Latest %d, current Latest %d", expected, current)
	result.ExpectedVersion, result.CurrentVersion = expected, current
	return result
}

func rejectProposal(id string, code proposalErrorCode, format string, args ...any) ApplyResult {
	return ApplyResult{ProposalID: id, Accepted: false, Code: code, Reason: fmt.Sprintf(format, args...)}
}

func rejectProposalError(id string, code proposalErrorCode, err error) ApplyResult {
	result := rejectProposal(id, code, errorFormat, err)
	var placement *topology.PlacementError
	if errors.As(err, &placement) {
		report := placement.Report.Clone()
		result.Placement = &report
	}
	return result
}

func validatePlanStepPosition(step topology.Step, i int) error {
	if step.Seq != i+1 {
		return fmt.Errorf("plan steps must be contiguous from 1: position %d has seq %d", i+1, step.Seq)
	}
	if step.GroupID == "" {
		return fmt.Errorf("plan step %d has no group", step.Seq)
	}
	if !validStepKind(step.Kind) {
		return fmt.Errorf("plan step %d has unknown kind %q", step.Seq, step.Kind)
	}
	return nil
}

func validatePlanStepScope(step topology.Step) error {
	wholeGroup := step.Kind == topology.StepCreateFoundingGroup || step.Kind == topology.StepDeleteGroupState
	if step.TransferNodeName != "" || step.TransferReplicaID != 0 {
		if step.Kind != topology.StepDemoteVoter || step.TransferNodeName == "" || step.TransferReplicaID == 0 ||
			step.TransferReplicaID == step.ReplicaID {
			return fmt.Errorf("step %d has invalid demotion transfer identity", step.Seq)
		}
	}
	if wholeGroup && (step.NodeName != "" || step.ReplicaID != 0) {
		return fmt.Errorf("whole-group step %d carries replica identity", step.Seq)
	}
	return nil
}

func validateTransitionScope(ex operatorRecord, t topology.OperatorTransition) error {
	if len(ex.Steps) == 0 {
		if t.StepSeq != 0 || t.GroupID != "" || t.NodeName != "" {
			return errors.New("empty plan transition must name step 0 without group or node")
		}
	} else {
		if t.StepSeq < 1 || t.StepSeq > len(ex.Steps) {
			return fmt.Errorf("step %d is outside exact plan", t.StepSeq)
		}
		step := ex.Steps[t.StepSeq-1]
		if t.GroupID != step.GroupID || t.NodeName != step.NodeName {
			return fmt.Errorf("transition scope does not match exact step %d", t.StepSeq)
		}
	}

	return nil
}

func validateRepairGroup(a, b topology.Group) error {
	if a.ID != b.ID || a.Storage != b.Storage || len(a.Replicas) != len(b.Replicas) ||
		b.NextReplicaID < a.NextReplicaID || !reflect.DeepEqual(a.Placement, b.Placement) ||
		!reflect.DeepEqual(a.VoterConfig, b.VoterConfig) || !reflect.DeepEqual(a.Hosting, b.Hosting) ||
		!a.CreatedAt.Equal(b.CreatedAt) {
		return fmt.Errorf("repair changes group %q outside identity repair", a.ID)
	}
	for j := range a.Replicas {
		ra, rb := a.Replicas[j], b.Replicas[j]
		if ra.NodeName != rb.NodeName || ra.Role != rb.Role || ra.PinnedLearner != rb.PinnedLearner ||
			!ra.CreatedAt.Equal(rb.CreatedAt) {
			return fmt.Errorf("repair changes replica placement or role in group %q", a.ID)
		}
		if ra.ReplicaID != rb.ReplicaID && rb.ReplicaID < a.NextReplicaID {
			return fmt.Errorf("repair reuses replica ID %d in group %q", rb.ReplicaID, a.ID)
		}
		if rb.ReplicaID >= b.NextReplicaID {
			return fmt.Errorf("repair replica ID %d exceeds allocator in group %q", rb.ReplicaID, a.ID)
		}
	}
	return nil
}

func (sm *topologySM) validateBeginOperator(p BeginOperatorProposal) ApplyResult {
	if p.Version != sm.latest.Version {
		return rejectProposal(p.ProposalID, proposalSuperseded, "version %d is not Latest %d", p.Version,
			sm.latest.Version)
	}
	if p.Plan.Version != p.Version {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"plan version %d does not match execution version %d",
			p.Plan.Version, p.Version)
	}
	if err := validateExactPlan(p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, errorFormat, err)
	}
	if audit := sm.latest.Audit; audit != nil && audit.Forced &&
		(audit.Operation != topology.AuditOperationForceRetireNode || p.Actor == "" || p.Origin == "") {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"forced retirement execution provenance is required")
	}
	if p.At.IsZero() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "execution timestamp is required")
	}

	return ApplyResult{Accepted: true}
}

func validateOperatorRetry(previous *operatorRecord, p BeginOperatorProposal) ApplyResult {
	if previous.State == topology.OperatorSuperseded {
		return rejectProposal(p.ProposalID, proposalSuperseded, "execution %q is superseded", previous.ID)
	}
	if !operatorRecordPlan(*previous).Equal(p.Plan) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"version %d already has a different exact plan; stage a new version to replan", p.Version)
	}
	return ApplyResult{Accepted: true}
}

func (sm *topologySM) validateFirstOperatorPlan(p BeginOperatorProposal) ApplyResult {
	if err := sm.validatePlacementLocked(sm.latest); err != nil {
		return rejectProposalError(p.ProposalID, proposalGuardrail, err)
	}
	if err := sm.validatePlanShapeLocked(sm.latest, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, errorFormat, err)
	}
	return ApplyResult{Accepted: true}
}

func terminalOperatorTransition(ex *operatorRecord, p TransitionOperatorProposal) (ApplyResult, bool) {
	t := p.Transition
	if transitionMatchesRecord(t, *ex) {
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: ex.ID}, false
	}
	code := proposalAuthorization
	if ex.State == topology.OperatorSuperseded {
		code = proposalSuperseded
	}
	return rejectProposal(p.ProposalID, code, "terminal execution %q cannot transition from %q", ex.ID,
		ex.State), false
}

func (sm *topologySM) validateTransitionConsumptionLocked(ex *operatorRecord, t topology.OperatorTransition) error {
	if t.State != topology.OperatorSuperseded && (t.StepSeq > ex.CurrentStep ||
		t.State == topology.OperatorSucceeded) && ex.CurrentStep > 0 {
		if !sm.stepConsumedLocked(ex, ex.Steps[ex.CurrentStep-1]) {
			return errors.New("exact step consumption receipts are incomplete")
		}
	}
	if t.State == topology.OperatorSucceeded {
		if len(ex.Steps) > 0 && !sm.stepConsumedLocked(ex, ex.Steps[len(ex.Steps)-1]) {
			return errors.New("final step consumption receipts are incomplete")
		}
		for _, revocation := range sm.authority.revocations {
			if !revokedComplete(revocation) {
				return errors.New("predecessor revocation is pending")
			}
		}
	}
	return nil
}

func (sm *topologySM) operatorsToRevokeLocked(previous *operatorRecord, version uint64) []topology.OperatorID {
	var revoked []topology.OperatorID
	for _, old := range sm.operators {
		if previous != nil && old.ID == previous.ID || old.Version < version && !old.State.Terminal() {
			revoked = append(revoked, old.ID)
		}
	}
	return revoked
}

func nextOperatorAttempt(previous *operatorRecord) (uint32, error) {
	if previous == nil {
		return 1, nil
	}
	if previous.Attempt == ^uint32(0) {
		return 0, errors.New("execution attempt exhausted")
	}
	return previous.Attempt + 1, nil
}

func (sm *topologySM) recordOperatorTransitionEffectsLocked(index uint64, ex *operatorRecord,
	t topology.OperatorTransition) {
	if t.State == topology.OperatorSuperseded {
		for _, admission := range sm.admissions {
			if admission.OperatorID == ex.ID && !admission.State.terminal() {
				admission.State = AdmissionSuperseded
			}
		}
	} else if ex.CurrentStep > 0 && ex.CurrentStep <= len(ex.Steps) && (t.StepSeq > ex.CurrentStep ||
		t.State == topology.OperatorSucceeded) {
		sm.updateAdmissionForCompletedStepLocked(ex, ex.Steps[ex.CurrentStep-1])
		sm.recordRoleEffectLocked(index, ex, ex.Steps[ex.CurrentStep-1], t.At)
	}
}
