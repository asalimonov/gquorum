package metaengine

import (
	"fmt"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func (sm *topologySM) applyBeginOperator(p BeginOperatorProposal) (ApplyResult, bool) {
	if p.Version != sm.latest.Version {
		return rejectProposal(p.ProposalID, proposalSuperseded,
			"version %d is not Latest %d", p.Version, sm.latest.Version), false
	}
	if p.Plan.Version != p.Version {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"plan version %d does not match execution version %d", p.Plan.Version, p.Version), false
	}
	if err := validateExactPlan(p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if audit := sm.latest.Audit; audit != nil && audit.Forced &&
		(audit.Operation != topology.AuditOperationForceRetireNode || p.Actor == "" || p.Origin == "") {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"forced retirement execution provenance is required"), false
	}
	if p.At.IsZero() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "execution timestamp is required"), false
	}

	previous, found := sm.latestAttemptLocked(p.Version)
	if found {
		switch {
		case !previous.State.Terminal(), previous.State == topology.OperatorSucceeded:
			if exactStepsEqual(previous.Steps, p.Plan.Steps) {
				return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: previous.ID}, false
			}
			return rejectProposal(p.ProposalID, proposalAuthorization,
				"version %d already has a different exact plan", p.Version), false
		case previous.State == topology.OperatorSuperseded:
			return rejectProposal(p.ProposalID, proposalSuperseded,
				"execution %q is superseded", previous.ID), false
		}
	}

	// Retry the immutable accepted plan before consulting its evolving cleanup
	// observations. Completed effects may have removed all former identities.
	base, err := sm.deletionBaseLocked(sm.latest)
	if err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := topology.ValidatePlanShape(base, sm.latest, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	for _, group := range base.Groups {
		if group.ID == topology.MetaGroupID || sm.latest.Group(group.ID) != nil {
			continue
		}
		deleted := false
		for _, step := range p.Plan.Steps {
			deleted = deleted || step.GroupID == group.ID && step.Kind == topology.StepDeleteGroupState
		}
		if !deleted {
			return rejectProposal(p.ProposalID, proposalAuthorization, "plan omits cleanup of approved group %q", group.ID), false
		}
	}

	attempt := uint32(1)
	if found {
		if previous.Attempt == ^uint32(0) {
			return rejectProposal(p.ProposalID, proposalGuardrail, "execution attempt exhausted"), false
		}
		attempt = previous.Attempt + 1
	}
	if err := sm.validateCleanupCapacity(p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}
	sm.supersedeOlderLocked(p.Version, p.At)
	sm.retireRemovedGroups(sm.latest)
	sm.target = sm.latest.Clone()
	rec := newOperatorRecord(p.Version, attempt, p.Plan.Steps, p.Actor, p.Origin, p.At)
	sm.operators[rec.ID] = &rec
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: rec.ID}, true
}

func (sm *topologySM) applyCommitRepairOperator(p CommitRepairOperatorProposal) (ApplyResult, bool) {
	if p.ExpectedVersion != sm.latest.Version {
		return rejectProposal(p.ProposalID, proposalVersionConflict,
			"expected Latest %d, current Latest %d", p.ExpectedVersion, sm.latest.Version), false
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
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"repair plan version %d must equal %d", p.Plan.Version, doc.Version), false
	}
	if err := validateExactPlan(p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := topology.ValidatePlanShape(sm.active, doc, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := validateSingleReplicaRepair(sm.active, doc); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := validateRepairPlan(sm.active, doc, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if err := sm.validateDocument(doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}

	if err := sm.commitLatestLocked(&doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}
	sm.retireRemovedGroups(doc)
	sm.target = doc.Clone()
	rec := newOperatorRecord(doc.Version, 1, p.Plan.Steps, doc.ChangedBy, doc.Origin, p.At)
	sm.operators[rec.ID] = &rec
	return ApplyResult{
		ProposalID: p.ProposalID,
		Accepted:   true,
		Doc:        doc.Clone(),
		OperatorID: rec.ID,
	}, true
}

func (sm *topologySM) applyTransitionOperator(p TransitionOperatorProposal) (ApplyResult, bool) {
	t := p.Transition
	ex, ok := sm.operators[t.OperatorID]
	if !ok {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"execution %q does not exist", t.OperatorID), false
	}
	if t.Version != ex.Version || t.Attempt != ex.Attempt ||
		t.OperatorID != topology.MakeOperatorID(t.Version, t.Attempt) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"transition identity does not match execution %q", ex.ID), false
	}
	if t.At.IsZero() || t.At.Before(ex.UpdatedAt) {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"transition timestamp is missing or older than current state"), false
	}
	if ex.State.Terminal() {
		if transitionMatchesRecord(t, *ex) {
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: ex.ID}, false
		}
		code := proposalAuthorization
		if ex.State == topology.OperatorSuperseded {
			code = proposalSuperseded
		}
		return rejectProposal(p.ProposalID, code,
			"terminal execution %q cannot transition from %q", ex.ID, ex.State), false
	}
	if err := validateTransition(*ex, t); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if t.State == topology.OperatorSuperseded {
		for _, admission := range sm.admissions {
			if admission.OperatorID == ex.ID && !admission.State.terminal() {
				admission.State = AdmissionSuperseded
			}
		}
	} else if ex.CurrentStep > 0 && ex.CurrentStep <= len(ex.Steps) &&
		(t.StepSeq > ex.CurrentStep || t.State == topology.OperatorSucceeded) {
		sm.updateAdmissionForCompletedStepLocked(ex, ex.Steps[ex.CurrentStep-1])
	}

	ex.State = t.State
	ex.CurrentStep = t.StepSeq
	ex.GroupID = t.GroupID
	ex.NodeName = t.NodeName
	ex.WaitingReason = t.WaitingReason
	ex.Error = t.Error
	ex.UpdatedAt = t.At
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, OperatorID: ex.ID}, true
}

func (sm *topologySM) applyActivateTarget(p ActivateTargetProposal) (ApplyResult, bool) {
	ex, ok := sm.operators[p.OperatorID]
	if !ok || ex.Version != p.Version || ex.Attempt != p.Attempt {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"activation does not identify an exact execution"), false
	}
	if ex.State != topology.OperatorSucceeded {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"execution %q has not succeeded", ex.ID), false
	}
	if sm.target.Version != ex.Version {
		return rejectProposal(p.ProposalID, proposalSuperseded,
			"execution %q no longer owns Target version %d", ex.ID, sm.target.Version), false
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
	seenGroups := make(map[string]bool)
	lastGroup := ""
	for i, step := range plan.Steps {
		if step.Seq != i+1 {
			return fmt.Errorf("plan steps must be contiguous from 1: position %d has seq %d", i+1, step.Seq)
		}
		if step.GroupID == "" {
			return fmt.Errorf("plan step %d has no group", step.Seq)
		}
		if !validStepKind(step.Kind) {
			return fmt.Errorf("plan step %d has unknown kind %q", step.Seq, step.Kind)
		}
		if step.GroupID != lastGroup {
			if seenGroups[step.GroupID] {
				return fmt.Errorf("group %q appears in non-contiguous plan blocks", step.GroupID)
			}
			seenGroups[step.GroupID] = true
			lastGroup = step.GroupID
		}
		wholeGroup := step.Kind == topology.StepCreateFoundingGroup || step.Kind == topology.StepDeleteGroupState
		if wholeGroup && (step.NodeName != "" || step.ReplicaID != 0) {
			return fmt.Errorf("whole-group step %d carries replica identity", step.Seq)
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
	if !t.State.Valid() || t.State == topology.OperatorPending {
		return fmt.Errorf("invalid requested execution state %q", t.State)
	}
	if t.StepSeq < ex.CurrentStep || t.StepSeq > ex.CurrentStep+1 {
		return fmt.Errorf("step %d cannot follow current step %d", t.StepSeq, ex.CurrentStep)
	}
	if len(ex.Steps) == 0 {
		if t.StepSeq != 0 || t.GroupID != "" || t.NodeName != "" {
			return fmt.Errorf("empty plan transition must name step 0 without group or node")
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

	if t.State.Waiting() {
		if t.WaitingReason == "" || t.Error != "" {
			return fmt.Errorf("waiting transition requires a reason and no terminal error")
		}
	} else if t.WaitingReason != "" {
		return fmt.Errorf("non-waiting transition carries a waiting reason")
	}
	if t.State == topology.OperatorFailed {
		if t.Error == "" {
			return fmt.Errorf("failed transition requires an error")
		}
	} else if t.Error != "" {
		return fmt.Errorf("non-failed transition carries an error")
	}
	if t.State == topology.OperatorSucceeded && t.StepSeq != len(ex.Steps) {
		return fmt.Errorf("success at step %d before exact plan length %d", t.StepSeq, len(ex.Steps))
	}
	return nil
}

func validateRepairDocument(active, repair topology.Document) error {
	if active.ClusterID != repair.ClusterID || !reflect.DeepEqual(active.Placement, repair.Placement) ||
		!reflect.DeepEqual(active.Audit, repair.Audit) {
		return fmt.Errorf("repair changes cluster policy or forced audit")
	}
	if len(active.Nodes) != len(repair.Nodes) || len(active.Groups) != len(repair.Groups) {
		return fmt.Errorf("repair changes node or group cardinality")
	}
	for i := range active.Nodes {
		a, b := active.Nodes[i], repair.Nodes[i]
		a.UpdatedAt, b.UpdatedAt = time.Time{}, time.Time{}
		if !reflect.DeepEqual(a, b) {
			return fmt.Errorf("repair changes node %q beyond an update timestamp", active.Nodes[i].Name)
		}
	}
	for i := range active.Groups {
		a, b := active.Groups[i], repair.Groups[i]
		if a.ID != b.ID ||
			!reflect.DeepEqual(a.Placement, b.Placement) ||
			!a.CreatedAt.Equal(b.CreatedAt) || len(a.Replicas) != len(b.Replicas) ||
			b.NextReplicaID < a.NextReplicaID {
			return fmt.Errorf("repair changes group %q outside identity repair", a.ID)
		}
		for j := range a.Replicas {
			ra, rb := a.Replicas[j], b.Replicas[j]
			if ra.NodeName != rb.NodeName || ra.Role != rb.Role ||
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
		return fmt.Errorf("repair parent is not Active")
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
			if group.ID == topology.MetaGroupID || old.Role != topology.ReplicaVoter || node == nil || node.State != topology.NodeActive || next.ReplicaID != group.NextReplicaID || next.ReplicaID == ^uint64(0) {
				return fmt.Errorf("repair must replace one exact Active load voter with its allocator identity")
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
		return fmt.Errorf("repair changes fields outside one exact Active load voter")
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
		case topology.StepRemoveReplica, topology.StepStopReplica, topology.StepCleanupReplicaState:
			old := activeReplica(active, step.GroupID, step.ReplicaID)
			if old == nil || repairReplica(repair, step.GroupID, step.ReplicaID) != nil ||
				(step.NodeName != "" && old.NodeName != step.NodeName) {
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

func newOperatorRecord(version uint64, attempt uint32, steps []topology.Step, actor string, origin topology.Origin, at time.Time) operatorRecord {
	id := topology.MakeOperatorID(version, attempt)
	return operatorRecord{
		ID: id, Version: version, Attempt: attempt, State: topology.OperatorPending,
		Steps: append([]topology.Step(nil), steps...), Actor: actor, Origin: origin,
		StartedAt: at, UpdatedAt: at,
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
	out.Steps = append([]topology.Step(nil), in.Steps...)
	return out
}

func operatorRecordView(in operatorRecord) topology.Operator {
	return topology.Operator{Status: operatorStatus(in), Plan: topology.Plan{Version: in.Version, Steps: append([]topology.Step(nil), in.Steps...)}, Origin: in.Origin, Actor: in.Actor, StartedAt: in.StartedAt, UpdatedAt: in.UpdatedAt}
}

func operatorStatus(in operatorRecord) topology.OperatorStatus {
	return topology.OperatorStatus{
		ID: in.ID, Version: in.Version, Attempt: in.Attempt, State: in.State,
		CurrentStep: in.CurrentStep, TotalSteps: len(in.Steps),
		GroupID: in.GroupID, NodeName: in.NodeName, WaitingReason: in.WaitingReason, Error: in.Error,
	}
}

func exactStepsEqual(a, b []topology.Step) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func transitionMatchesRecord(t topology.OperatorTransition, ex operatorRecord) bool {
	return t.OperatorID == ex.ID && t.Version == ex.Version && t.Attempt == ex.Attempt &&
		t.StepSeq == ex.CurrentStep && t.State == ex.State && t.GroupID == ex.GroupID &&
		t.NodeName == ex.NodeName && t.WaitingReason == ex.WaitingReason && t.Error == ex.Error
}

func rejectProposal(id string, code proposalErrorCode, format string, args ...any) ApplyResult {
	return ApplyResult{ProposalID: id, Accepted: false, Code: code, Reason: fmt.Sprintf(format, args...)}
}
