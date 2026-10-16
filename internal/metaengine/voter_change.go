package metaengine

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// VoterChangeDocument constructs the entire automatic role edit. It never
// merges staged intent or changes assignment identities, pins or lifecycle.
func VoterChangeDocument(base topology.Document, change topology.VoterChange, at time.Time,
	actor string) (topology.Document, error) {
	if base.Version == 0 || base.Version == math.MaxUint64 || change.Promote == nil && change.Demote == nil ||
		at.IsZero() || strings.TrimSpace(actor) == "" {
		return topology.Document{}, fault.ErrInvalid
	}
	next := base.Clone()
	g := next.Group(change.GroupID)
	if g == nil || g.VoterConfig == nil {
		return topology.Document{}, fault.ErrOperatorAuthorization
	}
	count := len(base.VotersOf(change.GroupID))
	if err := validateVoterCountChange(count, g.VoterConfig.TargetVoters, change); err != nil {
		return topology.Document{}, err
	}
	editor := voterRoleEditor{document: next, group: change.GroupID, at: at}
	if err := editor.edit(change.Promote, topology.ReplicaLearner, topology.ReplicaVoter); err != nil {
		return topology.Document{}, err
	}
	if err := editor.edit(change.Demote, topology.ReplicaVoter, topology.ReplicaLearner); err != nil {
		return topology.Document{}, err
	}
	var voters []topology.ReplicaKey
	for _, replica := range g.Replicas {
		if replica.Role == topology.ReplicaVoter {
			voters = append(voters, topology.ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID})
		}
	}
	if err := topology.ValidateVoterPlacement(next, change.GroupID, voters); err != nil {
		return topology.Document{}, err
	}
	g.UpdatedAt = at
	next.Version, next.ParentVersion = base.Version+1, base.Version
	next.Origin, next.ChangedBy = topology.OriginReconciler, actor
	return next, nil
}

func validateVoterCountChange(count, target int, change topology.VoterChange) error {
	switch {
	case change.Promote != nil && change.Demote != nil:
		if *change.Promote == *change.Demote || count != target {
			return fault.ErrOperatorAuthorization
		}
	case change.Promote != nil:
		if count >= target {
			return fault.ErrOperatorAuthorization
		}
	case change.Demote != nil:
		if count <= target {
			return fault.ErrOperatorAuthorization
		}
	default: // A change without either role was rejected above.
	}
	return nil
}

type voterRoleEditor struct {
	document topology.Document
	group    string
	at       time.Time
}

func (editor voterRoleEditor) edit(key *topology.ReplicaKey, from, to topology.ReplicaRole) error {
	if key == nil {
		return nil
	}
	r := documentReplica(editor.document, editor.group, key.ReplicaID)
	if r == nil || r.NodeName != key.NodeName || r.Role != from || r.PinnedLearner {
		return fault.ErrOperatorAuthorization
	}
	n := editor.document.Node(r.NodeName)
	if n == nil || to == topology.ReplicaVoter && n.State != topology.NodeActive || !n.State.Places() {
		return fault.ErrOperatorAuthorization
	}
	r.Role, r.UpdatedAt = to, editor.at
	return nil
}

func validateVoterChangeDocument(base, next topology.Document, change topology.VoterChange) error {
	g := next.Group(change.GroupID)
	if g == nil {
		return fault.ErrOperatorAuthorization
	}
	want, err := VoterChangeDocument(base, change, g.UpdatedAt, next.ChangedBy)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, next) {
		return errors.New("voter change edits fields outside exact role scope")
	}
	return nil
}

func (sm *topologySM) validateVoterChangePlan(next topology.Document, change topology.VoterChange,
	plan topology.Plan) error {
	if err := validateLegacyPlan(plan); err != nil {
		return err
	}
	if err := sm.validatePlanShapeLocked(next, plan); err != nil {
		return err
	}
	check := voterPlanCheck{next: next, change: change, plan: plan,
		voters: make(map[uint64]bool), policy: next.Group(change.GroupID).VoterConfig}
	for _, r := range sm.active.VotersOf(change.GroupID) {
		check.voters[r.ReplicaID] = true
	}
	for _, step := range plan.Steps {
		if err := check.step(step); err != nil {
			return err
		}
	}
	if check.promoted != (change.Promote != nil) || check.demoted != (change.Demote != nil) {
		return errors.New("voter plan omits requested role change")
	}
	return nil
}

type voterPlanCheck struct {
	next                           topology.Document
	change                         topology.VoterChange
	plan                           topology.Plan
	policy                         *topology.VoterConfig
	voters                         map[uint64]bool
	promoted, demoted, transferred bool
	waits                          int
}

func exactVoterStep(key *topology.ReplicaKey, step topology.Step) bool {
	return key != nil && key.NodeName == step.NodeName && key.ReplicaID == step.ReplicaID
}

func (check *voterPlanCheck) step(s topology.Step) error {
	if s.GroupID != check.change.GroupID {
		return errors.New("voter plan touches an unrelated group")
	}
	switch s.Kind {
	case topology.StepWaitCaughtUp:
		return check.waitCaughtUp(s)
	case topology.StepPromoteVoter:
		return check.promote(s)
	case topology.StepDemoteVoter:
		return check.demote(s)
	case topology.StepTransferLeader:
		return check.transfer(s)
	default:
		return fmt.Errorf("voter plan contains disallowed action %q", s.Kind)
	}
}

func (check *voterPlanCheck) waitCaughtUp(s topology.Step) error {
	if check.promoted || check.waits >= 2 || check.waits == 1 && !check.demoted ||
		!exactVoterStep(check.change.Promote, s) {
		return fault.ErrOperatorAuthorization
	}
	check.waits++
	return nil
}

func (check *voterPlanCheck) promote(s topology.Step) error {
	if check.waits == 0 || check.promoted || check.voters[s.ReplicaID] || !exactVoterStep(check.change.Promote, s) {
		return fault.ErrOperatorAuthorization
	}
	if len(check.voters) >= check.policy.TargetVoters &&
		len(check.voters)-check.policy.TargetVoters >= check.policy.MaxSurgeVoters {
		return errors.New("promotion exceeds approved voter surge")
	}
	if check.change.GroupID != topology.MetaGroupID &&
		len(check.voters) >= check.next.GroupPlacement(check.change.GroupID).MaxReplicas {
		return errors.New("promotion exceeds hard placement maximum")
	}
	check.voters[s.ReplicaID], check.promoted = true, true
	return nil
}

func (check *voterPlanCheck) demote(s topology.Step) error {
	if check.demoted || !check.voters[s.ReplicaID] || s.TransferReplicaID == 0 || !check.voters[s.TransferReplicaID] ||
		!exactVoterStep(check.change.Demote, s) {
		return fault.ErrOperatorAuthorization
	}
	if len(check.voters)-1 < topology.QuorumOf(len(check.voters)) {
		return errors.New("demotion crosses unsupported destructive quorum boundary")
	}
	delete(check.voters, s.ReplicaID)
	check.demoted = true
	return nil
}

func (check *voterPlanCheck) transfer(s topology.Step) error {
	if check.change.Demote == nil || check.demoted || check.transferred || !check.voters[s.ReplicaID] ||
		s.ReplicaID == check.change.Demote.ReplicaID {
		return fault.ErrOperatorAuthorization
	}
	// An explicit transfer is the prerequisite of this same victim's
	// guarded demotion, never a second independent control action.
	if s.Seq >= len(check.plan.Steps) {
		return fault.ErrOperatorAuthorization
	}
	nextStep := check.plan.Steps[s.Seq]
	if nextStep.Kind != topology.StepDemoteVoter || nextStep.TransferReplicaID != s.ReplicaID ||
		nextStep.TransferNodeName != s.NodeName {
		return fault.ErrOperatorAuthorization
	}
	check.transferred = true
	return nil
}

func (sm *topologySM) applyCommitVoterChange(index uint64, p CommitVoterChangeProposal) (ApplyResult, bool) {
	// Remove at Task 76 with complete scoped persistence and dispatch activation.
	if p.ExpectedTarget != 0 || p.Scopes != nil {
		return rejectProposalError(p.ProposalID, proposalUnsupported, fault.ErrUnsupported), false
	}
	if p.ExpectedVersion != sm.latest.Version {
		return rejectVersionConflict(p.ProposalID, p.ExpectedVersion, sm.latest.Version), false
	}
	if !reflect.DeepEqual(sm.latest, sm.target) || !reflect.DeepEqual(sm.target, sm.active) ||
		sm.hasNonterminalOperatorLocked() {
		return rejectProposal(p.ProposalID, proposalAuthorization,
			"voter change requires converged views and no nonterminal execution"), false
	}
	if p.At.IsZero() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "voter change timestamp required"), false
	}
	doc := p.Doc.Clone()
	if err := validateVoterChangeDocument(sm.active, doc, p.Change); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
	}
	if !doc.Group(p.Change.GroupID).UpdatedAt.Equal(p.At) {
		return rejectProposal(p.ProposalID, proposalAuthorization, "voter change timestamp differs"), false
	}
	if err := sm.validateVoterChangePlan(doc, p.Change, p.Plan); err != nil {
		return rejectProposal(p.ProposalID, proposalAuthorization, "%v", err), false
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
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}
	sm.target = doc.Clone()
	rec := newOperatorRecordWithPlan(p.Plan, 1, operatorAudit{doc.ChangedBy, doc.Origin, p.At})
	sm.operators[rec.ID] = &rec
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone(), OperatorID: rec.ID}, true
}
