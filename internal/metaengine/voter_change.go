package metaengine

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// VoterChangeDocument constructs the entire automatic role edit. It never
// merges staged intent or changes assignment identities, pins or policy.
func VoterChangeDocument(base topology.Document, change topology.VoterChange, at time.Time, actor string) (topology.Document, error) {
	if base.Version == 0 || base.Version == math.MaxUint64 || at.IsZero() || strings.TrimSpace(actor) == "" || change.Promote == nil && change.Demote == nil {
		return topology.Document{}, fault.ErrInvalid
	}
	next := base.Clone()
	g := next.Group(change.GroupID)
	if g == nil || g.VoterPolicy == nil {
		return topology.Document{}, fault.ErrOperatorAuthorization
	}
	count := len(base.VotersOf(change.GroupID))
	switch {
	case change.Promote != nil && change.Demote != nil:
		if *change.Promote == *change.Demote || count != g.VoterPolicy.TargetVoters {
			return topology.Document{}, fault.ErrOperatorAuthorization
		}
	case change.Promote != nil:
		if count >= g.VoterPolicy.TargetVoters {
			return topology.Document{}, fault.ErrOperatorAuthorization
		}
	case change.Demote != nil:
		if count <= g.VoterPolicy.TargetVoters {
			return topology.Document{}, fault.ErrOperatorAuthorization
		}
	}
	edit := func(key *topology.ReplicaKey, from, to topology.ReplicaRole) error {
		if key == nil {
			return nil
		}
		r := documentReplica(next, change.GroupID, key.ReplicaID)
		if r == nil || r.NodeName != key.NodeName || r.Role != from || r.PinnedLearner {
			return fault.ErrOperatorAuthorization
		}
		n := next.Node(r.NodeName)
		if n == nil || !n.State.Places() || to == topology.ReplicaVoter && n.State != topology.NodeActive {
			return fault.ErrOperatorAuthorization
		}
		r.Role, r.UpdatedAt = to, at
		return nil
	}
	if err := edit(change.Promote, topology.ReplicaLearner, topology.ReplicaVoter); err != nil {
		return topology.Document{}, err
	}
	if err := edit(change.Demote, topology.ReplicaVoter, topology.ReplicaLearner); err != nil {
		return topology.Document{}, err
	}
	g.UpdatedAt = at
	next.Version, next.ParentVersion = base.Version+1, base.Version
	next.Origin, next.ChangedBy = topology.OriginReconciler, actor
	return next, nil
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
		return fmt.Errorf("voter change edits fields outside exact role scope")
	}
	return nil
}

func validateVoterChangePlan(base, next topology.Document, change topology.VoterChange, plan topology.Plan) error {
	if err := topology.ValidatePlanShape(base, next, plan); err != nil {
		return err
	}
	voters := make(map[uint64]bool)
	for _, r := range base.VotersOf(change.GroupID) {
		voters[r.ReplicaID] = true
	}
	promoted, demoted, waited, transferred := false, false, false, false
	p := next.Group(change.GroupID).VoterPolicy
	exact := func(key *topology.ReplicaKey, s topology.Step) bool {
		return key != nil && key.NodeName == s.NodeName && key.ReplicaID == s.ReplicaID
	}
	for _, s := range plan.Steps {
		if s.GroupID != change.GroupID {
			return fmt.Errorf("voter plan touches an unrelated group")
		}
		switch s.Kind {
		case topology.StepWaitCaughtUp:
			if !exact(change.Promote, s) || waited {
				return fault.ErrOperatorAuthorization
			}
			waited = true
		case topology.StepPromoteVoter:
			if !exact(change.Promote, s) || !waited || promoted || voters[s.ReplicaID] {
				return fault.ErrOperatorAuthorization
			}
			if len(voters) >= p.TargetVoters && len(voters)-p.TargetVoters >= p.MaxSurgeVoters {
				return fmt.Errorf("promotion exceeds approved voter surge")
			}
			if change.GroupID != topology.MetaGroupID && len(voters) >= next.GroupPlacement(change.GroupID).MaxReplicas {
				return fmt.Errorf("promotion exceeds hard placement maximum")
			}
			voters[s.ReplicaID], promoted = true, true
		case topology.StepDemoteVoter:
			if !exact(change.Demote, s) || demoted || !voters[s.ReplicaID] || s.TransferReplicaID == 0 || !voters[s.TransferReplicaID] {
				return fault.ErrOperatorAuthorization
			}
			if len(voters)-1 < topology.QuorumOf(len(voters)) {
				return fmt.Errorf("demotion crosses unsupported destructive quorum boundary")
			}
			delete(voters, s.ReplicaID)
			demoted = true
		case topology.StepTransferLeader:
			if change.Demote == nil || demoted || transferred || !voters[s.ReplicaID] || s.ReplicaID == change.Demote.ReplicaID {
				return fault.ErrOperatorAuthorization
			}
			// An explicit transfer is the prerequisite of this same victim's
			// guarded demotion, never a second independent control action.
			if s.Seq >= len(plan.Steps) {
				return fault.ErrOperatorAuthorization
			}
			nextStep := plan.Steps[s.Seq]
			if nextStep.Kind != topology.StepDemoteVoter || nextStep.TransferReplicaID != s.ReplicaID || nextStep.TransferNodeName != s.NodeName {
				return fault.ErrOperatorAuthorization
			}
			transferred = true
		default:
			return fmt.Errorf("voter plan contains disallowed action %q", s.Kind)
		}
	}
	if promoted != (change.Promote != nil) || demoted != (change.Demote != nil) {
		return fmt.Errorf("voter plan omits requested role change")
	}
	return nil
}

func (sm *topologySM) applyCommitVoterChange(index uint64, p CommitVoterChangeProposal) (ApplyResult, bool) {
	if p.ExpectedVersion != sm.latest.Version {
		return rejectProposal(p.ProposalID, proposalVersionConflict, "expected Latest %d, current Latest %d", p.ExpectedVersion, sm.latest.Version), false
	}
	if !reflect.DeepEqual(sm.latest, sm.target) || !reflect.DeepEqual(sm.target, sm.active) || sm.hasNonterminalOperatorLocked() {
		return rejectProposal(p.ProposalID, proposalAuthorization, "voter change requires converged views and no nonterminal execution"), false
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
	if err := validateVoterChangePlan(sm.active, doc, p.Change, p.Plan); err != nil {
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
	rec := newOperatorRecord(doc.Version, 1, p.Plan.Steps, doc.ChangedBy, doc.Origin, p.At)
	sm.operators[rec.ID] = &rec
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone(), OperatorID: rec.ID}, true
}
