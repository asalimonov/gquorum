package metaengine

import (
	"errors"
	"fmt"
	"math"
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
		return topology.Document{}, fault.ErrOperationAuthorization
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
			return fault.ErrOperationAuthorization
		}
	case change.Promote != nil:
		if count >= target {
			return fault.ErrOperationAuthorization
		}
	case change.Demote != nil:
		if count <= target {
			return fault.ErrOperationAuthorization
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
		return fault.ErrOperationAuthorization
	}
	n := editor.document.Node(r.NodeName)
	if n == nil || to == topology.ReplicaVoter && n.State != topology.NodeActive || !n.State.Places() {
		return fault.ErrOperationAuthorization
	}
	r.Role, r.UpdatedAt = to, editor.at
	return nil
}

// Both approval paths apply the same surge, placement, transfer and quorum guards.
func validateVoterChangeSteps(next topology.Document, change topology.VoterChange,
	plan topology.Plan, voters []uint64) error {
	check := voterPlanCheck{next: next, change: change, plan: plan,
		voters: make(map[uint64]bool), policy: next.Group(change.GroupID).VoterConfig}
	for _, id := range voters {
		check.voters[id] = true
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
		return fault.ErrOperationAuthorization
	}
	check.waits++
	return nil
}

func (check *voterPlanCheck) promote(s topology.Step) error {
	if check.waits == 0 || check.promoted || check.voters[s.ReplicaID] || !exactVoterStep(check.change.Promote, s) {
		return fault.ErrOperationAuthorization
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
		return fault.ErrOperationAuthorization
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
		return fault.ErrOperationAuthorization
	}
	// An explicit transfer is the prerequisite of this same victim's
	// guarded demotion, never a second independent control action.
	if s.Seq >= len(check.plan.Steps) {
		return fault.ErrOperationAuthorization
	}
	nextStep := check.plan.Steps[s.Seq]
	if nextStep.Kind != topology.StepDemoteVoter || nextStep.TransferReplicaID != s.ReplicaID ||
		nextStep.TransferNodeName != s.NodeName {
		return fault.ErrOperationAuthorization
	}
	check.transferred = true
	return nil
}
