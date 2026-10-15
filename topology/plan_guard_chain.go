package topology

import (
	"encoding/hex"
	"slices"
)

const (
	voterDigestHexLength = 64
)

// ValidatePlanGuardChain checks document-independent voter proofs and, when
// present, scoped partitions and admission/removal barriers. Approval also
// validates exact topology identities with ValidatePlanShape; this check alone
// does not authorize effects.
func ValidatePlanGuardChain(plan Plan) error {
	if len(plan.Blocks) != 0 || len(plan.Dependencies) != 0 {
		if err := validatePlanBlocks(plan); err != nil {
			return err
		}
	}
	required := make(map[string]bool)
	for _, step := range plan.Steps {
		if isMembershipStep(step.Kind) {
			required[step.GroupID] = true
		} else if step.ExpectedVotersDigest != "" || step.ResultVotersDigest != "" {
			return planUnsafef("nonmembership step %d carries a voter guard", step.Seq)
		}
	}
	if len(required) != len(plan.VoterBases) {
		return planUnsafef("plan has incomplete or extraneous voter bases")
	}
	chain, err := newVoterGuardChain(plan, required)
	if err != nil {
		return err
	}
	for _, step := range plan.Steps {
		if err := chain.validateStep(step); err != nil {
			return err
		}
	}

	return nil
}

type voterGuardChain struct {
	sets     map[string]map[uint64]struct{}
	previous map[string]string
	opaque   map[string]bool
}

func newVoterGuardChain(plan Plan, required map[string]bool) (voterGuardChain, error) {
	chain := voterGuardChain{sets: make(map[string]map[uint64]struct{}, len(required)),
		previous: make(map[string]string, len(required)), opaque: make(map[string]bool, len(required))}
	last := ""
	for _, base := range plan.VoterBases {
		if !required[base.GroupID] || last != "" && comparePlanGroups(base.GroupID, last) <= 0 {
			return voterGuardChain{}, planUnsafef("plan voter bases are duplicate or out of order")
		}
		last = base.GroupID
		if err := validateVoterBase(base); err != nil {
			return voterGuardChain{}, err
		}
		chain.sets[base.GroupID] = idSet(base.Voters)
		chain.previous[base.GroupID] = VoterSetDigest(base.Voters)
	}

	return chain, nil
}

func validateVoterBase(base PlanVoterBase) error {
	if !slices.IsSorted(base.Voters) || len(slices.Compact(slices.Clone(base.Voters))) != len(base.Voters) {
		return planUnsafef("group %q has a noncanonical voter base", base.GroupID)
	}
	for _, id := range base.Voters {
		if id == 0 {
			return planUnsafef("group %q voter base contains zero ID", base.GroupID)
		}
	}

	return nil
}

func validVoterDigest(value string) bool {
	if len(value) != voterDigestHexLength {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func (chain *voterGuardChain) validateStep(step Step) error {
	if !isMembershipStep(step.Kind) {
		return nil
	}
	set := chain.sets[step.GroupID]
	if set == nil || step.ExpectedVotersDigest != chain.previous[step.GroupID] ||
		!validVoterDigest(step.ExpectedVotersDigest) || !validVoterDigest(step.ResultVotersDigest) {
		return planUnsafef("step %d has a missing or mismatched predecessor voter proof", step.Seq)
	}
	switch step.Kind {
	case StepCreateFoundingGroup:
		if len(set) != 0 || chain.opaque[step.GroupID] {
			return planUnsafef("founding group %q has an invalid voter base", step.GroupID)
		}
		// The initial voters are in the desired document, which may no
		// longer be available when an old operator is restored.
		chain.opaque[step.GroupID] = true
	case StepPromoteVoter:
		set[step.ReplicaID] = struct{}{}
	case StepDemoteVoter:
		if !chain.opaque[step.GroupID] {
			if _, voter := set[step.ReplicaID]; !voter {
				return planUnsafef("step %d demotes a nonvoter", step.Seq)
			}
		}
		delete(set, step.ReplicaID)
	case StepRemoveReplica:
		delete(set, step.ReplicaID)
	default: // Other steps preserve the simulated voter set.
	}
	if !chain.opaque[step.GroupID] && step.ResultVotersDigest != digestIDSet(set) {
		return planUnsafef("step %d has a mismatched result voter proof", step.Seq)
	}
	chain.previous[step.GroupID] = step.ResultVotersDigest
	return nil
}
