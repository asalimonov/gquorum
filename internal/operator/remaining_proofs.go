package operator

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func membershipStep(kind StepKind) bool {
	switch kind {
	case StepCreateFoundingGroup,
		StepAddLearner,
		StepPromoteVoter,
		StepDemoteVoter,
		StepRemoveReplica:
		return true
	default:
		return false
	}
}

func voterIDs(voters map[uint64]bool) []uint64 {
	ids := make([]uint64, 0, len(voters))
	for id := range voters {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// The metadata read supplies exact granted actions and recorded receipts from
// one state snapshot. A native receipt can close the crash window after Raft
// applies an effect but before its receipt reaches metadata.
func remainingStepProofs(ex Record, native map[string]topology.NativeState) (map[int]bool, error) {
	grants, err := remainingGrants(ex)
	if err != nil {
		return nil, err
	}
	receipts, err := remainingReceipts(ex, grants)
	if err != nil {
		return nil, err
	}
	return completedRemainingSteps(grants, receipts, native)
}

func remainingGrants(ex Record) (map[topology.AuthorityKey]topology.AuthorityGrant, error) {
	grants := make(map[topology.AuthorityKey]topology.AuthorityGrant, len(ex.Grants))
	for _, grant := range ex.Grants {
		if err := validateRemainingGrant(ex, grant); err != nil {
			return nil, err
		}
		if _, duplicate := grants[grant.Key]; duplicate {
			return nil, ErrOperatorAuthorization
		}
		grants[grant.Key] = grant
	}
	return grants, nil
}

func validateRemainingGrant(ex Record, grant topology.AuthorityGrant) error {
	validationErr := grant.Validate()
	if validationErr != nil || grant.Action == nil || grant.OperatorID != ex.ID {
		return ErrOperatorAuthorization
	}
	seq := grant.Action.StepSeq
	if seq < 1 || seq > len(ex.Steps) || !grant.Action.Matches(ex.ID, ex.Attempt, ex.Version, ex.Steps[seq-1]) {
		return ErrOperatorAuthorization
	}
	return nil
}

func remainingReceipts(ex Record, grants map[topology.AuthorityKey]topology.AuthorityGrant) (
	map[topology.AuthorityKey][]topology.EffectReceipt, error) {
	receipts := make(map[topology.AuthorityKey][]topology.EffectReceipt)
	for _, receipt := range ex.Receipts {
		grant, exists := grants[receipt.Key]
		if !exists || receipt.Validate(grant) != nil {
			return nil, ErrOperatorAuthorization
		}
		receipts[receipt.Key] = append(receipts[receipt.Key], receipt)
	}
	return receipts, nil
}

func completedRemainingSteps(grants map[topology.AuthorityKey]topology.AuthorityGrant,
	receipts map[topology.AuthorityKey][]topology.EffectReceipt,
	native map[string]topology.NativeState) (map[int]bool, error) {
	completed := make(map[int]bool)
	for key, grant := range grants {
		complete, err := remainingGrantComplete(grant, receipts[key], native)
		if err != nil {
			return nil, err
		}
		completed[grant.Action.StepSeq] = complete
	}
	return completed, nil
}

func remainingGrantComplete(grant topology.AuthorityGrant, receipts []topology.EffectReceipt,
	native map[string]topology.NativeState) (bool, error) {
	all := slices.Clone(receipts)
	all, err := appendNativeReceipt(grant, all, native)
	if err != nil {
		return false, err
	}
	complete, err := receiptSetComplete(grant, all)
	if err != nil {
		return false, err
	}
	if !complete && grant.Action.Kind == StepCreateFoundingGroup {
		complete = remainingFoundingComplete(grant, native)
	}
	return complete, nil
}

func appendNativeReceipt(grant topology.AuthorityGrant, existing []topology.EffectReceipt,
	native map[string]topology.NativeState) ([]topology.EffectReceipt, error) {
	state, ok := native[grant.Key.GroupID]
	if !ok || state.Grant == nil || state.Receipt == nil || !state.Grant.Equal(grant) {
		return existing, nil
	}
	if state.Receipt.Validate(grant) != nil {
		return nil, fault.ErrUnreadable
	}
	if !slices.ContainsFunc(existing, func(old topology.EffectReceipt) bool {
		return old.Key == state.Receipt.Key &&
			old.Resource == state.Receipt.Resource && old.Target == state.Receipt.Target
	}) {
		return append(existing, state.Receipt.Clone()), nil
	}
	return existing, nil
}

func remainingFoundingComplete(grant topology.AuthorityGrant, native map[string]topology.NativeState) bool {
	// A founder can run before its first group receipt is recorded. Its initial
	// peers were checked against the exact grant before storage acquisition; this
	// quorum-confirmed read proves the voter result, but the receipt is still owed.
	state, ok := native[grant.Key.GroupID]
	if !ok || state.Grant == nil {
		return false
	}
	grantMatches := state.Grant.Equal(grant)
	return grantMatches && state.Applied != 0 &&
		topology.VoterSetDigest(state.ConfState.Voters) == grant.Action.ResultVotersDigest
}
