package metaengine

import (
	"reflect"

	"github.com/asalimonov/gquorum/fault"
)

func (s *scopedState) applyCommitVoterChange(index uint64,
	p CommitVoterChangeProposal) (proposalApplication, error) {
	repair := CommitRepairOperationProposal{Doc: p.Doc, ExpectedVersion: p.ExpectedVersion,
		ExpectedTarget: p.ExpectedTarget, Scopes: p.Scopes, Plan: p.Plan, At: p.At, ProposalID: p.ProposalID}
	if p.ExpectedVersion != s.Latest.Version || p.ExpectedTarget != s.Target.Version {
		return s.repairConflict(repair), nil
	}
	pair, err := s.prepareRepairDocuments(p.Doc, p.At)
	if err != nil {
		return proposalApplication{}, err
	}
	want, err := VoterChangeDocument(s.Target, p.Change, p.At, p.Doc.ChangedBy)
	if err != nil {
		return proposalApplication{}, err
	}
	want.Version, want.CreatedAt, want.RebasedFrom = pair.Repair.Version, pair.Repair.CreatedAt, 0
	canonicalizeDocument(&want)
	if !reflect.DeepEqual(want, pair.Repair) {
		return proposalApplication{}, fault.New(fault.OperationAuthorization,
			"voter change edits fields outside exact role scope", nil)
	}
	var voters []uint64
	for _, base := range p.Plan.VoterBases {
		if base.GroupID == p.Change.GroupID {
			voters = base.Voters
		}
	}
	if err := validateVoterChangeSteps(pair.Repair, p.Change, p.Plan, voters); err != nil {
		return proposalApplication{}, err
	}
	// Scoped shape validation checks captured native guard chains and exact
	// source/owner before the shared installer can publish either document.
	return s.commitRepairDocuments(index, pair, repair)
}
