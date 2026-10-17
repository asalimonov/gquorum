//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func promotedAdmissionFixture() scopedState {
	s := scopedAdmissionFixture()
	s.Target.MetaGroup().Replicas[1].Role = topology.ReplicaVoter
	s.Latest = s.Target.Clone()
	op := &s.Operations[0]
	op.Plan.Steps = append(op.Plan.Steps,
		topology.Step{Seq: 3, Kind: topology.StepWaitCaughtUp, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2},
		topology.Step{Seq: 4, Kind: topology.StepPromoteVoter, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2,
			ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1, 2})},
		topology.Step{Seq: 5, Kind: topology.StepTransferLeader, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1})
	op.Plan.Blocks[0].Steps = []int{1, 2, 3, 4, 5}
	op.Status.Blocks[0].CurrentStep, op.Status.Blocks[0].TotalSteps = 5, 5
	for _, seq := range []int{1, 2, 4} {
		admissionPrefixReceipt(&s, 0, seq)
	}
	s.Admissions[0].State = AdmissionPromoted
	return s
}

func admissionDemotion() topology.Step {
	return topology.Step{Kind: topology.StepDemoteVoter, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2,
		TransferNodeName: "n1", TransferReplicaID: 1,
		ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1, 2}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1})}
}

func demotingAdmissionFixture(steps []topology.Step) scopedState {
	s := promotedAdmissionFixture()
	s = admissionSuccessorFixture(s, steps)
	s.Target.MetaGroup().Replicas[1].Role = topology.ReplicaLearner
	s.Latest = s.Target.Clone()
	if len(steps) != 0 {
		s.Operations[1].Plan.VoterBases[0].Voters = []uint64{1, 2}
	}
	return s
}

func TestScopedAdmissionDemotionRequiresExactRoleChange(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"demote", "transfer and demote", "missing demote", "duplicate demote", "unpromoted demote"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			steps := []topology.Step{admissionDemotion()}
			switch mode {
			case "transfer and demote":
				steps = append([]topology.Step{{Kind: topology.StepTransferLeader, GroupID: topology.MetaGroupID,
					NodeName: "n1", ReplicaID: 1}}, steps...)
			case "missing demote":
				steps = nil
			case "duplicate demote":
				steps = append(steps, admissionDemotion())
			default:
			}
			s := demotingAdmissionFixture(steps)
			if mode == "unpromoted demote" {
				s.AuthorityRecords[2].Effects = nil
				s.Admissions[0].State = AdmissionLearner
			}
			_, err := prepareScopedGraph(&s, s.Rules)
			valid := mode == "demote" || mode == "transfer and demote"
			if (err == nil) != valid {
				t.Fatal("incorrect demotion grammar", err)
			}
		})
	}
}

func TestScopedAdmissionDemoteOnlyOwnerSupportsRepromotion(t *testing.T) {
	t.Parallel()
	s := demotingAdmissionFixture([]topology.Step{admissionDemotion()})
	admissionPrefixReceipt(&s, 1, 1)
	steps := s.Operations[0].Plan.Clone().Steps[2:4]
	s = admissionSuccessorFixture(s, steps)
	s.Target.MetaGroup().Replicas[1].Role = topology.ReplicaVoter
	s.Latest = s.Target.Clone()
	s.Operations[2].Plan.VoterBases[0].Voters = []uint64{1}
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("intermediate demotion was lost before re-promotion", err)
	}
	s.AuthorityRecords[len(s.AuthorityRecords)-1].Effects = nil
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("unconsumed demotion changed native role", err)
	}
}

func TestScopedAdmissionCheckpointRoleOrdering(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"before demote", "after demote", "member absent", "live"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := demotingAdmissionFixture([]topology.Step{admissionDemotion()})
			admissionPrefixReceipt(&s, 1, 1)
			s = admissionSuccessorFixture(s, nil)
			s.Admissions[0].State = AdmissionSuperseded
			checkpoint := &s.Groups[0]
			switch mode {
			case "before demote":
				checkpoint.SourceVersion, checkpoint.Declaration = 2, s.History[1].MetaGroup()
			case "after demote", "live":
				checkpoint.SourceVersion, checkpoint.Declaration = 3, s.History[2].MetaGroup()
				if mode == "live" {
					s.Admissions[0].State = AdmissionPromoted
				}
			default: // Version 1 does not contain this exact member.
			}
			index, err := s.buildIndexes()
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := restoreScopedAuthority(&s, index)
			if err != nil {
				t.Fatal(err)
			}
			progress, valid := scopedAdmissionProgress(s.Admissions[0], &s.Operations[2], index, catalog)
			if mode == "live" {
				if valid {
					t.Fatal("live admission covered by convergence")
				}
				return
			}
			if !valid || progress.nativeRole != topology.ReplicaLearner || !progress.promoted {
				t.Fatal("checkpoint reset role or erased historical promotion", progress, valid)
			}
		})
	}
}

func TestScopedAdmissionDemotionCannotPrecedeOrigin(t *testing.T) {
	t.Parallel()
	s := demotingAdmissionFixture([]topology.Step{admissionDemotion()})
	admissionPrefixReceipt(&s, 1, 1)
	s = admissionSuccessorFixture(s, nil)
	s = admissionSuccessorFixture(s, nil)
	s.Admissions[0].ReadmissionOperatorID = s.Operations[2].Status.ID
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := restoreScopedAuthority(&s, index)
	if err != nil {
		t.Fatal(err)
	}
	// Isolate the demotion issuance rule; structural origin eligibility is
	// separately validated by the full graph.
	for key, record := range catalog.records {
		if record.Grant.Action.Kind != topology.StepDemoteVoter {
			delete(catalog.records, key)
		}
	}
	if _, valid := admissionPrefixOperations(s.Admissions[0], &s.Operations[3], index, catalog); valid {
		t.Fatal("older issued Demote did not constrain the origin")
	}
}

func TestScopedApprovalDemotesPromotedUnconvergedAdmission(t *testing.T) {
	t.Parallel()
	s := promotedAdmissionFixture()
	s, proposal := admissionApprovalFixture(t, s, 5)
	s.Latest.MetaGroup().Replicas[1].Role = topology.ReplicaLearner
	step := admissionDemotion()
	step.Seq = 1
	proposal.Plan.Steps, proposal.Plan.Blocks[0].Steps = []topology.Step{step}, []int{1}
	proposal.Plan.VoterBases = []topology.PlanVoterBase{{GroupID: topology.MetaGroupID, Voters: []uint64{1, 2}}}
	next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
	if err != nil {
		t.Fatal("promoted unconverged member could not hand over to demotion", err)
	}
	if next.Admissions[0].State != AdmissionPromoted || next.Admissions[0].ReadmissionOperatorID != "exec-v2-a1" {
		t.Fatal("demotion handover erased historical admission progress", next.Admissions[0])
	}
	proposal.Plan.Steps, proposal.Plan.Blocks[0].Steps, proposal.Plan.VoterBases = nil, nil, nil
	if _, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval); err == nil {
		t.Fatal("approval manufactured demotion from its desired role")
	}
}

func TestScopedAdmissionPromotedRetryKeepsImmutableGrammar(t *testing.T) {
	t.Parallel()
	s := promotedAdmissionFixture()
	next := admissionRetryCandidate(t, &s)
	if err := prepareAdmissionHandovers(&s, next); err != nil {
		t.Fatal("consumed promotion invalidated its immutable retry", err)
	}
	if next.Admissions[0].State != AdmissionPromoted || !next.Operations[0].Plan.Equal(next.Operations[1].Plan) {
		t.Fatal("retry changed immutable work or lost promotion")
	}
}

func TestScopedAdmissionCheckpointWithoutMemberCannotLeaveAdmissionLive(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	admissionPrefixReceipt(&s, 0, 1)
	admissionPrefixReceipt(&s, 0, 2)
	s = admissionSuccessorFixture(s, nil)
	// The exact-member role initializer stays empty, but the newer metadata
	// checkpoint still proves this admission must have completed or cancelled.
	s.Groups[0].SourceVersion = 2
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := restoreScopedAuthority(&s, index)
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := scopedAdmissionProgress(s.Admissions[0], &s.Operations[1], index, catalog); valid {
		t.Fatal("checkpoint excluding the member left its admission live")
	}
	s.Admissions[0].State = AdmissionSuperseded
	if _, valid := scopedAdmissionProgress(s.Admissions[0], &s.Operations[1], index, catalog); !valid {
		t.Fatal("terminal admission lost its historical prefix")
	}
}
