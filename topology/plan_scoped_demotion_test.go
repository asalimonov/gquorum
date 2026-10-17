//go:build !functional

package topology

import "testing"

func TestScopedDemotionNeedsExactApprovedMember(t *testing.T) {
	t.Parallel()
	input, desired, _ := scopedReadmissionPlanFixture()
	input.Approved = desired.Clone()
	desired.Version++
	desired.MetaGroup().Replicas[0].Role = ReplicaLearner
	step := Step{Seq: 1, GroupID: MetaGroupID, NodeName: "a", ReplicaID: 4, Kind: StepDemoteVoter,
		TransferNodeName: "b", TransferReplicaID: 2, ExpectedVotersDigest: VoterSetDigest([]uint64{2, 3, 4}),
		ResultVotersDigest: VoterSetDigest([]uint64{2, 3})}
	plan := Plan{Version: desired.Version, Steps: []Step{step},
		VoterBases: []PlanVoterBase{{GroupID: MetaGroupID, Voters: []uint64{2, 3, 4}}},
		Blocks:     []PlanBlock{{GroupID: MetaGroupID, SourceVersion: 1, Steps: []int{1}}}}
	if err := ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("exact approved but unconverged member cannot be demoted", err)
	}
	flat := plan.Clone()
	flat.Blocks = nil
	if err := ValidatePlanShape(input, desired, flat); err == nil {
		t.Fatal("scoped admission exception changed flat validation")
	}
	for _, mode := range []string{"unapproved member", "foreign host"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			bad := input
			bad.Approved = input.Approved.Clone()
			if mode == "unapproved member" {
				bad.Approved.MetaGroup().Replicas[0].ReplicaID = 1
			} else {
				bad.Approved.MetaGroup().Replicas[0].NodeName = "foreign"
			}
			if err := ValidatePlanShape(bad, desired, plan); err == nil {
				t.Fatal("demotion borrowed another member's approved identity")
			}
		})
	}
}
