//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedVoterCompletionMayFollowConsumedPredecessor(t *testing.T) {
	t.Parallel()
	s := scopedVoterFixture()
	s.Operations[0].Status.Blocks[0].State = topology.OperationFailed
	s.Operations[1].Status.Blocks[0].State = topology.OperationPending
	s.Groups[0].SourceVersion, s.Groups[0].Declaration = 1, s.History[0].MetaGroup()
	s.Groups[0].Owner = s.Operations[1].Status.ID
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("consumption without recorded completion created cooldown", err)
	}
	s.Operations[1].Status.Blocks[0].State = topology.OperationSucceeded
	s.Groups[0].SourceVersion, s.Groups[0].Declaration, s.Groups[0].Owner = 2, s.Target.MetaGroup(), ""
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("retry completion lost consumed predecessor", err)
	}
	s.AuthorityRecords[0].Effects = nil
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("fence substituted for role consumption", err)
	}
}

func TestScopedVoterDemotionMatchesOriginalRoleEffect(t *testing.T) {
	t.Parallel()
	s := scopedVoterFixture()
	s.History[0].MetaGroup().Replicas[1].Role = topology.ReplicaVoter
	s.Target.MetaGroup().Replicas[1].Role = topology.ReplicaLearner
	s.Latest, s.Groups[0].Declaration = s.Target.Clone(), s.Target.MetaGroup()
	for i := range s.Operations {
		plan := &s.Operations[i].Plan
		plan.VoterBases[0].Voters = []uint64{1, 2}
		step := &plan.Steps[0]
		step.Kind = topology.StepDemoteVoter
		step.ExpectedVotersDigest, step.ResultVotersDigest = step.ResultVotersDigest, step.ExpectedVotersDigest
	}
	action := s.AuthorityRecords[0].Grant.Action
	action.Kind = topology.StepDemoteVoter
	action.ExpectedVotersDigest, action.ResultVotersDigest = action.ResultVotersDigest, action.ExpectedVotersDigest
	s.VoterSwitches[0].Last.From, s.VoterSwitches[0].Last.To = topology.ReplicaVoter, topology.ReplicaLearner
	s.VoterSwitches[0].Replicas[0] = s.VoterSwitches[0].Last
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("exact consumed demotion refused", err)
	}
}

func TestScopedVoterRetainsOneRemovedCooldownAnchor(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	group := &s.VoterSwitches[1] // a: current replica 8; former 6 has no other retained reference.
	group.Last.ReplicaID = 6
	group.Replicas[0] = group.Last
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("removed last holder lost group cooldown", err)
	}
	newer := group.Last
	newer.ReplicaID, newer.Index = 8, 27
	group.Last, group.Replicas = newer, append(group.Replicas, newer)
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("unreferenced non-last role history grew without bound", err)
	}
}

func TestScopedVoterRejectsBrokenSummaryAndStagedProvenance(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"missing last":  func(s *scopedState) { s.VoterSwitches[0].Replicas = nil },
		"unknown group": func(s *scopedState) { s.VoterSwitches[0].GroupID = "absent" },
		"staged approval": func(s *scopedState) {
			s.VoterSwitches[0].Last.Version, s.VoterSwitches[0].Last.OperationID = 4, "exec-v4-a1"
			s.VoterSwitches[0].Replicas[0] = s.VoterSwitches[0].Last
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedCodecFixture()
			corrupt(&s)
			if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("invalid role summary accepted", err)
			}
		})
	}
}

func TestScopedVoterCompletionIndexMayBeSharedAcrossGroups(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	// An atomic retry may acknowledge previously consumed role changes in
	// several failed blocks. The exact action/receipt remains group-specific.
	s.VoterSwitches[0].Last.Index = s.VoterSwitches[1].Last.Index
	s.VoterSwitches[0].Replicas[0] = s.VoterSwitches[0].Last
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("shared metadata Apply index rejected independent role effects", err)
	}
}
