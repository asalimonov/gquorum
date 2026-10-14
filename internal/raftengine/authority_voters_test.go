//go:build !functional

package raftengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestValidateAuthorizedVoterSet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind          topology.StepKind
		before, after []uint64
		id            uint64
	}{
		{topology.StepAddLearner, []uint64{1, 2}, []uint64{1, 2}, 3},
		{topology.StepPromoteVoter, []uint64{1, 2}, []uint64{1, 2, 3}, 3},
		{topology.StepDemoteVoter, []uint64{1, 2, 3}, []uint64{1, 2}, 3},
		{topology.StepRemoveReplica, []uint64{1, 2, 3}, []uint64{1, 2}, 3},
		{topology.StepRemoveReplica, []uint64{1, 2}, []uint64{1, 2}, 3}, // learner removal
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			a := topology.AuthorizedStep{Kind: tc.kind, ReplicaID: tc.id,
				ExpectedVotersDigest: topology.VoterSetDigest(tc.before), ResultVotersDigest: topology.VoterSetDigest(tc.after)}
			if err := validateAuthorizedVoterSet(&a, tc.before, approvedVoterSet); err != nil {
				t.Fatal(err)
			}
			if err := validateAuthorizedVoterSet(&a, tc.after, resultingVoterSet); err != nil {
				t.Fatal(err)
			}
			if err := validateAuthorizedVoterSet(&a, []uint64{99}, approvedVoterSet); !errors.Is(err, fault.ErrUnsafeMembership) {
				t.Fatalf("native drift passed predecessor: %v", err)
			}
			a.ResultVotersDigest = topology.VoterSetDigest([]uint64{99})
			if err := validateAuthorizedVoterSet(&a, tc.before, approvedVoterSet); !errors.Is(err, fault.ErrUnsafeMembership) {
				t.Fatalf("forged projection passed: %v", err)
			}
			a.ExpectedVotersDigest = ""
			if err := validateAuthorizedVoterSet(&a, tc.before, approvedVoterSet); !errors.Is(err, fault.ErrUnsafeMembership) {
				t.Fatalf("missing predecessor passed: %v", err)
			}
		})
	}
}

func TestCommittedManagedConfigurationRejectsVoterDriftBeforeApply(t *testing.T) {
	t.Parallel()
	a := topology.AuthorizedStep{Kind: topology.StepAddLearner, GroupID: "work", NodeName: "n2", ReplicaID: 2,
		ExpectedVotersDigest: topology.VoterSetDigest([]uint64{99}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1})}
	grant := topology.AuthorityGrant{Action: &a}
	g := &Group{cs: raftpb.ConfState{Voters: []uint64{1}}, authority: raftstate.AuthorityState{Grant: &grant}}
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}}}
	entry := raftstate.Entry{Grant: &grant, Action: &a}
	if err := g.applyAuthorizedConfChange(raftpb.Entry{Index: 2, Term: 1}, cc, entry); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatalf("committed drift reached Raft: %v", err)
	}
	if len(g.cs.Learners) != 0 || len(g.cs.Voters) != 1 || g.cs.Voters[0] != 1 {
		t.Fatalf("invalid commit changed membership: %+v", g.cs)
	}
}
