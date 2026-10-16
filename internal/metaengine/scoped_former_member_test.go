//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func threeMemberReadmissionFixture(t *testing.T, prepared bool) scopedState {
	t.Helper()
	s, _ := readmissionDeletionFixture(t)
	if prepared {
		for i := range s.AuthorityRecords {
			s.AuthorityRecords[i].Effects = fixtureEffectReceipts(s.AuthorityRecords[i].Grant)
		}
		action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[0])
		grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: s.Target.ClusterID,
			GroupID: topology.MetaGroupID, Revision: 8}, Purpose: topology.AuthorityEffect,
			OperatorID: action.OperatorID, Action: &action,
			Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n1"}},
			Targets:   []topology.AuthorityTarget{{NodeName: "n1", MemberID: 2, ReplicaID: 2}}}
		s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
			authorityRecord: authorityRecord{Grant: grant, Effects: fixtureEffectReceipts(grant)}})
		for i := range s.AuthorityFloors {
			if s.AuthorityFloors[i].Resource.GroupID == topology.MetaGroupID {
				s.AuthorityFloors[i].Key = grant.Key
			}
		}
	} else {
		s.AuthorityRecords, s.AuthorityFloors = nil, nil
	}
	s.History = append(s.History, s.Target.Clone())
	s.Target.Version, s.Target.ParentVersion = 3, 2
	s.Target.MetaGroup().Replicas[0].ReplicaID, s.Target.MetaGroup().NextReplicaID = 3, 4
	s.Target.Group("shard0").Replicas[0].ReplicaID, s.Target.Group("shard0").NextReplicaID = 3, 4
	s.Latest = s.Target.Clone()
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = 4
	}
	op := s.Operations[0].Clone()
	op.Status.ID, op.Status.Version, op.Plan.Version = "exec-v3-a1", 3, 3
	for i := range op.Plan.Steps {
		step := &op.Plan.Steps[i]
		if step.ReplicaID == 2 {
			step.ReplicaID = 3
		}
		if step.Kind == topology.StepPromoteVoter {
			step.ResultVotersDigest = topology.VoterSetDigest([]uint64{1, 3})
		}
		if step.Kind == topology.StepRemoveReplica {
			step.ExpectedVotersDigest, step.ResultVotersDigest = topology.VoterSetDigest([]uint64{1, 3}), topology.VoterSetDigest([]uint64{3})
		}
	}
	s.Operations[0].Status.State = topology.OperationSuperseded
	for i := range s.Operations[0].Status.Blocks {
		s.Operations[0].Status.Blocks[i].State = topology.OperationSuperseded
	}
	s.Operations = append(s.Operations, op)
	for i := range s.Groups {
		s.Groups[i].Owner = op.Status.ID
	}
	s.Nodes[0].Owner = op.Status.ID
	s.Admissions[0].State = AdmissionSuperseded
	s.Admissions = append(s.Admissions, AdmissionRecord{NodeName: "n1", InstanceID: "third",
		MemberID: 3, OperatorID: "exec-v3-a1", State: AdmissionPending})
	prepareScopedFixture(t, &s)
	return s
}

func TestScopedReadmissionDerivesFormerOwnerAcrossUnpreparedApproval(t *testing.T) {
	t.Parallel()
	for _, prepared := range []bool{false, true} {
		s := threeMemberReadmissionFixture(t, prepared)
		want, wrong := uint64(1), uint64(2)
		if prepared {
			want, wrong = wrong, want
		}
		p := BeginReadmissionAuthority{NodeName: "n1", InstanceID: "third", MemberID: 3,
			PreviousMemberID: wrong, OperatorID: "exec-v3-a1", ProposalID: "third"}
		before, _ := json.Marshal(s)
		if _, err := s.applyBeginReadmissionAuthority(11, p); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("stale/unprepared former member was trusted", prepared, err)
		}
		after, _ := json.Marshal(s)
		if !bytes.Equal(before, after) {
			t.Fatal("wrong former member installed partial authority")
		}
		p.PreviousMemberID = want
		result, err := s.applyBeginReadmissionAuthority(11, p)
		if err != nil || !result.result.Accepted || len(result.result.Grants) != 2 {
			t.Fatal("actual former owner could not retire", prepared, result, err)
		}
		for _, grant := range result.result.Grants {
			for _, target := range grant.Targets {
				if target.MemberID != want {
					t.Fatal("batch retired the wrong process", target)
				}
			}
		}
		if scopedAdmissionRetirement(s.Admissions[1], &s, s.indexes, s.authorityCatalog()) {
			t.Fatal("pending exact batch permitted preparation")
		}
		for _, grant := range result.result.Grants {
			for _, receipt := range fixtureEffectReceipts(grant) {
				recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: s.Operations[1].UpdatedAt})
			}
		}
		if !scopedAdmissionRetirement(s.Admissions[1], &s, s.indexes, s.authorityCatalog()) {
			t.Fatal("complete exact retirement did not permit preparation")
		}
		retry, err := s.applyBeginReadmissionAuthority(s.AppliedIndex+1, p)
		if err != nil || retry.changed || len(retry.result.Grants) != 2 {
			t.Fatal("completed retry derived a different former owner", retry, err)
		}
	}
}

func TestScopedReadmissionRefusesContradictoryMultipleFormerOwners(t *testing.T) {
	t.Parallel()
	s := threeMemberReadmissionFixture(t, true)
	// Corrupt the prerequisite proof behind the already issued second member.
	// A new batch must not silently select just one of two possible owners.
	for i := range s.AuthorityRecords {
		if s.AuthorityRecords[i].Grant.Purpose == topology.AuthorityReadmit {
			s.AuthorityRecords[i].Effects = nil
		}
	}
	s.indexes, _ = s.buildIndexes()
	before, _ := json.Marshal(s)
	p := BeginReadmissionAuthority{NodeName: "n1", InstanceID: "third", MemberID: 3,
		PreviousMemberID: 2, OperatorID: "exec-v3-a1", ProposalID: "contradictory"}
	if _, err := s.applyBeginReadmissionAuthority(11, p); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("ambiguous former owner was selected", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) || scopedAdmissionRetirement(s.Admissions[1], &s, s.indexes, s.authorityCatalog()) {
		t.Fatal("contradictory old ownership authorized preparation or changed state")
	}
}
