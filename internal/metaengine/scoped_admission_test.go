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

func scopedAdmissionFixture() scopedState {
	s := scopedGraphFixture()
	approved := s.Target.Clone()
	approved.Nodes = append(approved.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	approved.MetaGroup().Replicas = append(approved.MetaGroup().Replicas,
		topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	approved.MetaGroup().NextReplicaID, s.Allocations[0].NextReplicaID = 3, 3
	s.Target, s.Latest = approved, approved.Clone()
	op := s.Operations[0].Clone()
	op.Status.State, op.Status.TotalBlocks = topology.OperationPending, 1
	op.Status.Blocks = []topology.BlockStatus{{GroupID: topology.MetaGroupID,
		State: topology.OperationPending, CurrentStep: 1, TotalSteps: 2}}
	op.Plan.Steps = []topology.Step{
		{Seq: 1, Kind: topology.StepPrepareReplica, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2},
		{Seq: 2, Kind: topology.StepAddLearner, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2},
	}
	op.Plan.Blocks = []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 1, Steps: []int{1, 2}}}
	op.Plan.VoterBases = []topology.PlanVoterBase{{GroupID: topology.MetaGroupID, Voters: []uint64{1}}}
	op.Plan.Steps[1].ExpectedVotersDigest = topology.VoterSetDigest([]uint64{1})
	op.Plan.Steps[1].ResultVotersDigest = op.Plan.Steps[1].ExpectedVotersDigest
	s.Operations = []topology.Operation{op}
	s.Groups[0].SourceVersion, s.Groups[0].Declaration, s.Groups[0].Owner = 1, s.History[0].MetaGroup(), op.Status.ID
	s.Groups[1].Owner = ""
	s.Nodes = append(s.Nodes, nodeConvergence{NodeName: "n2", Owner: op.Status.ID})
	s.AuthorityRecords, s.AuthorityFloors, s.Revocations = nil, nil, nil
	s.Admissions = []AdmissionRecord{{NodeName: "n2", InstanceID: "process", MemberID: 2,
		OperationID: topology.OperationID(op.Status.ID), State: AdmissionPending}}
	return s
}

func TestScopedAdmissionKeepsItsPinnedApprovalAcrossUnrelatedVersions(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s.History = append(s.History, s.Target.Clone())
	s.Target.Version, s.Target.ParentVersion = 3, 2
	s.Latest = s.Target.Clone()
	s.Admissions[0].State = AdmissionReady // Local readiness can precede the effect acknowledgement.
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal(err)
	}
	s.Operations[0].Status.State, s.Operations[0].Status.Blocks[0].State = topology.OperationFailed, topology.OperationFailed
	s.Groups[0].Owner, s.Nodes[1].Owner = "", ""
	s.Admissions[0].State = AdmissionPending
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("failed admission lost retry evidence", err)
	}
}

func TestScopedAdmissionRejectsContradictoryLineageAndMilestones(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"missing operation":          func(s *scopedState) { s.Admissions[0].OperationID = "exec-v2-a9" },
		"foreign process":            func(s *scopedState) { s.Admissions[0].InstanceID = "" },
		"reused member":              func(s *scopedState) { s.Admissions[0].MemberID = 1 },
		"unallocated member":         func(s *scopedState) { s.Admissions[0].MemberID = 3 },
		"missing readmission origin": func(s *scopedState) { s.Admissions[0].ReadmissionInstanceID = "missing" },
		"wrong sequence":             func(s *scopedState) { s.Operations[0].Plan.Steps[1].Kind = topology.StepStopReplica },
		"learner without effect":     func(s *scopedState) { s.Admissions[0].State = AdmissionLearner },
		"promotion without effect":   func(s *scopedState) { s.Admissions[0].State = AdmissionPromoted },
		"completion without block":   func(s *scopedState) { s.Admissions[0].State = AdmissionCompleted },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedAdmissionFixture()
			good, _ := json.Marshal(s)
			var retained scopedState
			if err := json.Unmarshal(good, &retained); err != nil {
				t.Fatal(err)
			}
			index := retained.indexes
			corrupt(&s)
			if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("invalid admission accepted", err)
			}
			bad, _ := json.Marshal(s)
			if err := json.Unmarshal(bad, &retained); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("decoder installed invalid admission", err)
			}
			after, _ := json.Marshal(retained)
			if !bytes.Equal(good, after) || retained.indexes != index {
				t.Fatal("invalid admission replaced prior state or indexes")
			}
		})
	}
}

func TestScopedAdmissionLearnerRequiresItsRecordedNativeEffect(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	op := &s.Operations[0]
	op.Plan.Steps = append(op.Plan.Steps, topology.Step{Seq: 3, Kind: topology.StepTransferLeader,
		GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1})
	op.Plan.Blocks[0].Steps = []int{1, 2, 3}
	op.Status.Blocks[0].CurrentStep, op.Status.Blocks[0].TotalSteps = 3, 3
	s.Admissions[0].State = AdmissionLearner
	admissionPrefixReceipt(&s, 0, 1)
	admissionPrefixReceipt(&s, 0, 2)
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal(err)
	}
	s.AuthorityRecords[1].Effects = nil
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("learner without exact native proof", err)
	}
}

func TestScopedAdmissionReadinessRequiresCompleteOldOwnerRetirement(t *testing.T) {
	t.Parallel()
	s := scopedReadmissionCleanupFixture()
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := restoreScopedAuthority(&s, index)
	if err != nil {
		t.Fatal(err)
	}
	admission := s.Admissions[len(s.Admissions)-1]
	if !scopedAdmissionRetirement(admission, &s, index, catalog) {
		t.Fatal("complete readmission rejected")
	}
	for _, record := range catalog.records {
		if record.Grant.Purpose == topology.AuthorityReadmit && record.Grant.Key.GroupID == topology.MetaGroupID {
			record.Effects = nil
		}
	}
	if scopedAdmissionRetirement(admission, &s, index, catalog) {
		t.Fatal("readiness bypassed incomplete meta retirement")
	}
}

func TestScopedFoundingAdmissionIgnoresFailedLoadSibling(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	op := &s.Operations[1]
	op.Status.State, op.Status.TotalBlocks = topology.OperationFailed, 2
	op.Plan.Steps = []topology.Step{{Seq: 1, Kind: topology.StepPrepareReplica, GroupID: "live", NodeName: "n", ReplicaID: 1}}
	op.Plan.Blocks = append(op.Plan.Blocks, topology.PlanBlock{GroupID: "live", Steps: []int{1}})
	op.Status.Blocks = append(op.Status.Blocks, topology.BlockStatus{GroupID: "live", State: topology.OperationFailed, CurrentStep: 1, TotalSteps: 1})
	s.Groups[0] = groupConvergence{GroupID: "live"}
	s.FullyConverged = topology.FullyConverged{}
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("load failure invalidated founding admission", err)
	}
	op.Status.Blocks[0].State = topology.OperationFailed
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("founding without meta convergence", err)
	}
}
