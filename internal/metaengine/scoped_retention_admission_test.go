//go:build !functional

package metaengine

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestScopedRetentionMetadataPreparationSurvivesWithoutDependentOrSourceDocument(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	admissionPrefixReceipt(&s, 0, 1)
	s.Operations[0].Status.State, s.Operations[0].Status.Blocks[0].State = topology.OperationFailed, topology.OperationFailed
	s.Admissions[0].State = AdmissionSuperseded
	s.Groups[0].Owner, s.Nodes[1].Owner = "", ""
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	if len(s.AuthorityRecords) != 1 || s.indexes.documents[2].Version != 0 || len(s.Operations) != 0 {
		t.Fatal("preparation barrier did not compact independently of its old operation/document")
	}
	if !s.metadataPrepared("n2", 2, s.authorityCatalog()) || s.metadataPrepared("n2", 3, s.authorityCatalog()) {
		t.Fatal("archived preparation lost or widened exact member proof")
	}
	// Equivalent metadata convergence discharges this independent barrier root.
	s.Groups[0].SourceVersion, s.Groups[0].Declaration = s.Target.Version, s.Target.MetaGroup()
	prepareScopedFixture(t, &s)
	pruneScopedFixture(t, &s)
	if len(s.AuthorityRecords) != 0 || !s.metadataPrepared("n2", 2, s.authorityCatalog()) {
		t.Fatal("convergence did not replace preparation proof")
	}
}

func TestScopedRetentionKeepsEveryIssuedNeverConvergedFoundingOutcome(t *testing.T) {
	t.Parallel()
	for _, outcome := range []topology.EffectOutcome{"", topology.EffectApplied, topology.EffectAlreadySatisfied} {
		t.Run(string(outcome), func(t *testing.T) {
			t.Parallel()
			s, approval := foundingApprovalFixture(t)
			if _, err := foundingCommand(t, &s, approval, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
				t.Fatal(err)
			}
			op := s.Operations[0].Clone()
			applyScopedTransitionFixture(t, &s, scopedTransitionFor(op, "shard0", 1))
			action := scopedPlanAction(&op, op.Plan.Steps[0])
			issued, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: action})
			if err != nil {
				t.Fatal(err)
			}
			if outcome != "" {
				for i := range s.AuthorityRecords {
					s.AuthorityRecords[i].Effects = fixtureEffectReceipts(s.AuthorityRecords[i].Grant)
					for j := range s.AuthorityRecords[i].Effects {
						s.AuthorityRecords[i].Effects[j].Outcome = outcome
					}
				}
			}
			failed := scopedTransitionFor(s.Operations[0], "shard0", 1)
			failed.State, failed.ErrorCode, failed.Error = topology.OperationFailed, "unavailable", "founder unreachable"
			applyScopedTransitionFixture(t, &s, failed)
			s.Activated = []string{"n1"}
			ageScopedHistory(t, &s)
			pruneScopedFixture(t, &s)
			if s.indexes.grants[issued.result.Grant.Key] == nil {
				t.Fatal("issued founding proof was pruned")
			}
			collector := s.planningCollector()
			input, err := collector.collect(s.planningDeclarations(), s.Target)
			if err != nil || !input.Retained["shard0"].FoundingIssued {
				t.Fatal("planner could refound established identity", err)
			}
		})
	}
}

func TestScopedRetentionReadmissionKeepsWholeBatchAndEveryProcessOrigin(t *testing.T) {
	t.Parallel()
	s := scopedReadmissionCleanupFixture()
	origin := s.Admissions[len(s.Admissions)-1]
	replacement := origin
	replacement.InstanceID, replacement.ReadmissionInstanceID = "new-process", origin.InstanceID
	s.Admissions = append(s.Admissions, replacement)
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	id := ReadmissionID(origin)
	var grants, processes int
	for _, record := range s.AuthorityRecords {
		if record.Grant.AdmissionID == id {
			grants++
		}
	}
	for _, row := range s.Admissions {
		if ReadmissionID(row) == id {
			processes++
		}
	}
	if grants != 2 || processes != 2 || s.indexes.operations["exec-v2-a1"] == nil || s.indexes.operations["exec-v3-a1"] == nil {
		t.Fatal("readmission origin or complete batch pruned", grants, processes)
	}
	if !s.authorityCatalog().readmissionBatchConsumed(id) {
		t.Fatal("retirement stopped being consumed after pruning")
	}
}

func TestScopedRetentionPendingDeletionPreservesFormerInventory(t *testing.T) {
	t.Parallel()
	s, deletion := readmissionDeletionFixture(t)
	applyScopedApprovalFixture(t, &s, deletion)
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	if !s.indexes.groups["shard0"].Deleting || len(s.AuthorityRecords) != 2 || len(s.Admissions) != 1 ||
		s.indexes.operations["exec-v2-a1"] == nil || s.indexes.documents[1].Version == 0 {
		t.Fatal("pending deletion lost exact former owner/readmission inventory")
	}
}

func TestScopedRetentionKeepsPriorConsumedBatchWithoutFormerOwnerAddSource(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	base := &s.History[0]
	base.Nodes = append(base.Nodes, *s.Target.Node("n2"))
	base.MetaGroup().Replicas = append(base.MetaGroup().Replicas,
		topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	base.MetaGroup().NextReplicaID = 3
	s.Target.MetaGroup().Replicas[1].ReplicaID, s.Target.MetaGroup().NextReplicaID = 3, 4
	s.Latest, s.Allocations[0].NextReplicaID = s.Target.Clone(), 4
	op := &s.Operations[0]
	for i := range op.Plan.Steps {
		op.Plan.Steps[i].ReplicaID = 3
	}
	op.Status.State, op.Status.Blocks[0].State = topology.OperationSucceeded, topology.OperationSucceeded
	op.Status.Blocks[0].CurrentStep = 2
	s.Groups[0].SourceVersion, s.Groups[0].Declaration, s.Groups[0].Owner = 2, s.Target.MetaGroup(), ""
	s.Nodes[1].Owner = ""
	old := s.Admissions[0]
	old.MemberID, old.State = 3, AdmissionSuperseded
	current := old
	current.InstanceID, current.State = "later-process", AdmissionCompleted
	s.Admissions = []AdmissionRecord{old, current}
	admissionPrefixReceipt(&s, 0, 1)
	admissionPrefixReceipt(&s, 0, 2)
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n2"}
	for i, row := range s.Admissions {
		grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: s.Target.ClusterID,
			GroupID: topology.MetaGroupID, Revision: uint64(1 + i)}, Purpose: topology.AuthorityReadmit,
			AdmissionID: ReadmissionID(row), Resources: []topology.AuthorityResource{resource},
			Targets: []topology.AuthorityTarget{{NodeName: "n2", MemberID: 2, ReplicaID: 2}}}
		record := scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{Grant: grant}}
		if i == 0 {
			record.Effects = fixtureEffectReceipts(grant)
		}
		s.AuthorityRecords = append(s.AuthorityRecords, record)
	}
	ageScopedHistory(t, &s)
	if len(unretiredFormerMembers(&s, s.indexes, newAuthorityCatalog(), current)) != 0 {
		t.Fatal("fixture still has an independent former-owner add-source")
	}
	pruneScopedFixture(t, &s)
	if len(s.AuthorityRecords) != 4 || len(s.Admissions) != 2 ||
		!scopedAdmissionRetirement(current, &s, s.indexes, s.authorityCatalog()) {
		t.Fatal("Completed admission lost its previous consumed retirement witness")
	}
}

func TestScopedRetentionPinsIssuedIntermediateAdmissionPrefix(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s = admissionSuccessorFixture(s, s.Operations[0].Plan.Steps)
	admissionPrefixReceipt(&s, 1, 1)
	s = admissionSuccessorFixture(s, s.Operations[1].Plan.Steps)
	ageScopedHistory(t, &s)
	pruneScopedFixture(t, &s)
	for _, version := range []uint64{2, 3, 4} {
		if s.indexes.operations[topology.MakeOperationID(version, 1)] == nil || s.indexes.documents[version].Version == 0 {
			t.Fatal("handover lost issued intermediate prefix provenance", version)
		}
	}
}

func TestScopedIntermediateMemberRoutingSurvivesPruningAndRestore(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	admissionPrefixReceipt(&s, 0, 1)
	m := restoreMachineFixture(t, s)
	desired := s.History[0].Clone()
	desired.MetaGroup().NextReplicaID = s.Target.MetaGroup().NextReplicaID
	desired.ParentVersion = s.Target.Version
	staged := applyMachine(t, m, ProposeDocument{Doc: desired, ExpectedVersion: s.Target.Version, ProposalID: "abandon-member"})
	if !staged.Accepted {
		t.Fatal(staged)
	}
	desired = staged.Doc
	observed := assumedReadmissionStatuses(s.History[0], "")
	meta := observed[topology.MetaGroupID]
	meta.ConfState.Learners = []uint64{2}
	observed[topology.MetaGroupID] = meta
	plan, err := topology.BuildPlan(testPlanningInput(t, m, desired, observed), desired, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 2 || plan.Steps[0].Kind != topology.StepRemoveReplica || plan.Steps[1].Kind != topology.StepStopReplica || plan.Steps[0].NodeName != "n2" || plan.Steps[0].ReplicaID != 2 {
		t.Fatal("lost exact intermediate retirement", plan)
	}
	begin := applyMachine(t, m, BeginOperationProposal{Version: desired.Version, Plan: plan,
		Scopes: m.scoped.capturePlanScopes(topology.AffectedGroups(s.Target, desired)), At: s.Operations[0].UpdatedAt.Add(time.Second),
		Actor: "admin", Origin: topology.OriginAPI, ProposalID: "retire-member"})
	if !begin.Accepted {
		t.Fatal(begin)
	}
	ageScopedHistory(t, m.scoped)
	pruneScopedFixture(t, m.scoped)
	m = restoreMachineFixture(t, *m.scoped)
	if owner, established := m.MemberOwner(2); owner != "n2" || !established {
		t.Fatal("restore lost the prepared intermediate route", owner, established)
	}
	owners, _ := m.MemberOwners()
	delete(owners, 2)
	if owner, _ := m.MemberOwner(2); owner != "n2" {
		t.Fatal("routing read aliases retained proof")
	}
	op := m.scoped.indexes.operations[begin.OperationID].Clone()
	transition := scopedTransitionFor(op, topology.MetaGroupID, 1)
	if result := applyMachine(t, m, TransitionOperationProposal{Transition: transition, ProposalID: "removing"}); !result.Accepted {
		t.Fatal(result)
	}
	issued := applyMachine(t, m, BeginEffectProposal{Action: scopedPlanAction(&op, plan.Steps[0]), ProposalID: "remove-member"})
	if !issued.Accepted || issued.Grant == nil {
		t.Fatal(issued)
	}
	for _, receipt := range fixtureEffectReceipts(*issued.Grant) {
		result := applyMachine(t, m, RecordEffectProposal{Receipt: receipt, At: transition.At, ProposalID: "removed"})
		if !result.Accepted {
			t.Fatal(result)
		}
	}
	if owner, _ := m.MemberOwner(2); owner != "" {
		t.Fatal("consumed removal still accepts the retired member", owner)
	}
	m = restoreMachineFixture(t, *m.scoped)
	if owner, _ := m.MemberOwner(2); owner != "" {
		t.Fatal("restore resurrected the retired intermediate route", owner)
	}
}
