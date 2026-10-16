//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestScopedLoadScopeFoundingUsesPinnedOwnerAndExactHostReceipt(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"issued", "host", "native_only"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s, p := foundingApprovalFixture(t)
			seed := raftpb.ConfState{Voters: []uint64{1}}
			if _, err := foundingCommand(t, &s, p, seed); err != nil {
				t.Fatal(err)
			}
			op := s.Operations[0].Clone()
			applyScopedTransitionFixture(t, &s, scopedTransitionFor(op, "shard0", 1))
			issued, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: scopedPlanAction(&op, op.Plan.Steps[0])})
			if err != nil {
				t.Fatal(err)
			}
			for _, receipt := range fixtureEffectReceipts(*issued.result.Grant) {
				if phase == "host" && receipt.Resource.Kind == topology.AuthorityHost || phase == "native_only" && receipt.Resource.Kind == topology.AuthorityGroup {
					recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: p.At.Add(time.Second)})
				}
			}
			doc := s.Target.Clone()
			doc.Comment, doc.ParentVersion = "unrelated approval", 1
			if _, err := foundingCommand(t, &s, ProposeDocument{Doc: doc, ExpectedVersion: 1}, seed); err != nil {
				t.Fatal(err)
			}
			if _, err := foundingCommand(t, &s, BeginOperationProposal{Version: 2, Plan: topology.Plan{Version: 2}, Scopes: []ScopeExpectation{}, At: p.At.Add(2 * time.Second), Actor: "admin", Origin: topology.OriginOperator, ProposalID: "independent"}, seed); err != nil {
				t.Fatal(err)
			}
			id := storage.Identity{ClusterID: s.Target.ClusterID, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
			scope, err := queryScopedLoadScope(&s, id, 1)
			if err != nil || scope.Version != 2 || !scope.Founding || scope.Started != (phase == "host") || scope.Established != (phase == "host") {
				t.Fatal(scope, err)
			}
			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			var restored scopedState
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			again, err := queryScopedLoadScope(&restored, id, 1)
			if err != nil || !reflect.DeepEqual(scope, again) {
				t.Fatal("restore changed boot evidence", again, err)
			}
		})
	}
}

// n2 already belongs to proven metadata; only its load replica is new. No
// admission/node owner can accidentally pin the failed load operation's history.
func loadMembershipScopeFixture(t *testing.T, kind topology.StepKind) scopedState {
	t.Helper()
	s := scopedAdmissionFixture()
	base := s.Target.Clone()
	base.Version, base.ParentVersion = 1, 0
	desired := base.Clone()
	desired.Version, desired.ParentVersion = 2, 1
	role := topology.ReplicaLearner
	steps := []topology.Step{{Seq: 1, GroupID: "shard0", Kind: topology.StepPrepareReplica, NodeName: "n2", ReplicaID: 2},
		{Seq: 2, GroupID: "shard0", Kind: topology.StepAddLearner, NodeName: "n2", ReplicaID: 2, ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1})}}
	if kind == topology.StepPromoteVoter {
		role = topology.ReplicaVoter
		steps = append(steps, topology.Step{Seq: 3, GroupID: "shard0", Kind: topology.StepWaitCaughtUp, NodeName: "n2", ReplicaID: 2},
			topology.Step{Seq: 4, GroupID: "shard0", Kind: topology.StepPromoteVoter, NodeName: "n2", ReplicaID: 2, ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1, 2})})
	}
	desired.Group("shard0").Replicas = append(desired.Group("shard0").Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: role})
	desired.Group("shard0").NextReplicaID = 3
	op := s.Operations[0].Clone()
	op.Plan = topology.Plan{Version: 2, Steps: steps, VoterBases: []topology.PlanVoterBase{{GroupID: "shard0", Voters: []uint64{1}}}, Blocks: []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 1}}}
	for _, step := range steps {
		op.Plan.Blocks[0].Steps = append(op.Plan.Blocks[0].Steps, step.Seq)
	}
	op.Status.State, op.Status.TotalBlocks = topology.OperationRunning, 1
	op.Status.Blocks = []topology.BlockStatus{{GroupID: "shard0", State: topology.OperationRunning, CurrentStep: 1, TotalSteps: len(steps)}}
	s.Operations, s.Target, s.Latest, s.History = []topology.Operation{op}, desired, desired.Clone(), []topology.Document{base}
	s.Groups = []groupConvergence{{GroupID: topology.MetaGroupID, Declaration: base.MetaGroup(), SourceVersion: 1, CompletedAt: op.StartedAt}, {GroupID: "shard0", Declaration: base.Group("shard0"), SourceVersion: 1, CompletedAt: op.StartedAt, Owner: op.Status.ID}}
	s.Nodes = nil
	for _, node := range base.Nodes {
		s.Nodes = append(s.Nodes, nodeConvergence{NodeName: node.Name, Declaration: base.Node(node.Name), SourceVersion: 1, MemberID: base.MemberID(node.Name), CompletedAt: op.StartedAt})
	}
	s.Admissions = nil
	s.Allocations[1].NextReplicaID = 3
	prepareScopedFixture(t, &s)
	return s
}

func TestScopedLoadScopeRetainsNativeEstablishmentAcrossPruneAndRestore(t *testing.T) {
	t.Parallel()
	for _, kind := range []topology.StepKind{topology.StepAddLearner, topology.StepPromoteVoter} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			s := loadMembershipScopeFixture(t, kind)
			id := storage.Identity{ClusterID: s.Target.ClusterID, GroupID: "shard0", NodeName: "n2", ReplicaID: 2}
			var native topology.AuthorityKey
			for _, step := range s.Operations[0].Plan.Steps {
				op := s.Operations[0].Clone()
				if step.Seq != 1 {
					applyScopedTransitionFixture(t, &s, scopedTransitionFor(op, "shard0", step.Seq))
				}
				if step.Kind == topology.StepWaitCaughtUp {
					continue
				}
				issued, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: scopedPlanAction(&op, step)})
				if err != nil {
					t.Fatal(err)
				}
				before, err := queryScopedLoadScope(&s, id, 2)
				if err != nil {
					t.Fatal(err)
				}
				if step.Kind == topology.StepPrepareReplica && (before.Started || before.Established) {
					t.Fatal("issuance became creation", before)
				}
				for _, receipt := range fixtureEffectReceipts(*issued.result.Grant) {
					if receipt.Resource.Kind == topology.AuthorityGroup {
						native = receipt.Key
					}
					recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: receipt, At: op.StartedAt.Add(time.Second)})
				}
				scope, err := queryScopedLoadScope(&s, id, 2)
				if err != nil || !scope.Started || scope.Founding || scope.Established != (step.Kind != topology.StepPrepareReplica) {
					t.Fatal(scope, err)
				}
			}
			failed := scopedTransitionFor(s.Operations[0], "shard0", len(s.Operations[0].Plan.Steps))
			failed.State, failed.ErrorCode, failed.Error = topology.OperationFailed, "unavailable", "failed before convergence"
			applyScopedTransitionFixture(t, &s, failed)
			ageScopedHistory(t, &s)
			pruneScopedFixture(t, &s)
			if len(s.Operations) != 0 || s.indexes.grants[native] == nil {
				t.Fatal("membership fact lost or masked by pinned operation", s.Operations, s.AuthorityRecords)
			}
			for i := range s.AuthorityRecords {
				s.AuthorityRecords[i].Closed = true
			}
			prepareScopedFixture(t, &s)
			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			var restored scopedState
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			scope, err := queryScopedLoadScope(&restored, id, 2)
			if err != nil || !scope.Established || !scope.Started {
				t.Fatal("archived native receipt lost establishment", scope, err)
			}
			// Convergence replaces the receipt dependency, making it prunable.
			s.Groups[1].Declaration, s.Groups[1].SourceVersion = s.Target.Group("shard0"), s.Target.Version
			prepareScopedFixture(t, &s)
			pruneScopedFixture(t, &s)
			if s.indexes.grants[native] != nil {
				t.Fatal("proven membership retained obsolete native receipt")
			}
		})
	}
}

func TestScopedLoadScopeRequiresExactApprovedIdentityAndOwnsDeclaration(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	prepareScopedFixture(t, &s)
	id := storage.Identity{ClusterID: s.Target.ClusterID, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
	scope, err := queryScopedLoadScope(&s, id, 1)
	if err != nil || !scope.Started || !scope.Established || scope.Founding {
		t.Fatal(scope, err)
	}
	scope.Group.Replicas[0].ReplicaID = 99
	if s.Target.Group(id.GroupID).Replicas[0].ReplicaID != 1 {
		t.Fatal("scope aliases approval")
	}
	if _, err := queryScopedLoadScope(&s, id, 9); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	id.ReplicaID = 2
	if _, err := queryScopedLoadScope(&s, id, 1); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	s.RetiredGroups = append(s.RetiredGroups, retiredGroup{ID: id.GroupID, Version: 2})
	if _, err := queryScopedLoadScope(&s, id, 1); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal(err)
	}
	called := false
	if err := NewMachine(testRules(), nil).WithLoadScope(id, 1, func(LoadScope) error { called = true; return nil }); !errors.Is(err, fault.ErrUnsupported) || called {
		t.Fatal(err, called)
	}
}
