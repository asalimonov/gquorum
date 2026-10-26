//go:build !functional

package metaengine

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedReceiptsRejectForeignAndRegressingBoundaries(t *testing.T) {
	t.Parallel()
	s, approval := approvalFixture(t, "shard0")
	key := scopedPendingTransfer(&s)
	m := restoreMachineFixture(t, s)
	grant := m.scoped.indexes.grants[key].Grant.Clone()
	proof := groupFenceProof(grant, 10)
	if r := applyMachine(t, m, RecordFenceProposal{Evidence: proof, ProposalID: "fence"}); !r.Accepted {
		t.Fatal(r)
	}
	if r := applyMachine(t, m, RecordEffectProposal{Receipt: groupEffectProof(grant, 9), At: approval.At, ProposalID: "receipt"}); r.Accepted {
		t.Fatal("pre-install effect accepted")
	}
	proof.Receiver.MemberID = 99
	if r := applyMachine(t, m, RecordFenceProposal{Evidence: proof, ProposalID: "fence"}); r.Accepted {
		t.Fatal("foreign receiver accepted")
	}
	if r := applyMachine(t, m, approval); !r.Accepted {
		t.Fatal(r)
	}
	var fence topology.AuthorityGrant
	for _, record := range m.scoped.AuthorityRecords {
		if record.Grant.Key.GroupID == "shard0" && record.Grant.Purpose == topology.AuthorityFence {
			fence = record.Grant.Clone()
		}
	}
	if fence.Key.Revision <= grant.Key.Revision {
		t.Fatal("missing successor fence")
	}
	if r := applyMachine(t, m, RecordFenceProposal{Evidence: groupFenceProof(fence, 9), ProposalID: "fence"}); r.Accepted {
		t.Fatal("regressing fence accepted")
	}
	if r := applyMachine(t, m, RecordFenceProposal{Evidence: groupFenceProof(fence, 20), ProposalID: "fence"}); !r.Accepted {
		t.Fatal(r)
	}
	if r := applyMachine(t, m, RecordEffectProposal{Receipt: groupEffectProof(grant, 21), At: approval.At, ProposalID: "receipt"}); r.Accepted {
		t.Fatal("post-fence effect accepted")
	}
}

func TestScopedDirectTransferRestoredRetryAndConflictingApproval(t *testing.T) {
	t.Parallel()
	s, approval := approvalFixture(t, topology.MetaGroupID)
	m := restoreMachineFixture(t, s)
	request := GrantTransferProposal{ExpectedSourceVersion: 2, GroupID: topology.MetaGroupID,
		NodeName: "n1", ReplicaID: 1, RequestID: "same-transfer", ProposalID: "transfer"}
	issued := applyMachine(t, m, request)
	if !issued.Accepted || issued.Grant == nil {
		t.Fatal(issued)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := mustMachine(t, s.Rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	repeated := applyMachine(t, restored, request)
	if !repeated.Accepted || !reflect.DeepEqual(repeated.Grant, issued.Grant) {
		t.Fatal("restored retry minted transfer authority", repeated)
	}
	// The new approval touches the same native resource, unlike a disjoint
	// declaration or host-only cleanup operation.
	approval.Plan.Steps = []topology.Step{{Seq: 1, GroupID: topology.MetaGroupID, Kind: topology.StepTransferLeader, NodeName: "n1", ReplicaID: 1}}
	approval.Plan.Blocks[0].Steps = []int{1}
	result := applyMachine(t, restored, approval)
	if !result.Accepted {
		t.Fatal(result)
	}
	old := restored.scoped.indexes.grants[issued.Grant.Key]
	resource := issued.Grant.Resources[0]
	if old == nil || !old.Closed || len(restored.scoped.indexes.obligations[resource]) == 0 {
		t.Fatal("conflicting approval left old transfer authorized")
	}
	// Immutable proof remains verifiable; the newer resource floor determines
	// whether a receiver may execute it.
	if err := restored.ValidateGrant(t.Context(), *issued.Grant); err != nil {
		t.Fatal(err)
	}
	if floor := restored.scoped.authorityCatalog().floors[resource]; floor.Revision <= issued.Grant.Key.Revision {
		t.Fatal("supersession failed to advance the conflicting resource floor")
	}
}

func TestScopedReplacementPreparationRequiresWholeReadmissionBatch(t *testing.T) {
	t.Parallel()
	s, _ := readmissionDeletionFixture(t)
	prepareScopedFixture(t, &s)
	op := s.Operations[0].Clone()
	action := scopedPlanAction(&op, op.Plan.Steps[0])
	m := restoreMachineFixture(t, s)
	if r := applyMachine(t, m, TransitionOperationProposal{Transition: scopedTransitionFor(op, topology.MetaGroupID, 1), ProposalID: "run"}); !r.Accepted {
		t.Fatal(r)
	}
	pending := func() {
		t.Helper()
		r := applyMachine(t, m, BeginEffectProposal{Action: action, ProposalID: "prepare"})
		if r.Accepted || r.Grant != nil || r.Code != proposalErrorCode(fault.Unavailable) {
			t.Fatal("replacement prepared before whole retirement batch", r)
		}
	}
	pending()
	grants := make([]topology.AuthorityGrant, len(s.AuthorityRecords))
	for i, record := range s.AuthorityRecords {
		grants[i] = record.Grant.Clone()
		grant := grants[i]
		fence := topology.FenceEvidence{Key: grant.Key, Resource: grant.Resources[0],
			Receiver: topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: "n1", MemberID: 1, InstanceID: "process"}, LocalSequence: grant.Key.Revision*10 + 1}
		if r := applyMachine(t, m, RecordFenceProposal{Evidence: fence, ProposalID: "fence"}); !r.Accepted {
			t.Fatal(r)
		}
	}
	pending() // A fence stops stale actions; it does not prove retirement.
	for i, grant := range grants {
		for _, receipt := range fixtureEffectReceipts(grant) {
			if r := applyMachine(t, m, RecordEffectProposal{Receipt: receipt, At: op.UpdatedAt.Add(time.Second), ProposalID: "receipt"}); !r.Accepted {
				t.Fatal(r)
			}
		}
		if i != len(grants)-1 {
			pending()
		}
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if r := applyMachine(t, m, BeginEffectProposal{Action: action, ProposalID: "prepare"}); !r.Accepted || r.Grant == nil {
		t.Fatal("complete restored retirement did not admit replacement", r)
	}
}

func TestScopedLateReadmissionReceiptsSettleWithoutResurrectingAdmission(t *testing.T) {
	t.Parallel()
	original, _ := readmissionDeletionFixture(t)
	s := threeMemberReadmissionFixture(t, false)
	s.AuthorityRecords, s.AuthorityFloors = original.AuthorityRecords, original.AuthorityFloors
	prepareScopedFixture(t, &s)
	if err := s.revokeAuthorityLocked(11, []blockRef{{"exec-v2-a1", topology.MetaGroupID}}, "exec-v3-a1", DeletionSemanticsLegacy); err != nil {
		t.Fatal(err)
	}
	s.AppliedIndex = 11
	m := restoreMachineFixture(t, s)
	admission := original.Admissions[0]
	var receipts []topology.EffectReceipt
	for _, record := range original.AuthorityRecords {
		receipts = append(receipts, fixtureEffectReceipts(record.Grant)...)
	}
	result := applyMachine(t, m, RecordReadmissionReceiptsProposal{Admission: admission, Receipts: receipts, At: s.Operations[1].UpdatedAt.Add(time.Second), ProposalID: "late-receipts"})
	if !result.Accepted {
		t.Fatal(result)
	}
	for _, record := range original.AuthorityRecords {
		if len(m.scoped.indexes.obligations[record.Grant.Resources[0]]) != 0 {
			t.Fatal("late exact retirement left revocation unsettled")
		}
	}
	result = applyMachine(t, m, ReadyAdmissionProposal{NodeName: admission.NodeName, InstanceID: admission.InstanceID, MemberID: admission.MemberID, OperationID: admission.OperationID, ProposalID: "stale-ready"})
	if result.Accepted {
		t.Fatal("superseded admission became ready")
	}
}

func TestScopedRepeatedSupersessionKeepsTransferFenceUntilInstalled(t *testing.T) {
	t.Parallel()
	for _, started := range []bool{false, true} {
		t.Run(fmt.Sprint("transfer-started-", started), func(t *testing.T) {
			t.Parallel()
			s, approval := approvalFixture(t, "shard0")
			key := scopedPendingTransfer(&s)
			m := restoreMachineFixture(t, s)
			grant, _ := m.AuthorityGrant(key)
			if started {
				result := applyMachine(t, m, RecordEffectProposal{Receipt: groupEffectProof(grant, 10),
					At: approval.At, ProposalID: "transfer-started"})
				if !result.Accepted {
					t.Fatal(result)
				}
			}
			if result := applyMachine(t, m, approval); !result.Accepted {
				t.Fatal(result)
			}
			before, err := queryScopedAuthority(m.scoped, QueryRequest{Kind: "authority", OperationID: grant.OperationID})
			if err != nil || !before.RevocationRequested || before.RevocationComplete || len(before.Obligations) != 1 {
				t.Fatal("pending transfer lost its native fence", before, err)
			}
			doc := m.Latest()
			doc.Group("shard0").Labels["owner"] = "fourth"
			stage := applyMachine(t, m, ProposeDocument{Doc: doc, ExpectedVersion: doc.Version, ProposalID: "fourth"})
			if !stage.Accepted {
				t.Fatal(stage)
			}
			approval.Version, approval.At = stage.Doc.Version, approval.At.Add(time.Second)
			approval.Plan = topology.Plan{Version: approval.Version, Blocks: []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 3}}}
			approval.Scopes = []ScopeExpectation{{GroupID: "shard0", SourceVersion: 3}}
			approval.ProposalID = "approve-fourth"
			if result := applyMachine(t, m, approval); !result.Accepted {
				t.Fatal(result)
			}
			m = restoreMachineFixture(t, *m.scoped)
			after, err := queryScopedAuthority(m.scoped, QueryRequest{Kind: "authority", OperationID: grant.OperationID})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("second approval or restore lost original pending fence", before, after, err)
			}
			fence, found := m.AuthorityGrant(after.Obligations[0].Key)
			if !found {
				t.Fatal("lost fence grant")
			}
			if result := applyMachine(t, m, RecordFenceProposal{Evidence: groupFenceProof(fence, 20), ProposalID: "settle"}); !result.Accepted {
				t.Fatal(result)
			}
			after, err = queryScopedAuthority(m.scoped, QueryRequest{Kind: "authority", OperationID: grant.OperationID})
			if err != nil || !after.RevocationComplete {
				t.Fatal(after, err)
			}
		})
	}
}

func TestScopedGrantTargetsExcludeStagedOnlyMembers(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	m := restoreMachineFixture(t, s)
	doc := m.Latest()
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	for i := range doc.Groups {
		doc.Groups[i].Replicas = append(doc.Groups[i].Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
		doc.Groups[i].NextReplicaID = 3
	}
	if result := applyMachine(t, m, ProposeDocument{Doc: doc, ExpectedVersion: doc.Version, ProposalID: "unapproved-member"}); !result.Accepted {
		t.Fatal(result)
	}
	action := scopedPlanAction(&s.Operations[1], s.Operations[1].Plan.Steps[1])
	if result := applyMachine(t, m, TransitionOperationProposal{Transition: topology.OperationTransition{
		OperationID: action.OperationID, Version: action.Version, Attempt: action.Attempt,
		GroupID: action.GroupID, StepSeq: action.StepSeq, State: topology.OperationRunning,
		At: s.Operations[1].UpdatedAt.Add(time.Second)}, ProposalID: "start-transfer"}); !result.Accepted {
		t.Fatal(result)
	}
	issued := applyMachine(t, m, BeginEffectProposal{Action: action, ProposalID: "existing-transfer"})
	if !issued.Accepted || issued.Grant == nil {
		t.Fatal(issued)
	}
	want := []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}
	if !reflect.DeepEqual(issued.Grant.Targets, want) {
		t.Fatal("unapproved receiver acquired authority", issued.Grant.Targets)
	}
}
