//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func readmissionDeletionFixture(t *testing.T) (scopedState, BeginOperationProposal) {
	t.Helper()
	s := scopedAdmissionFixture()
	s.Target.Nodes = s.Target.Nodes[:1]
	s.Target.MetaGroup().Replicas = []topology.Replica{{NodeName: "n1", ReplicaID: 2, Role: topology.ReplicaVoter}}
	s.Target.Group("shard0").Replicas[0].ReplicaID, s.Target.Group("shard0").NextReplicaID = 2, 3
	s.Nodes = s.Nodes[:1]
	s.Nodes[0].Owner, s.Groups[1].Owner = "exec-v2-a1", "exec-v2-a1"
	s.Allocations[1].NextReplicaID = 3
	op := &s.Operations[0]
	for i := range op.Plan.Steps {
		op.Plan.Steps[i].NodeName = "n1"
	}
	op.Plan.Steps = append(op.Plan.Steps,
		topology.Step{Seq: 3, GroupID: topology.MetaGroupID, Kind: topology.StepWaitCaughtUp, NodeName: "n1", ReplicaID: 2},
		topology.Step{Seq: 4, GroupID: topology.MetaGroupID, Kind: topology.StepPromoteVoter, NodeName: "n1", ReplicaID: 2,
			ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1, 2})},
		topology.Step{Seq: 5, GroupID: topology.MetaGroupID, Kind: topology.StepTransferLeader, NodeName: "n1", ReplicaID: 2},
		topology.Step{Seq: 6, GroupID: topology.MetaGroupID, Kind: topology.StepRemoveReplica, NodeName: "n1", ReplicaID: 1,
			ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1, 2}), ResultVotersDigest: topology.VoterSetDigest([]uint64{2})},
		topology.Step{Seq: 7, GroupID: "shard0", Kind: topology.StepPrepareReplica, NodeName: "n1", ReplicaID: 2})
	op.Plan.Blocks[0].Steps, op.Status.Blocks[0].TotalSteps = []int{1, 2, 3, 4, 5, 6}, 6
	op.Plan.Blocks = append(op.Plan.Blocks, topology.PlanBlock{GroupID: "shard0", SourceVersion: 1, Steps: []int{7}})
	op.Status.Blocks = append(op.Status.Blocks, topology.BlockStatus{GroupID: "shard0", State: topology.OperationPending, TotalSteps: 1})
	op.Status.TotalBlocks = 2
	s.Admissions[0].NodeName = "n1"
	for _, group := range []string{topology.MetaGroupID, "shard0"} {
		host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: "n1"}
		key := topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: group, Revision: 5}
		grant := topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityReadmit, AdmissionID: ReadmissionID(s.Admissions[0]),
			Resources: []topology.AuthorityResource{host}, Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
		s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution", authorityRecord: authorityRecord{Grant: grant}})
		s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: host, Key: key})
	}
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "shard0" })
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	p := BeginOperationProposal{Version: 3, At: op.UpdatedAt.Add(time.Second), Plan: topology.Plan{Version: 3,
		Blocks: []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 1}}},
		Scopes: []ScopeExpectation{{GroupID: "shard0", SourceVersion: 1, Owner: op.Status.ID}}}
	return s, p
}

func TestScopedDeletionCompletesOnlyWithWholeBatch(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		s, p := readmissionDeletionFixture(t)
		for i := range s.AuthorityRecords {
			r := &s.AuthorityRecords[i]
			if complete || r.Grant.Key.GroupID == "shard0" {
				r.Effects = fixtureEffectReceipts(r.Grant)
			}
		}
		before, _ := json.Marshal(s)
		next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
		if err != nil {
			t.Fatal(err)
		}
		row := next.Groups[1]
		if (row.SourceVersion == 3) != complete || row.Deleting == complete || (len(next.Cleanup) == 1) != complete {
			t.Fatal("partial retirement created completion or complete proof stayed blocked", row, next.Cleanup)
		}
		if complete {
			d := next.Cleanup[0]
			if d.OperationID != "exec-v2-a1" || d.MemberID != 1 || d.Identity.ReplicaID != 1 ||
				d.StopAction != nil || d.RetireAction != nil || d.Removed != nil || d.ReadmissionKey == nil ||
				d.Storage != s.History[0].Group("shard0").Storage || !reflect.DeepEqual(d.Stopped, d.Retired) {
				t.Fatal("completion lost original exact actionless retirement", d)
			}
		}
		next.History = append(slices.Clone(next.History), s.Target.Clone())
		if _, err := prepareScopedGraph(next, next.Rules); err != nil {
			t.Fatal("deletion cannot restore", err)
		}
		after, _ := json.Marshal(s)
		if !bytes.Equal(before, after) {
			t.Fatal("completion changed source proof")
		}
		if complete {
			next.Cleanup = nil
			if _, err := prepareScopedGraph(next, next.Rules); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("tombstone hid missing exact retirement debt", err)
			}
		}
	}
}

func TestScopedDeletionRejectsCancellationOfDependedOnBatch(t *testing.T) {
	t.Parallel()
	s, p := readmissionDeletionFixture(t)
	next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range []uint64{0, 2, 3} {
		candidate := *next
		candidate.Target = next.Target.Clone()
		candidate.Target.MetaGroup().Replicas[0].ReplicaID = member
		collector := s.planningCollector()
		if _, err := collector.collect(s.planningDeclarations(), s.Latest); err != nil {
			t.Fatal(err)
		}
		collector.markConvergedOwners(s.planningDeclarations().Groups)
		before, _ := json.Marshal(next)
		prep := deletionPreparation{&s, &candidate, collector}
		err := prep.protectReadmission()
		if member == 2 && err != nil || member != 2 && !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("pending retirement source was canceled or same-member handover refused", member, err)
		}
		for _, record := range collector.evidence.authority.records {
			record.Effects = fixtureEffectReceipts(record.Grant)
		}
		if err := prep.protectReadmission(); err != nil {
			t.Fatal("consumed proof prevented cancellation", err)
		}
		// Restore source receipts changed above so each case starts pending.
		for _, record := range collector.evidence.authority.records {
			record.Effects = nil
		}
		after, _ := json.Marshal(next)
		if !bytes.Equal(before, after) {
			t.Fatal("cancellation validation mutated the candidate")
		}
	}
}

func TestScopedDeletionReusesProofAndRefusesPartialCompletionAtomically(t *testing.T) {
	t.Parallel()
	s, proposal := readmissionDeletionFixture(t)
	for i := range s.AuthorityRecords {
		s.AuthorityRecords[i].Effects = fixtureEffectReceipts(s.AuthorityRecords[i].Grant)
	}
	next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	collector := s.planningCollector()
	if _, err := collector.collect(s.planningDeclarations(), s.Latest); err != nil {
		t.Fatal(err)
	}
	collector.markConvergedOwners(s.planningDeclarations().Groups)
	prep := deletionPreparation{&s, next, collector}
	before, _ := json.Marshal(next)
	if err := prep.complete("shard0", proposal.At.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(next)
	if !bytes.Equal(before, after) {
		t.Fatal("repeat completion duplicated or rewrote original debt")
	}
	next.Cleanup[0].Stopped, next.Cleanup[0].Retired, next.Cleanup[0].ReadmissionKey = nil, nil, nil
	next.Cleanup[0].Storage = topology.StorageMemory
	before, _ = json.Marshal(next)
	if err := prep.complete("shard0", proposal.At); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("completion changed original storage", err)
	}
	after, _ = json.Marshal(next)
	if !bytes.Equal(before, after) {
		t.Fatal("refused completion partially installed debt")
	}
	next.Cleanup = nil
	s.AuthorityRecords[0].Effects = nil // The load receipt alone cannot complete its batch.
	before, _ = json.Marshal(next)
	if err := prep.complete("shard0", proposal.At); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	after, _ = json.Marshal(next)
	if !bytes.Equal(before, after) {
		t.Fatal("partial batch installed completion debt")
	}
}

func TestScopedDeletionMatchesLaterBatchToFormerIdentity(t *testing.T) {
	t.Parallel()
	s, _ := readmissionDeletionFixture(t)
	collector := s.planningCollector()
	later := s.Admissions[0]
	later.MemberID, later.OperationID = 3, "exec-v4-a1"
	collector.evidence.admissions = append(slices.Clone(s.Admissions), later)
	for _, original := range s.AuthorityRecords {
		grant := original.Grant.Clone()
		grant.Key.Revision, grant.AdmissionID = 9, ReadmissionID(later)
		collector.evidence.authority.records[grant.Key] = &authorityRecord{Grant: grant,
			Effects: fixtureEffectReceipts(grant), Closed: true}
		collector.evidence.authority.floors[grant.Resources[0]] = grant.Key
	}
	target := topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}
	record, err := collector.deletionReadmission("shard0", target, s.Target)
	if err != nil || record.Grant.AdmissionID != ReadmissionID(later) || record.Effects[0].Target != target {
		t.Fatal("complete later-member batch lost exact former ownership", record, err)
	}
	old := collector.evidence.authority.records[s.AuthorityRecords[1].Grant.Key]
	if !collector.evidence.authority.preferReadmission(record, old) ||
		collector.evidence.authority.preferReadmission(old, record) {
		t.Fatal("older pending batch hid later complete proof")
	}
	target.MemberID = 2
	if _, err := collector.deletionReadmission("shard0", target, s.Target); err == nil {
		t.Fatal("former member was confused with its replacement")
	}
}

func TestScopedApprovalCannotCancelPendingDeletionRetirement(t *testing.T) {
	t.Parallel()
	s, p := readmissionDeletionFixture(t)
	pending, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	pending.History = append(slices.Clone(pending.History), s.Target.Clone())
	pending.Latest = pending.Target.Clone()
	pending.Latest.Version, pending.Latest.ParentVersion = 4, 3
	pending.Latest.MetaGroup().Replicas[0].ReplicaID, pending.Latest.MetaGroup().NextReplicaID = 3, 4
	pending.Allocations = slices.Clone(pending.Allocations)
	pending.Allocations[0].NextReplicaID = 4
	pending.indexes, err = prepareScopedGraph(pending, pending.Rules)
	if err != nil {
		t.Fatal(err)
	}
	plan := s.Operations[0].Plan.Clone()
	plan.Version, plan.Steps, plan.Blocks = 4, plan.Steps[:4], plan.Blocks[:1]
	for i := range plan.Steps {
		plan.Steps[i].ReplicaID = 3
	}
	plan.Steps[3].ResultVotersDigest = topology.VoterSetDigest([]uint64{1, 3})
	plan.Steps = append(plan.Steps,
		topology.Step{Seq: 5, GroupID: topology.MetaGroupID, Kind: topology.StepTransferLeader,
			NodeName: "n1", ReplicaID: 3},
		topology.Step{Seq: 6, GroupID: topology.MetaGroupID, Kind: topology.StepRemoveReplica, NodeName: "n1", ReplicaID: 1,
			ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1, 3}), ResultVotersDigest: topology.VoterSetDigest([]uint64{3})})
	plan.Blocks[0].Steps = []int{1, 2, 3, 4, 5, 6}
	proposal := BeginOperationProposal{Version: 4, At: p.At.Add(time.Second), Plan: plan,
		Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 1, Owner: "exec-v2-a1"}}}
	before, _ := json.Marshal(pending)
	_, err = prepareScopedApproval(pending, pending.Latest, proposal, explicitApproval)
	if !errors.Is(err, fault.ErrOperationAuthorization) || !strings.Contains(err.Error(), "requires pending readmission") {
		t.Fatal("actual approval bypassed dependent-batch protection", err)
	}
	after, _ := json.Marshal(pending)
	if !bytes.Equal(before, after) {
		t.Fatal("refused approval partially changed pending deletion")
	}
	for i := range pending.AuthorityRecords {
		pending.AuthorityRecords[i].Effects = fixtureEffectReceipts(pending.AuthorityRecords[i].Grant)
	}
	if _, err := prepareScopedApproval(pending, pending.Latest, proposal, explicitApproval); err != nil {
		t.Fatal("consumed batch still prevented replacement approval", err)
	}
}
