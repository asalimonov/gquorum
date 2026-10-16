//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func convergedWithDetachedRetirement(t *testing.T) scopedState {
	t.Helper()
	s := scopedCodecFixture()
	for i := range s.Groups {
		row := &s.Groups[i]
		if row.GroupID == "a" {
			row.SourceVersion, row.Declaration = s.Target.Version, s.Target.Group("a")
		}
		if row.GroupID == "b" {
			row.SourceVersion, row.Declaration, row.Deleting = 2, nil, false
		}
	}
	for i := range s.Cleanup {
		s.Cleanup[i].Retired, s.Cleanup[i].RetiredAt, s.Cleanup[i].Collection = nil, nil, nil
	}
	for i := range s.AuthorityRecords {
		if s.AuthorityRecords[i].Class == "cleanup" {
			s.AuthorityRecords[i].Effects = nil
		}
	}
	prepareScopedFixture(t, &s)
	return s
}

func TestScopedMarkerAdvancesWithDetachedDebtAndPinsItsApprovedDocument(t *testing.T) {
	t.Parallel()
	s := convergedWithDetachedRetirement(t)
	at := s.Operations[0].UpdatedAt.Add(time.Second)
	if err := s.advanceNodeProgressLocked(at); err != nil {
		t.Fatal(err)
	}
	s.updateFullyConvergedLocked(at)
	if s.FullyConverged.Version != 5 || s.Latest.Version != 6 || s.Cleanup[0].Retired != nil ||
		s.Cleanup[1].Retired != nil {
		t.Fatal("staged intent or physical retirement changed the completion boundary", s.FullyConverged)
	}
	prepareScopedFixture(t, &s)
	marked := s.Target.Clone()
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 7, 5
	s.Latest.MetaGroup().NextReplicaID = 4
	s.Latest.Node("n").Labels = map[string]string{"owner": "new"}
	prepareScopedFixture(t, &s)
	proposal := BeginOperationProposal{Version: 7, At: at.Add(time.Second), Plan: topology.Plan{Version: 7,
		Steps: []topology.Step{{Seq: 1, GroupID: topology.MetaGroupID, Kind: topology.StepTransferLeader,
			NodeName: "n", ReplicaID: 1}},
		Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 1, Steps: []int{1}}}},
		Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 1}}}
	applyScopedApprovalFixture(t, &s, proposal)
	if s.FullyConverged.Version != 5 || !reflect.DeepEqual(marked, s.indexes.documents[5]) {
		t.Fatal("new pending Target lost or rewrote the historical marker document")
	}
	s.Target.Node("n").Labels["owner"] = "candidate-only"
	retained := s.indexes.documents[5]
	if retained.Node("n").Labels["owner"] != "" {
		t.Fatal("historical marker declaration aliases current Target")
	}
}

func TestScopedNodeProgressKeepsUnknownAdmissionAndOtherScopeProof(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	at := s.Operations[0].UpdatedAt.Add(time.Second)
	before := s.Nodes[0]
	if err := s.advanceNodeProgressLocked(at); err != nil {
		t.Fatal(err)
	}
	s.updateFullyConvergedLocked(at)
	if s.Nodes[1].SourceVersion != 0 || s.Nodes[1].Declaration != nil || s.Nodes[1].MemberID != 0 ||
		!reflect.DeepEqual(before, s.Nodes[0]) || s.FullyConverged.Version != 1 {
		t.Fatal("unknown admission manufactured proof or changed an independent checkpoint", s.Nodes)
	}
	encoded, _ := json.Marshal(s)
	if err := s.advanceNodeProgressLocked(time.Time{}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("missing completion time accepted", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(encoded, after) {
		t.Fatal("refused node update changed state")
	}
}

func TestScopedNodeProgressWaitsForExactDependentDeletion(t *testing.T) {
	t.Parallel()
	s, proposal := readmissionDeletionFixture(t)
	applyScopedApprovalFixture(t, &s, proposal)
	// Isolate the dependent-node update after metadata convergence. The full
	// atomic operation/admission installation is exercised with Task 42.
	s.Groups[0] = groupConvergence{GroupID: topology.MetaGroupID, SourceVersion: 2,
		Declaration: s.Target.MetaGroup(), CompletedAt: proposal.At}
	old := s.Nodes[0]
	if err := s.advanceNodeProgressLocked(proposal.At); err != nil {
		t.Fatal(err)
	}
	if s.Nodes[0].MemberID != old.MemberID || s.Nodes[0].SourceVersion != old.SourceVersion {
		t.Fatal("metadata convergence waived the old load owner's required stop")
	}
	for i := range s.AuthorityRecords {
		s.AuthorityRecords[i].Effects = fixtureEffectReceipts(s.AuthorityRecords[i].Grant)
	}
	var err error
	s.indexes, err = s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	op := s.Operations[len(s.Operations)-1]
	completion := scopedTransitionFor(op, "shard0", 0)
	completion.State = topology.OperationSucceeded
	next, err := s.prepareGroupCompletion(completion)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.advanceNodeProgressLocked(completion.At); err != nil {
		t.Fatal(err)
	}
	if next.Nodes[0].MemberID != 2 || next.Nodes[0].SourceVersion != 3 ||
		next.Cleanup[0].OperationID != "exec-v2-a1" || len(next.RetiredGroups) != 1 {
		t.Fatal("complete exact retirement did not release the node prerequisite", next.Nodes, next.Cleanup)
	}
	next.updateFullyConvergedLocked(completion.At)
	if next.FullyConverged.Version != 1 {
		t.Fatal("group preparation advanced the marker before required block publication")
	}
}

func TestScopedMarkerClockAndSkippedVersions(t *testing.T) {
	t.Parallel()
	s, proposal := approvalFixture(t, "shard0")
	old := s.FullyConverged
	applyScopedApprovalFixture(t, &s, proposal)
	if old.Version != 1 || s.FullyConverged.Version != 3 {
		t.Fatal("superseded version was required to receive a marker")
	}
	marker := s.FullyConverged
	s.updateFullyConvergedLocked(proposal.At.Add(time.Hour))
	if s.FullyConverged != marker {
		t.Fatal("repeated marker evaluation rewrote historical completion time")
	}
	s = convergedWithDetachedRetirement(t)
	at := s.Operations[0].UpdatedAt
	s.Groups[0].CompletedAt = at.Add(time.Hour)
	s.updateFullyConvergedLocked(at)
	if s.FullyConverged.Version != 5 || !s.FullyConverged.At.Equal(s.Groups[0].CompletedAt) {
		t.Fatal("marker predates one of the checkpoints it proves", s.FullyConverged)
	}
	prepareScopedFixture(t, &s)
}

func TestScopedNodeDrainKeepsDependenciesAcrossUnrelatedMetadataApproval(t *testing.T) {
	t.Parallel()
	s, repair := repairCommitFixture(t)
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 2, 1
	s.Latest.Node("n2").State = topology.NodeDraining
	s.Latest.Group("b").Replicas = slices.DeleteFunc(s.Latest.Group("b").Replicas,
		func(r topology.Replica) bool { return r.NodeName == "n2" })
	s.Intents = nil
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = s.Latest.Group(s.Allocations[i].GroupID).NextReplicaID
	}
	prepareScopedFixture(t, &s)
	plan := repairFixturePlan(t, &s, s.Latest)
	for i := range plan.Blocks {
		block := &plan.Blocks[i]
		for _, seq := range block.Steps {
			if plan.Steps[seq-1].Kind == topology.StepStopReplica {
				block.Cleanup = append(block.Cleanup, seq)
			}
		}
		block.Steps = slices.DeleteFunc(block.Steps, func(seq int) bool {
			return plan.Steps[seq-1].Kind == topology.StepStopReplica
		})
		slices.Sort(block.Cleanup)
	}
	p := BeginOperationProposal{Version: 2, Plan: plan, Scopes: repair.Scopes, At: repair.At,
		Actor: "drainer", Origin: topology.OriginOperator}
	applyScopedApprovalFixture(t, &s, p)
	old := *s.indexes.nodes["n2"]
	if old.SourceVersion != 1 || old.Declaration.State != topology.NodeActive || s.indexes.groups["b"].Owner != "exec-v2-a1" {
		t.Fatal("drain completed before its load evacuation")
	}
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	s.Latest.Node("n3").Region = "unrelated"
	prepareScopedFixture(t, &s)
	p = BeginOperationProposal{Version: 3, At: p.At.Add(time.Second), Actor: "other", Origin: topology.OriginOperator,
		Plan:   topology.Plan{Version: 3, Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 2}}},
		Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 2}}}
	applyScopedApprovalFixture(t, &s, p)
	if row := s.indexes.nodes["n2"]; row.SourceVersion != 1 || row.Declaration.State != topology.NodeActive ||
		s.indexes.groups["b"].Owner != "exec-v2-a1" {
		t.Fatal("unrelated metadata approval waived original drain dependency", row)
	}
	// The predicate can release X only after the exact dependent checkpoint.
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	group := index.groups["b"]
	group.Owner, group.SourceVersion, group.Declaration = "", 2, s.Target.Group("b")
	if _, ready := nodeApprovalProof(*s.indexes.nodes["n2"], &s, index); !ready {
		t.Fatal("complete evacuation still held the node")
	}
}
