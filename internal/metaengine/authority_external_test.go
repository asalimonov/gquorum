//go:build !functional

package metaengine

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func convergedAuthorityFixture(t *testing.T) *Machine {
	t.Helper()
	return convergedAuthorityFixtureWithLearners(t)
}

func convergedAuthorityFixtureWithLearners(t *testing.T, learners ...string) *Machine {
	t.Helper()
	m := NewMachine(testRules(), nil)
	doc := baseDoc()
	for i := 2; i <= 3; i++ {
		name := fmt.Sprint("n", i)
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		for j := range doc.Groups {
			doc.Groups[j].Replicas = append(doc.Groups[j].Replicas, topology.Replica{NodeName: name, ReplicaID: uint64(i), Role: topology.ReplicaVoter})
			doc.Groups[j].NextReplicaID = 4
		}
	}
	for i := range doc.Groups {
		for j := range doc.Groups[i].Replicas {
			if slices.Contains(learners, doc.Groups[i].Replicas[j].NodeName) {
				doc.Groups[i].Replicas[j].Role = topology.ReplicaLearner
			}
		}
	}
	at := time.Unix(100, 0).UTC()
	proposals := []any{
		ProposeDocument{Doc: doc},
		BeginOperatorProposal{Version: 1, Plan: topology.Plan{Version: 1}, At: at, Actor: "n1"},
		TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: at.Add(time.Second)}},
		ActivateTargetProposal{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1},
		GrantSession{SessionID: "old-session", NodeName: "n1", InstanceID: "old", TTLSeconds: 30},
	}
	for i, p := range proposals {
		if r := apply(t, m.topologySM, uint64(i+1), p); !r.Accepted {
			t.Fatal(r)
		}
	}
	return m
}
func TestDirectTransferGrantIsOneShotAndApprovalRevokesIt(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	p := GrantTransferProposal{ExpectedActiveVersion: 1, GroupID: "shard0", NodeName: "n1", ReplicaID: 1, RequestID: "transfer"}
	first := apply(t, m.topologySM, 6, p)
	if !first.Accepted || first.Grant == nil || first.Grant.Purpose != topology.AuthorityTransfer {
		t.Fatal(first)
	}
	if retry := apply(t, m.topologySM, 7, p); !retry.Accepted || !retry.Grant.Equal(*first.Grant) {
		t.Fatal(retry)
	}
	bad := p
	bad.NodeName = "n2"
	bad.ReplicaID = 2
	if r := apply(t, m.topologySM, 8, bad); r.Accepted {
		t.Fatal("request identity changed target")
	}
	if r := apply(t, m.topologySM, 9, RecordEffectProposal{Receipt: groupEffectProof(*first.Grant, 10)}); !r.Accepted {
		t.Fatal(r)
	}
	p.RequestID = "second"
	second := apply(t, m.topologySM, 10, p)
	if !second.Accepted || second.Grant.Key == first.Grant.Key {
		t.Fatal(second)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal("two requests for the same target were conflated", err)
	}
	op := supersedeAuthority(t, m, 11)
	status, err := m.AuthorityStatus(op)
	if err != nil || status.RevocationRequested || len(status.Obligations) != 1 {
		t.Fatal("nonoperator prerequisite became operator revocation", status, err)
	}
	if r := apply(t, m.topologySM, 13, p); r.Accepted || r.Code != proposalAuthorityFenced {
		t.Fatal(r)
	}
	q, _ := json.Marshal(QueryRequest{Kind: "authority", OperatorID: op})
	result, err := m.Query(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(result.Data, &status); err != nil || status.RevocationRequested || len(status.Obligations) != 1 {
		t.Fatal(status, err)
	}
}
func TestApprovedReadmissionGetsDistinctHostAuthorityAndIsRevocable(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	at := time.Unix(200, 0).UTC()
	doc, err := buildReadmissionDocument(m.active, "n1", at)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := topology.BuildPlan(testPlanningInput(t, m, doc, assumedReadmissionStatuses(m.active, "n1")), doc, m.rules)
	if err != nil {
		t.Fatal(err)
	}
	admission := AdmissionRecord{NodeName: "n1", InstanceID: "replacement", MemberID: doc.MemberID("n1"), OperatorID: topology.MakeOperatorID(2, 1)}
	p := BeginReadmissionAuthority{PreviousMemberID: 1, NodeName: admission.NodeName, InstanceID: admission.InstanceID, MemberID: admission.MemberID, OperatorID: admission.OperatorID}
	if r := apply(t, m.topologySM, 6, p); r.Accepted {
		t.Fatal("unapproved readmission got authority")
	}
	approved := apply(t, m.topologySM, 7, CommitReadmissionProposal{Admission: admission, Doc: doc, ExpectedVersion: 1, Plan: plan, At: at})
	if !approved.Accepted {
		t.Fatal(approved)
	}
	r := apply(t, m.topologySM, 8, p)
	if !r.Accepted || len(r.Grants) != 2 {
		t.Fatal(r)
	}
	for _, g := range r.Grants {
		if g.Key.Revision != 8 || g.Purpose != topology.AuthorityReadmit || g.AdmissionID != ReadmissionID(admission) || len(g.Targets) != 1 || g.Targets[0].MemberID != 1 || g.Targets[0].ReplicaID != 1 {
			t.Fatal(g)
		}
	}
	if retry := apply(t, m.topologySM, 9, p); !retry.Accepted || len(retry.Grants) != 2 || !retry.Grants[0].Equal(r.Grants[0]) {
		t.Fatal(retry)
	}
	q, _ := json.Marshal(QueryRequest{Kind: "authority_floors", Key: "n1"})
	result, err := m.Query(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	var floors []topology.AuthorityGrant
	if err := json.Unmarshal(result.Data, &floors); err != nil || len(floors) != 2 || !floors[0].Equal(r.Grants[0]) {
		t.Fatal(floors, err)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if retry := apply(t, restored.topologySM, 10, p); !retry.Accepted || len(retry.Grants) != 2 {
		t.Fatal(retry)
	}
	if staged := apply(t, m.topologySM, 10, ProposeDocument{Doc: doc, ExpectedVersion: 2}); !staged.Accepted {
		t.Fatal(staged)
	}
	plan.Version = 3
	if replacement := apply(t, m.topologySM, 11, BeginOperatorProposal{Version: 3, Plan: plan, At: at.Add(time.Second), Actor: "n1"}); !replacement.Accepted {
		t.Fatal(replacement)
	}
	if retry := apply(t, m.topologySM, 12, p); retry.Accepted || retry.Code != proposalAuthorityFenced {
		t.Fatal(retry)
	}
	status, err := m.AuthorityStatus(admission.OperatorID)
	if err != nil || !status.RevocationRequested || status.RevocationComplete || len(status.Obligations) != 2 {
		t.Fatal(status, err)
	}
}

func TestReadmissionAuthorityWaitsForConflictingTransferRevocation(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	transfer := apply(t, m.topologySM, 6, GrantTransferProposal{ExpectedActiveVersion: 1, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1, RequestID: "meta-transfer"})
	if !transfer.Accepted {
		t.Fatal(transfer)
	}
	at := time.Unix(200, 0).UTC()
	doc, err := buildReadmissionDocument(m.active, "n1", at)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := topology.BuildPlan(testPlanningInput(t, m, doc, assumedReadmissionStatuses(m.active, "n1")), doc, m.rules)
	if err != nil {
		t.Fatal(err)
	}
	admission := AdmissionRecord{NodeName: "n1", InstanceID: "replacement", MemberID: doc.MemberID("n1"), OperatorID: topology.MakeOperatorID(2, 1)}
	if r := apply(t, m.topologySM, 7, CommitReadmissionProposal{Admission: admission, Doc: doc, ExpectedVersion: 1, Plan: plan, At: at}); !r.Accepted {
		t.Fatal(r)
	}
	p := BeginReadmissionAuthority{PreviousMemberID: 1, NodeName: admission.NodeName, InstanceID: admission.InstanceID, MemberID: admission.MemberID, OperatorID: admission.OperatorID}
	if r := apply(t, m.topologySM, 8, p); r.Accepted || r.Code != proposalAuthorityPending {
		t.Fatal(r)
	}
	pending := m.PendingFences()
	if len(pending) != 1 {
		t.Fatal(pending)
	}
	fence, _ := m.AuthorityGrant(pending[0].Key)
	if r := apply(t, m.topologySM, 9, RecordFenceProposal{Evidence: groupFenceProof(fence, 20)}); !r.Accepted {
		t.Fatal(r)
	}
	r := apply(t, m.topologySM, 10, p)
	if !r.Accepted || len(r.Grants) != 2 {
		t.Fatal(r)
	}
	for _, grant := range r.Grants {
		if grant.Key.Revision != 10 {
			t.Fatal("grant collided with approval fence revision", grant)
		}
	}
}

func TestDirectTransferRetryRecoversUnconsumedGrantAfterRestore(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	p := GrantTransferProposal{ExpectedActiveVersion: 1, GroupID: "shard0", NodeName: "n1", ReplicaID: 1, RequestID: "lost-caller"}
	first := apply(t, m.topologySM, 6, p)
	if !first.Accepted || first.Grant == nil {
		t.Fatal(first)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	p.RequestID = "retry-from-another-process"
	retry := apply(t, restored.topologySM, 7, p)
	if !retry.Accepted || retry.Grant == nil || !retry.Grant.Equal(*first.Grant) {
		t.Fatal("retry stranded the original grant", retry)
	}
	conflicting := p
	conflicting.RequestID, conflicting.NodeName, conflicting.ReplicaID = "different-target", "n2", 2
	if r := apply(t, restored.topologySM, 8, conflicting); r.Accepted || r.Code != proposalAuthorityPending {
		t.Fatal("different action reused pending authority", r)
	}
	if r := apply(t, restored.topologySM, 9, RecordEffectProposal{Receipt: groupEffectProof(*first.Grant, 10)}); !r.Accepted {
		t.Fatal(r)
	}
	// A completed command is not silently made repeatable. A later call needs a
	// new grant/floor, while retries by the original ID retain their old receipt.
	next := apply(t, restored.topologySM, 10, p)
	if !next.Accepted || next.Grant == nil || next.Grant.Key == first.Grant.Key || next.Grant.RequestID != p.RequestID {
		t.Fatal(next)
	}
	p.RequestID = first.Grant.RequestID
	old := apply(t, restored.topologySM, 11, p)
	if !old.Accepted || !old.Grant.Equal(*first.Grant) {
		t.Fatal("original request lost identity", old)
	}
}
