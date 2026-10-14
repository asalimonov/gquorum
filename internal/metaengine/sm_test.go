//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func TestCodec_Roundtrip(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	plan := testPlan(5, 1)
	operatorID := topology.MakeOperatorID(5, 1)
	admission := AdmissionRecord{NodeName: "n2", InstanceID: "instance-n2", MemberID: 2, OperatorID: operatorID, State: AdmissionPending}
	cases := []struct {
		name string
		want any
		kind ProposalKind
	}{
		{"propose_document", ProposeDocument{Doc: baseDoc(), ExpectedVersion: 3, ProposalID: "p1"}, KindProposeDocument},
		{"admit_node", AdmitNode{Node: topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive}, MemberID: 2, ProposalID: "p2"}, KindAdmitNode},
		{"begin_execution", BeginOperatorProposal{Version: 5, Plan: plan, Actor: "n1", At: at, ProposalID: "p3"}, KindBeginOperator},
		{"commit_repair_execution", CommitRepairOperatorProposal{Doc: baseDoc(), ExpectedVersion: 4, Plan: plan, At: at, ProposalID: "p4"}, KindCommitRepairOperator},
		{"transition_execution", TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: operatorID, Version: 5, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning, GroupID: "shard0", NodeName: "n1", At: at}, ProposalID: "p5"}, KindTransitionOperator},
		{"activate_target", ActivateTargetProposal{OperatorID: operatorID, Version: 5, Attempt: 1, ProposalID: "p6"}, KindActivateTarget},
		{"bind_admission", BindAdmissionProposal{Admission: admission, ReplaceInstance: true, At: at, ProposalID: "p7"}, KindBindAdmission},
		{"commit_readmission", CommitReadmissionProposal{Admission: admission, Doc: baseDoc(), ExpectedVersion: 4, Plan: plan, At: at, ProposalID: "p8"}, KindCommitReadmission},
		{"ready_admission", ReadyAdmissionProposal{NodeName: "n2", InstanceID: "instance-n2", MemberID: 2, OperatorID: operatorID, ProposalID: "p9"}, KindReadyAdmission},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := mustEncode(t, tc.want)
			decoded, err := decodeEnvelope(data)
			kind, body := decoded.Kind, decoded.Body
			if err != nil {
				t.Fatalf("decodeEnvelope: %v", err)
			}
			if kind != tc.kind {
				t.Fatalf("kind = %q, want %q", kind, tc.kind)
			}
			var got any
			switch tc.kind {
			case KindProposeDocument:
				var p ProposeDocument
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindAdmitNode:
				var p AdmitNode
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindBeginOperator:
				var p BeginOperatorProposal
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindCommitRepairOperator:
				var p CommitRepairOperatorProposal
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindTransitionOperator:
				var p TransitionOperatorProposal
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindActivateTarget:
				var p ActivateTargetProposal
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindBindAdmission:
				var p BindAdmissionProposal
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindCommitReadmission:
				var p CommitReadmissionProposal
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			case KindReadyAdmission:
				var p ReadyAdmissionProposal
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				got = p
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("roundtrip mismatch:\n got  %+v\n want %+v", got, tc.want)
			}
		})
	}
}

func TestCodec_EncodeUnknownType(t *testing.T) {
	t.Parallel()
	if _, err := Encode(struct{ X int }{X: 1}); err == nil {
		t.Fatal("expected error encoding an unknown proposal type")
	}
}

func TestSM_ProposeDocument_AssignsVersionAndCommits(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	res := apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	if !res.Accepted {
		t.Fatalf("first proposal rejected: %q", res.Reason)
	}
	if res.ProposalID != "p1" {
		t.Fatalf("ProposalID = %q, want p1", res.ProposalID)
	}
	if res.Doc.Version != 1 {
		t.Fatalf("committed version = %d, want 1", res.Doc.Version)
	}
	if got := sm.Current().Version; got != 1 {
		t.Fatalf("current version = %d, want 1", got)
	}
}

func TestSM_ProposeDocument_CASReject(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})

	// Second proposal still expects version 0, but current is now 1 → reject.
	stale := apply(t, sm, 2, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p2"})
	if stale.Accepted {
		t.Fatal("stale CAS proposal should have been rejected")
	}
	if stale.ProposalID != "p2" {
		t.Fatalf("rejected ProposalID = %q, want p2", stale.ProposalID)
	}
	if stale.Reason == "" {
		t.Fatal("rejected proposal must carry a reason")
	}
	if got := sm.Current().Version; got != 1 {
		t.Fatalf("current version = %d after reject, want 1 (unchanged)", got)
	}

	// A proposal with the correct expected version now succeeds.
	ok := apply(t, sm, 3, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 1, ProposalID: "p3"})
	if !ok.Accepted {
		t.Fatalf("in-order proposal rejected: %q", ok.Reason)
	}
	if got := sm.Current().Version; got != 2 {
		t.Fatalf("current version = %d, want 2", got)
	}
}

func TestSM_ProposeDocument_ValidationReject(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	bad := baseDoc()
	bad.Placement.MinReplicas = 0 // violates 1 <= min
	res := apply(t, sm, 1, ProposeDocument{Doc: bad, ExpectedVersion: 0, ProposalID: "p1"})
	if res.Accepted {
		t.Fatal("invalid document should have been rejected at apply")
	}
	if got := sm.Current().Version; got != 0 {
		t.Fatalf("current version = %d after invalid apply, want 0", got)
	}
}

func TestSM_AdmitNode_AddAndIdempotent(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})

	// Admit a second node; state defaulted to active.
	res := apply(t, sm, 2, AdmitNode{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, ProposalID: "j1"})
	if !res.Accepted {
		t.Fatalf("admit rejected: %q", res.Reason)
	}
	cur := sm.Current()
	if cur.Version != 2 {
		t.Fatalf("version after admit = %d, want 2", cur.Version)
	}
	n2 := cur.Node("n2")
	if n2 == nil || n2.State != topology.NodeActive {
		t.Fatalf("n2 not admitted active: %+v", n2)
	}

	// Re-admitting the same name is an accepted no-op (no version bump).
	dup := apply(t, sm, 3, AdmitNode{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, ProposalID: "j2"})
	if !dup.Accepted {
		t.Fatal("idempotent re-admit should be accepted")
	}
	if got := sm.Current().Version; got != 2 {
		t.Fatalf("version after duplicate admit = %d, want 2 (unchanged)", got)
	}
}

// TestSM_Apply_RefusesReplayedEntry pins apply-index dedupe. The SM blob is
// persisted per changed entry (carrying its own appliedIndex), while the
// raft-level applied index is written once per Ready batch AFTER the loop — so
// a fail-stop partway through a batch leaves the SM durably ahead of raft, and
// the next boot re-delivers entries already folded in. AdmitNode{ReplaceExisting}
// is the one non-idempotent handler: it allocates a fresh ReplicaID off
// NextReplicaID++ and bumps Version, so a replay re-swaps the identity and
// leaves this node permanently diverged from quorum.
func TestSM_Apply_RefusesReplayedEntry(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	apply(t, sm, 2, AdmitNode{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, ProposalID: "j1"})

	// n2 is re-admitted at a fresh identity (the S4b re-admission path).
	swap := AdmitNode{
		Node:            topology.Node{Name: "n2", Type: "worker"},
		MemberID:        2,
		ReplaceExisting: true,
		ProposalID:      "j2",
	}
	if res := apply(t, sm, 3, swap); !res.Accepted {
		t.Fatalf("replace-existing admit rejected: %q", res.Reason)
	}
	afterFirst := sm.Current()

	// The process fail-stopped before the batch's applied index reached disk, so
	// raft re-delivers entry 3 on the next boot. It must be a no-op.
	if _, err := sm.Apply(3, 1, mustEncode(t, swap)); err != nil {
		t.Fatalf("replayed entry returned an error: %v", err)
	}
	replayed := sm.Current()

	if replayed.Version != afterFirst.Version {
		t.Errorf("replay bumped version %d → %d; the SM is now ahead of quorum",
			afterFirst.Version, replayed.Version)
	}
	replicaOf := func(doc topology.Document, groupID string) (uint64, bool) {
		g := doc.Group(groupID)
		if g == nil {
			return 0, false
		}
		for _, r := range g.Replicas {
			if r.NodeName == "n2" {
				return r.ReplicaID, true
			}
		}
		return 0, false
	}
	for _, g := range afterFirst.Groups {
		before, ok := replicaOf(afterFirst, g.ID)
		if !ok {
			continue
		}
		after, ok := replicaOf(replayed, g.ID)
		if !ok {
			t.Errorf("group %s: replay dropped n2's replica", g.ID)
			continue
		}
		if before != after {
			t.Errorf("group %s: replay re-allocated n2's replica ID %d → %d", g.ID, before, after)
		}
	}
}

// TestSM_AdmitNode_StampsAuditFieldsFromProposal: a node that JOINS (rather than
// being admitted through Manager.AddNode) travels this apply path, which left
// CreatedAt zero — so every self-joined node served "0001-01-01T00:00:00Z" from
// the published API, against the documented "stamped once, at admission". The
// stamp must come from the proposal: this SM is replicated, and an applier
// reading its own clock would land a different value on every node.
func TestSM_AdmitNode_StampsAuditFieldsFromProposal(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})

	at := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	res := apply(t, sm, 2, AdmitNode{
		Node:       topology.Node{Name: "n2", Type: "worker"},
		MemberID:   2,
		At:         at,
		ProposalID: "j1",
	})
	if !res.Accepted {
		t.Fatalf("admit rejected: %q", res.Reason)
	}
	cur := sm.Current()
	n2 := cur.Node("n2")
	if n2 == nil {
		t.Fatal("n2 not admitted")
	}
	if !n2.CreatedAt.Equal(at) || !n2.UpdatedAt.Equal(at) {
		t.Errorf("n2 audit stamps = (created %v, updated %v), want both %v", n2.CreatedAt, n2.UpdatedAt, at)
	}
	// The _meta replica the admission creates is stamped too — it is the join
	// path's counterpart to the shard replicas allocateReplica already stamps.
	mg := cur.MetaGroup()
	if mg == nil {
		t.Fatal("no _meta group")
	}
	var found *topology.Replica
	for i := range mg.Replicas {
		if mg.Replicas[i].NodeName == "n2" {
			found = &mg.Replicas[i]
			break
		}
	}
	if found == nil {
		t.Fatal("n2 has no _meta replica")
	}
	if !found.CreatedAt.Equal(at) || !found.UpdatedAt.Equal(at) {
		t.Errorf("n2 _meta replica stamps = (created %v, updated %v), want both %v",
			found.CreatedAt, found.UpdatedAt, at)
	}
}

// The same apply must be deterministic across replicas: identical proposals land
// byte-identical state, which is exactly why the timestamp rides in the proposal
// instead of being read from each applier's clock.
func TestSM_AdmitNode_StampIsDeterministicAcrossReplicas(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	admit := AdmitNode{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, At: at, ProposalID: "j1"}

	a, b := newTopologySM(nil, testRules()), newTopologySM(nil, testRules())
	for _, sm := range []*topologySM{a, b} {
		apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
		apply(t, sm, 2, admit)
	}
	sa, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot A: %v", err)
	}
	sb, err := b.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot B: %v", err)
	}
	if !bytes.Equal(sa, sb) {
		t.Error("two replicas applying the same admit produced different snapshots — the SM diverged")
	}
}

// Replacement allocates new replica identities from each group allocator.
func TestSM_AdmitNode_ReplaceExisting_FreshMemberID(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})

	before := sm.Current()
	oldID := before.MemberID("n1")
	nextBefore := before.MetaGroup().NextReplicaID // the ID the counter is about to hand out

	res := apply(t, sm, 2, AdmitNode{
		Node:            topology.Node{Name: "n1"}, // no MemberID / address on the wire
		ReplaceExisting: true,
		ProposalID:      "readmit1",
	})
	if !res.Accepted {
		t.Fatalf("re-admission rejected: %q", res.Reason)
	}
	got := res.Doc.Node("n1")
	if got == nil {
		t.Fatal("re-admitted node missing from result document")
	}
	freshID := res.Doc.MemberID("n1")
	if freshID == oldID {
		t.Fatalf("re-admission reused the stale member ID %d", oldID)
	}
	if freshID != nextBefore {
		t.Fatalf("fresh member ID = %d, want the pre-allocation counter value %d", freshID, nextBefore)
	}
	if got.State != topology.NodeActive {
		t.Fatalf("re-admitted node state = %q, want active", got.State)
	}

	cur := sm.Current()
	if cur.Version != 2 {
		t.Fatalf("version after re-admission = %d, want 2", cur.Version)
	}
	if cur.MetaGroup().NextReplicaID <= nextBefore {
		t.Fatalf("counter not advanced past the fresh ID: NextReplicaID = %d, want > %d", cur.MetaGroup().NextReplicaID, nextBefore)
	}
	if cur.MemberID("n1") != freshID {
		t.Fatalf("committed member ID %d does not match the result doc %d", cur.MemberID("n1"), freshID)
	}
}

func TestSM_History_Ordering(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	apply(t, sm, 2, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 1, ProposalID: "p2"})
	apply(t, sm, 3, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 2, ProposalID: "p3"})

	all := sm.History(0, 0)
	if len(all) != 3 {
		t.Fatalf("history len = %d, want 3", len(all))
	}
	// Newest first.
	wantVersions := []uint64{3, 2, 1}
	for i, w := range wantVersions {
		if all[i].Version != w {
			t.Fatalf("history[%d].Version = %d, want %d", i, all[i].Version, w)
		}
	}
	// Limit returns the two newest.
	two := sm.History(0, 2)
	if len(two) != 2 || two[0].Version != 3 || two[1].Version != 2 {
		t.Fatalf("limited history = %v, want versions [3 2]", versionsOf(two))
	}
	// Get by exact version.
	if d, ok := sm.Get(2); !ok || d.Version != 2 {
		t.Fatalf("Get(2) = (%+v, %v)", d, ok)
	}
	if _, ok := sm.Get(99); ok {
		t.Fatal("Get(99) should not be found")
	}
}

// TestSM_ExclusionVersion pins the scalar the cluster page's discovered tiles
// read on every poll: it must name the version the exclusion LANDED at (not the
// newest one that still shows it excluded), and answer 0 rather than guess when
// the exclusion has aged out of the ring or never happened.
func TestSM_ExclusionVersion(t *testing.T) {
	t.Parallel()
	withNode := func(state topology.NodeState) topology.Document {
		d := baseDoc()
		d.Nodes = append(d.Nodes, topology.Node{Name: "n2", Type: "worker", State: state})
		if state != topology.NodeExcluded {
			if err := d.SetMemberID("n2", 2, time.Time{}); err != nil {
				t.Fatal(err)
			}
		} else {
			d.MetaGroup().NextReplicaID = 3 // Retain the removed member's allocation high water.
		}
		return d
	}
	sm := newTopologySM(nil, testRules())
	for i, state := range []topology.NodeState{topology.NodeActive, topology.NodeDrained, topology.NodeExcluded, topology.NodeExcluded, topology.NodeExcluded} {
		result := apply(t, sm, uint64(i+1), ProposeDocument{Doc: withNode(state), ExpectedVersion: uint64(i), ProposalID: fmt.Sprintf("p%d", i+1)})
		if !result.Accepted {
			t.Fatalf("stage %d: %s", i+1, result.Reason)
		}
	}
	// Two more documents that keep n2 excluded: the answer must not drift forward
	// with them, which is exactly what stamping the active version would do.

	if got := sm.ExclusionVersion("n2"); got != 3 {
		t.Fatalf("ExclusionVersion(n2) = %d, want 3 (where the exclusion landed)", got)
	}
	if got := sm.ExclusionVersion("n1"); got != 0 {
		t.Fatalf("ExclusionVersion(n1) = %d, want 0 — a node that was never excluded", got)
	}
	if got := sm.ExclusionVersion("never-seen"); got != 0 {
		t.Fatalf("ExclusionVersion(never-seen) = %d, want 0", got)
	}
	// Agreement with History(): the cheap path and the copying path must not be
	// able to disagree about the same ring.
	hist := sm.History(0, 0)
	if hist[len(hist)-3].Version != 3 {
		t.Fatalf("history layout changed; ExclusionVersion assertion above is no longer meaningful")
	}
}

// TestSM_ExclusionVersion_AgedOutOfRing is the horizon case the doc comment
// promises and the ring makes reachable: once the pre-exclusion document has
// rotated out, the oldest RETAINED document still shows the node excluded, and
// its version is merely where the ring starts. Reporting it would render
// "excluded at v<horizon>" as fact for a node retired long before — the tile and
// the confirm dialog both print that number.
func TestSM_ExclusionVersion_AgedOutOfRing(t *testing.T) {
	t.Parallel()
	withNode := func(state topology.NodeState) topology.Document {
		d := baseDoc()
		d.Nodes = append(d.Nodes, topology.Node{Name: "n2", Type: "worker", State: state})
		if err := d.SetMemberID("n2", 2, time.Time{}); err != nil {
			t.Fatal(err)
		}
		return d
	}
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: withNode(topology.NodeActive), ExpectedVersion: 0, ProposalID: "p1"})
	apply(t, sm, 2, ProposeDocument{Doc: withNode(topology.NodeExcluded), ExpectedVersion: 1, ProposalID: "p2"})
	// Turn the ring over completely: every retained document now shows n2
	// excluded, and v2 is gone.
	for v := uint64(2); v < historyRetention+5; v++ {
		apply(t, sm, v+1, ProposeDocument{Doc: withNode(topology.NodeExcluded), ExpectedVersion: v, ProposalID: fmt.Sprintf("pf%d", v)})
	}
	if _, ok := sm.Get(2); ok {
		t.Fatal("v2 is still retained; the ring did not turn over and this test proves nothing")
	}
	if got := sm.ExclusionVersion("n2"); got != 0 {
		t.Fatalf("ExclusionVersion(n2) = %d, want 0 — the exclusion aged out, so the version is unknown, not the ring horizon", got)
	}
}

// TestSM_History_Paging pins the offset semantics the journal API pages with:
// offset skips the N newest documents, order stays newest-first, and paging past
// the ring returns nothing.
func TestSM_History_Paging(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	for v := range uint64(4) {
		apply(t, sm, v+1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: v, ProposalID: fmt.Sprintf("p%d", v)})
	}

	cases := []struct {
		offset, limit int
		want          []uint64
	}{
		{0, 2, []uint64{4, 3}},    // first page
		{2, 2, []uint64{2, 1}},    // second page starts where the first stopped
		{1, 0, []uint64{3, 2, 1}}, // no limit: everything below the newest
		{3, 2, []uint64{1}},       // last page is short
		{4, 2, []uint64{}},        // exactly past the end
		{99, 2, []uint64{}},       // well past the end — empty, not an error
		{-1, 2, []uint64{4, 3}},   // a negative offset reads as 0
	}
	for _, tc := range cases {
		got := versionsOf(sm.History(tc.offset, tc.limit))
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("History(%d, %d) = %v, want %v", tc.offset, tc.limit, got, tc.want)
		}
	}
}

func TestSM_ExactOperatorTransitions(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	at := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	id := topology.MakeOperatorID(1, 1)

	if _, ok := sm.Operator(id); ok {
		t.Fatal("no execution should exist before BeginExecution")
	}

	begin := apply(t, sm, 2, beginProposal(1, 3, "b1", at))
	if !begin.Accepted || begin.OperatorID != id {
		t.Fatalf("begin = %+v, want accepted %q", begin, id)
	}
	apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning,
		GroupID: "shard0", NodeName: "n1", At: at.Add(time.Second),
	}, ProposalID: "s1"})
	index := consumeTestStep(t, sm, id, 4)
	apply(t, sm, index, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, StepSeq: 2, State: topology.OperatorRunning,
		GroupID: "shard0", NodeName: "n1", At: at.Add(2 * time.Second),
	}, ProposalID: "s2"})

	st, ok := sm.Operator(id)
	if !ok {
		t.Fatal("execution should exist after plan steps")
	}
	if st.CurrentStep != 2 || st.State != topology.OperatorRunning {
		t.Fatalf("execution = %+v, want CurrentStep 2 running", st)
	}

	// A stale lower-sequence transition is rejected and cannot rewind progress.
	stale := apply(t, sm, index+1, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning,
		GroupID: "shard0", NodeName: "n1", At: at.Add(3 * time.Second),
	}, ProposalID: "s1replay"})
	if stale.Accepted {
		t.Fatal("stale transition accepted")
	}
	if st, _ := sm.Operator(id); st.CurrentStep != 2 {
		t.Fatalf("CurrentStep rewound to %d after stale replay, want 2", st.CurrentStep)
	}

	index = consumeTestStep(t, sm, id, index+2)
	apply(t, sm, index, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, StepSeq: 3, State: topology.OperatorFailed,
		GroupID: "shard0", NodeName: "n1", Error: "conf change timeout", At: at.Add(4 * time.Second),
	}, ProposalID: "s3"})
	st, _ = sm.Operator(id)
	if st.State != topology.OperatorFailed || st.Error != "conf change timeout" || st.CurrentStep != 3 {
		t.Fatalf("failed execution = %+v", st)
	}
	terminalReplay := apply(t, sm, index+1, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, StepSeq: 3, State: topology.OperatorRunning,
		GroupID: "shard0", NodeName: "n1", At: at.Add(5 * time.Second),
	}, ProposalID: "terminal-rewind"})
	if terminalReplay.Accepted {
		t.Fatal("terminal execution returned to running")
	}

	// Execution transitions do not bump Latest.
	if got := sm.Current().Version; got != 1 {
		t.Fatalf("document version = %d after plan steps, want 1", got)
	}
}

func TestSM_ForcedRetirementRecordsApplyingActor(t *testing.T) {
	t.Parallel()
	active := multiNodeDoc()
	active.Version = 1
	target := active.Clone()
	target.Version = 2
	target.ChangedBy = "staging-admin"
	target.Origin = topology.OriginAPI
	target.Node("node3").State = topology.NodeExcluded
	meta := target.MetaGroup()
	meta.Replicas = meta.Replicas[:2]
	target.Audit = &topology.ChangeAudit{Operation: topology.AuditOperationForceRetireNode,
		TargetNode: "node3", Forced: true, Reason: "host lost", StagedBy: "staging-admin", StagedAt: time.Now().UTC()}
	plan := topology.Plan{Version: 2, Steps: []topology.Step{
		{Seq: 1, Kind: topology.StepRemoveReplica, GroupID: topology.MetaGroupID, NodeName: "node3", ReplicaID: 3},
		{Seq: 2, Kind: topology.StepStopReplica, GroupID: topology.MetaGroupID, NodeName: "node3", ReplicaID: 3},
	}}
	plan = withTestVoterProof(active, target, plan)
	sm := newTopologySM(nil, testRules())
	sm.active = active.Clone()
	sm.latest = target.Clone()
	sm.target = active.Clone()
	at := time.Now().UTC()
	missing := apply(t, sm, 1, BeginOperatorProposal{Version: 2, Plan: plan, Origin: topology.OriginAPI, At: at, ProposalID: "missing"})
	if missing.Accepted {
		t.Fatal("forced retirement accepted without applying actor")
	}
	res := apply(t, sm, 2, BeginOperatorProposal{Version: 2, Plan: plan, Actor: "applying-admin",
		Origin: topology.OriginAPI, At: at, ProposalID: "apply"})
	if !res.Accepted {
		t.Fatalf("forced begin rejected: %s", res.Reason)
	}
	record, ok := sm.CurrentOperator()
	if !ok || record.Actor != "applying-admin" || target.Audit.StagedBy != "staging-admin" {
		t.Fatalf("execution/audit actors = %+v / %+v", record, target.Audit)
	}
}

func TestSM_OperatorSupersededAtExactBoundary(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	at := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	id := topology.MakeOperatorID(1, 1)
	apply(t, sm, 2, beginProposal(1, 3, "begin", at))

	result := apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, StepSeq: 1,
		State: topology.OperatorSuperseded, GroupID: "shard0", NodeName: "n1", At: at.Add(time.Second),
	}, ProposalID: "supersede"})
	if !result.Accepted {
		t.Fatalf("boundary supersession rejected: %+v", result)
	}
	status, ok := sm.Operator(id)
	if !ok || status.State != topology.OperatorSuperseded || status.CurrentStep != 1 {
		t.Fatalf("superseded execution = (%+v, %v)", status, ok)
	}
	retry := apply(t, sm, 4, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, StepSeq: 1,
		State: topology.OperatorRunning, GroupID: "shard0", NodeName: "n1", At: at.Add(2 * time.Second),
	}, ProposalID: "retry-old"})
	if retry.Accepted || retry.Code != proposalSuperseded {
		t.Fatalf("terminal superseded execution accepted retry: %+v", retry)
	}
}

func TestSM_OperatorBeginRetryAndSupersession(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	at := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	plan := testPlan(1, 1)

	first := apply(t, sm, 2, beginProposal(1, 1, "b1", at))
	firstID := topology.MakeOperatorID(1, 1)
	if !first.Accepted || first.OperatorID != firstID {
		t.Fatalf("first begin = %+v", first)
	}
	replay := apply(t, sm, 3, beginProposal(1, 1, "b1-replay", at.Add(time.Second)))
	if !replay.Accepted || replay.OperatorID != firstID || len(sm.operators) != 1 {
		t.Fatalf("idempotent begin = %+v, executions=%d", replay, len(sm.operators))
	}
	conflictPlan := plan
	conflictPlan.Steps = append(conflictPlan.Steps, topology.Step{
		Seq: 2, GroupID: "shard0", Kind: topology.StepPromoteVoter, NodeName: "n1", ReplicaID: 1,
	})
	conflict := apply(t, sm, 4, BeginOperatorProposal{
		Version: 1, Plan: conflictPlan, Actor: "n1", At: at.Add(2 * time.Second), ProposalID: "b-conflict",
	})
	if conflict.Accepted || conflict.Code != proposalAuthorization {
		t.Fatalf("conflicting begin = %+v, want authorization rejection", conflict)
	}

	apply(t, sm, 5, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: firstID, Version: 1, Attempt: 1, StepSeq: 1, State: topology.OperatorFailed,
		GroupID: "shard0", NodeName: "n1", Error: "injected", At: at.Add(3 * time.Second),
	}, ProposalID: "fail-a1"})
	retry := apply(t, sm, 6, beginProposal(1, 1, "retry", at.Add(4*time.Second)))
	retryID := topology.MakeOperatorID(1, 2)
	if !retry.Accepted || retry.OperatorID != retryID {
		t.Fatalf("retry = %+v, want %q", retry, retryID)
	}
	if replayRetry := apply(t, sm, 7, beginProposal(1, 1, "retry-replay", at.Add(5*time.Second))); !replayRetry.Accepted || replayRetry.OperatorID != retryID || len(sm.operators) != 2 {
		t.Fatalf("retry replay = %+v, executions=%d", replayRetry, len(sm.operators))
	}

	apply(t, sm, 8, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 1, ProposalID: "p2"})
	newer := apply(t, sm, 9, beginProposal(2, 0, "b2", at.Add(6*time.Second)))
	if !newer.Accepted {
		t.Fatalf("newer begin rejected: %+v", newer)
	}
	old, _ := sm.Operator(retryID)
	if old.State != topology.OperatorSuperseded {
		t.Fatalf("old retry state = %q, want superseded", old.State)
	}
	if stale := apply(t, sm, 10, beginProposal(1, 0, "stale", at.Add(7*time.Second))); stale.Accepted || stale.Code != proposalSuperseded {
		t.Fatalf("stale begin = %+v", stale)
	}
}

func TestSM_RejectsMalformedPlanAndPrematureActivation(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	at := time.Date(2026, 7, 16, 13, 30, 0, 0, time.UTC)
	malformed := []topology.Plan{
		{Version: 1, Steps: []topology.Step{{Seq: 2, GroupID: "shard0", Kind: topology.StepAddLearner, NodeName: "n1", ReplicaID: 1}}},
		{Version: 1, Steps: []topology.Step{{Seq: 1, GroupID: "shard0", Kind: topology.StepKind("unknown")}}},
		{Version: 1, Steps: []topology.Step{
			{Seq: 1, GroupID: "shard0", Kind: topology.StepAddLearner, NodeName: "n1", ReplicaID: 1},
			{Seq: 2, GroupID: topology.MetaGroupID, Kind: topology.StepWaitCaughtUp, NodeName: "n1", ReplicaID: 1},
			{Seq: 3, GroupID: "shard0", Kind: topology.StepPromoteVoter, NodeName: "n1", ReplicaID: 1},
		}},
	}
	index := uint64(1)
	for i, plan := range malformed {
		index++
		res := apply(t, sm, index, BeginOperatorProposal{
			Version: 1, Plan: plan, Actor: "n1", At: at.Add(time.Duration(i) * time.Second),
			ProposalID: fmt.Sprintf("bad-%d", i),
		})
		if res.Accepted || res.Code != proposalAuthorization {
			t.Fatalf("malformed plan %d = %+v", i, res)
		}
	}
	index++
	begin := apply(t, sm, index, beginProposal(1, 0, "begin", at.Add(4*time.Second)))
	index++
	early := apply(t, sm, index, ActivateTargetProposal{
		OperatorID: begin.OperatorID, Version: 1, Attempt: 1, ProposalID: "early-activate",
	})
	if early.Accepted || sm.Active().Version != 0 {
		t.Fatalf("premature activation = %+v, Active v%d", early, sm.Active().Version)
	}
}

func TestSM_CommitRepairOperatorGuardrails(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	at := time.Date(2026, 7, 16, 14, 0, 0, 0, time.UTC)
	id := topology.MakeOperatorID(1, 1)
	apply(t, sm, 2, beginProposal(1, 0, "b1", at))
	apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: at.Add(time.Second),
	}, ProposalID: "done"})
	apply(t, sm, 4, ActivateTargetProposal{OperatorID: id, Version: 1, Attempt: 1, ProposalID: "activate"})

	repair := sm.Active()
	repair.Origin = topology.OriginReconciler
	repair.ChangedBy = "reconciler"
	repair.ParentVersion = repair.Version
	repair.Groups[1].Replicas[0].ReplicaID = 2
	repair.Groups[1].NextReplicaID = 3
	repairPlan := topology.Plan{Version: 2, Steps: []topology.Step{
		{Seq: 1, GroupID: "shard0", Kind: topology.StepPrepareReplica, NodeName: "n1", ReplicaID: 2},
		{Seq: 2, GroupID: "shard0", Kind: topology.StepAddLearner, NodeName: "n1", ReplicaID: 2},
		{Seq: 3, GroupID: "shard0", Kind: topology.StepWaitCaughtUp, NodeName: "n1", ReplicaID: 2},
		{Seq: 4, GroupID: "shard0", Kind: topology.StepPromoteVoter, NodeName: "n1", ReplicaID: 2},
	}}
	repairPlan = withTestVoterProof(sm.Active(), repair, repairPlan)
	badOrigin := repair.Clone()
	badOrigin.Origin = topology.OriginAPI
	if res := apply(t, sm, 5, CommitRepairOperatorProposal{
		Doc: badOrigin, ExpectedVersion: 1, Plan: repairPlan, At: at.Add(2 * time.Second), ProposalID: "api",
	}); res.Accepted || res.Code != proposalAuthorization {
		t.Fatalf("API repair = %+v", res)
	}
	if res := apply(t, sm, 6, CommitRepairOperatorProposal{
		Doc: repair, ExpectedVersion: 0, Plan: repairPlan, At: at.Add(3 * time.Second), ProposalID: "stale",
	}); res.Accepted || res.Code != proposalVersionConflict {
		t.Fatalf("stale repair = %+v", res)
	}
	dangerousPlan := topology.Plan{Version: 2, Steps: []topology.Step{{
		Seq: 1, GroupID: "shard0", Kind: topology.StepTransferLeader, NodeName: "n1", ReplicaID: 2,
	}}}
	if res := apply(t, sm, 7, CommitRepairOperatorProposal{
		Doc: repair, ExpectedVersion: 1, Plan: dangerousPlan, At: at.Add(4 * time.Second), ProposalID: "dangerous",
	}); res.Accepted || res.Code != proposalAuthorization {
		t.Fatalf("dangerous repair plan = %+v", res)
	}
	unsafe := apply(t, sm, 8, CommitRepairOperatorProposal{
		Doc: repair, ExpectedVersion: 1, Plan: repairPlan, At: at.Add(4 * time.Second), ProposalID: "repair",
	})
	if unsafe.Accepted {
		t.Fatalf("single-voter identity replacement crossed an unsafe boundary: %+v", unsafe)
	}
	if sm.Latest().Version != 1 || sm.Target().Version != 1 || sm.Active().Version != 1 {
		t.Fatalf("repair views = Latest v%d Target v%d Active v%d", sm.Latest().Version, sm.Target().Version, sm.Active().Version)
	}
}

func TestSM_PinsTargetActiveAndNonterminalBeyondHistory(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	at := time.Date(2026, 7, 16, 16, 0, 0, 0, time.UTC)
	id1 := topology.MakeOperatorID(1, 1)
	apply(t, sm, 2, beginProposal(1, 0, "b1", at))
	apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id1, Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: at.Add(time.Second),
	}, ProposalID: "done1"})
	apply(t, sm, 4, ActivateTargetProposal{OperatorID: id1, Version: 1, Attempt: 1, ProposalID: "activate1"})
	apply(t, sm, 5, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 1, ProposalID: "p2"})
	id2 := topology.MakeOperatorID(2, 1)
	apply(t, sm, 6, beginProposal(2, 1, "b2", at.Add(2*time.Second)))

	index := uint64(6)
	for version := uint64(2); version < historyRetention+20; version++ {
		index++
		apply(t, sm, index, ProposeDocument{Doc: baseDoc(), ExpectedVersion: version, ProposalID: fmt.Sprintf("p%d", version+1)})
	}
	if sm.Active().Version != 1 || sm.Target().Version != 2 {
		t.Fatalf("pinned views = Active v%d Target v%d, want v1/v2", sm.Active().Version, sm.Target().Version)
	}
	if _, ok := sm.Get(1); ok {
		t.Fatal("version 1 unexpectedly remained in bounded Latest history")
	}
	if _, ok := sm.Operator(id1); !ok {
		t.Fatal("Active-referenced terminal execution was pruned")
	}
	if st, ok := sm.Operator(id2); !ok || st.State.Terminal() {
		t.Fatalf("Target nonterminal execution = (%+v,%v), want pinned", st, ok)
	}
}

func TestSM_ApplyDeterminism(t *testing.T) {
	t.Parallel()
	// The same committed-entry sequence must yield byte-identical snapshots on
	// two independent state machines (the raft SM determinism contract).
	at := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	id := topology.MakeOperatorID(2, 1)
	seq := []struct {
		index uint64
		prop  any
	}{
		{1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"}},
		{2, AdmitNode{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, ProposalID: "j1"}},
		{3, beginProposal(2, 1, "b1", at)},
		{4, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "pStale"}}, // rejected CAS
		{5, TransitionOperatorProposal{Transition: topology.OperatorTransition{
			OperatorID: id, Version: 2, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning,
			GroupID: "shard0", NodeName: "n1", At: at.Add(time.Second),
		}, ProposalID: "s1"}},
	}
	run := func() []byte {
		sm := newTopologySM(nil, testRules())
		for _, s := range seq {
			apply(t, sm, s.index, s.prop)
		}
		snap, err := sm.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		return snap
	}
	a, b := run(), run()
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic snapshots:\n a=%s\n b=%s", a, b)
	}
}

func TestSM_SnapshotRestoreEquivalence(t *testing.T) {
	t.Parallel()
	src := newTopologySM(nil, testRules())
	apply(t, src, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	apply(t, src, 2, AdmitNode{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, ProposalID: "j1"})
	at := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	apply(t, src, 3, beginProposal(2, 1, "b1", at))
	id := topology.MakeOperatorID(2, 1)
	apply(t, src, 4, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: 2, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning,
		GroupID: "shard0", NodeName: "n1", At: at.Add(time.Second),
	}, ProposalID: "s1"})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	dst := newTopologySM(nil, testRules())
	if err := dst.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// A restored SM must re-serialize to identical bytes.
	snap2, err := dst.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot after restore: %v", err)
	}
	if !bytes.Equal(snap, snap2) {
		t.Fatalf("restore not byte-equivalent:\n orig=%s\n new =%s", snap, snap2)
	}

	// And observable state must match.
	if !reflect.DeepEqual(src.Current(), dst.Current()) {
		t.Fatal("current document differs after restore")
	}
	if !reflect.DeepEqual(src.History(0, 0), dst.History(0, 0)) {
		t.Fatal("history differs after restore")
	}
	if si, sok := src.Operator(id); true {
		di, dok := dst.Operator(id)
		if sok != dok || !reflect.DeepEqual(si, di) {
			t.Fatalf("execution differs after restore: src=(%+v,%v) dst=(%+v,%v)", si, sok, di, dok)
		}
	}
	idxA, termA := src.AppliedState()
	idxB, termB := dst.AppliedState()
	if idxA != idxB || termA != termB {
		t.Fatalf("applied state differs after restore: (%d,%d) vs (%d,%d)", idxA, termA, idxB, termB)
	}
}

func TestSM_HistoryRingBounded(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})

	snapSize := func() int {
		snap, err := sm.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		return len(snap)
	}

	// Fill exactly to the retention bound.
	for v := uint64(1); v < historyRetention; v++ {
		apply(t, sm, v+1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: v, ProposalID: fmt.Sprintf("f%d", v)})
	}
	if got := len(sm.History(0, 0)); got != historyRetention {
		t.Fatalf("history len at bound = %d, want %d", got, historyRetention)
	}
	sizeAtBound := snapSize()

	// Commit well past the bound; length stays pinned and serialized size does
	// not grow beyond negligible version-digit expansion.
	for v := uint64(historyRetention); v < 200; v++ {
		apply(t, sm, v+1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: v, ProposalID: fmt.Sprintf("g%d", v)})
	}
	all := sm.History(0, 0)
	if len(all) != historyRetention {
		t.Fatalf("history len after 200 commits = %d, want %d (ring must prune)", len(all), historyRetention)
	}
	// Newest first; the oldest retained is the horizon RollbackTo reports.
	if all[0].Version != 200 {
		t.Fatalf("newest retained version = %d, want 200", all[0].Version)
	}
	if oldest := all[len(all)-1].Version; oldest != 200-historyRetention+1 {
		t.Fatalf("oldest retained version = %d, want %d", oldest, 200-historyRetention+1)
	}
	if grow := snapSize() - sizeAtBound; grow > 1024 {
		t.Fatalf("serialized state grew by %d bytes past the ring bound, want bounded (ring not pruning)", grow)
	}
}

// versionsOf is a small helper for readable failure messages.
func versionsOf(docs []topology.Document) []uint64 {
	out := make([]uint64, len(docs))
	for i, d := range docs {
		out[i] = d.Version
	}
	return out
}

func multiNodeDoc() topology.Document {
	return topology.Document{
		ClusterID: "cluster-meta",
		Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Nodes: []topology.Node{
			{Name: "node1", Type: "worker", State: topology.NodeActive},
			{Name: "node2", Type: "worker", State: topology.NodeActive},
			{Name: "node3", Type: "worker", State: topology.NodeActive},
		},
		Groups: []topology.Group{
			// The _meta group carries every declared node's member ID; the running
			// raft membership (singleNodeConfig) is only node1 — the two are distinct.
			{ID: topology.MetaGroupID, NextReplicaID: 6, Replicas: []topology.Replica{
				{NodeName: "node1", ReplicaID: 1, Role: topology.ReplicaVoter},
				{NodeName: "node2", ReplicaID: 5, Role: topology.ReplicaVoter},
				{NodeName: "node3", ReplicaID: 3, Role: topology.ReplicaVoter},
			}},
			{ID: "shard0", NextReplicaID: 2, Replicas: []topology.Replica{
				{NodeName: "node1", ReplicaID: 1, Role: topology.ReplicaVoter},
			}},
		},
		ChangedBy: "test",
	}
}

func TestSM_PersistsToBoltStateBucket(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := metastore.Open(filepath.Join(dir, "meta.bolt"))
	if err != nil {
		t.Fatalf("openBoltStore: %v", err)
	}

	sm := newTopologySM(store, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})
	apply(t, sm, 2, AdmitNode{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, ProposalID: "j1"})

	want, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// State survives a close/reopen and restores an equivalent SM.
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store2, err := metastore.Open(filepath.Join(dir, "meta.bolt"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	data, err := store2.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if data == nil {
		t.Fatal("no SM state persisted to the state bucket")
	}

	recovered := newTopologySM(nil, testRules())
	if err := recovered.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, err := recovered.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("recovered SM differs from persisted:\n want=%s\n got =%s", want, got)
	}
}
