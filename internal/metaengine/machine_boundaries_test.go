//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type countingStatePersister struct{ writes int }

func (p *countingStatePersister) SaveState([]byte) error { p.writes++; return nil }

func TestMachineReplayAtPersistedBoundaryCannotAllocateOrPersist(t *testing.T) {
	t.Parallel()
	m := restoreMachineFixture(t, scopedCodecFixture())
	store := &countingStatePersister{}
	m.persister = store
	before, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	doc := m.Latest()
	doc.Comment = "would stage a fresh version"
	command := mustEncode(t, ProposeDocument{Doc: doc, ExpectedVersion: doc.Version, ProposalID: "replay"})
	index, term := m.AppliedState()
	for _, replay := range []uint64{index, index - 1, 1} {
		if _, err := m.Apply(t.Context(), raftengine.Command{Index: replay, Term: term, Data: command}); err != nil {
			t.Fatal(err)
		}
		after, err := m.Snapshot(t.Context())
		if err != nil || !bytes.Equal(before, after) || store.writes != 0 {
			t.Fatal("replayed entry allocated identities, changed state or persisted", err, store.writes)
		}
	}
}

func TestMachineMixedLogProducesIdenticalResultsAndSnapshots(t *testing.T) {
	t.Parallel()
	fixture, approval := foundingApprovalFixture(t)
	machines := []*Machine{mustMachine(t, testRules(), nil), mustMachine(t, testRules(), nil)}
	index := uint64(0)
	apply := func(proposal any, accepted bool) ApplyResult {
		t.Helper()
		index++
		command := raftengine.Command{Index: index, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1}}, Data: mustEncode(t, proposal)}
		var expected []byte
		var applied ApplyResult
		for _, m := range machines {
			result, err := m.Apply(t.Context(), command)
			if err != nil || json.Unmarshal(result.Data, &applied) != nil || applied.Accepted != accepted {
				t.Fatalf("Apply(%T): result=%+v error=%v", proposal, applied, err)
			}
			if expected != nil && !bytes.Equal(expected, result.Data) {
				t.Fatal("replicas returned different results")
			}
			expected = result.Data
		}
		left, err := machines[0].Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		right, err := machines[1].Snapshot(t.Context())
		if err != nil || !bytes.Equal(left, right) {
			t.Fatal("replicas produced different snapshots", err)
		}
		return applied
	}
	apply(ProposeDocument{Doc: fixture.Latest, ProposalID: "stage"}, true)
	apply(approval, true)
	op := machines[0].scoped.Operations[0].Clone()
	block := op.Plan.Blocks[1]
	for _, seq := range block.Steps {
		transition := scopedTransitionFor(op, block.GroupID, seq)
		apply(TransitionOperationProposal{Transition: transition, ProposalID: "transition"}, true)
		step := op.Plan.Steps[seq-1]
		if step.Kind == topology.StepWaitCaughtUp {
			continue
		}
		grant := apply(BeginEffectProposal{Action: scopedPlanAction(&op, step), ProposalID: "begin"}, true).Grant
		for _, receipt := range fixtureEffectReceipts(*grant) {
			apply(RecordEffectProposal{Receipt: receipt, At: transition.At, ProposalID: "receipt"}, true)
		}
	}
	complete := scopedTransitionFor(op, block.GroupID, block.Steps[len(block.Steps)-1])
	complete.State = topology.OperationSucceeded
	apply(TransitionOperationProposal{Transition: complete, ProposalID: "complete"}, true)
	apply(GrantSession{SessionID: "s", NodeName: "n1", InstanceID: "i", TTLSeconds: 10}, true)
	apply(AcquireLock{Key: "node/n1", SessionID: "s"}, true)
	stale := fixture.Latest.Clone()
	stale.Comment = "different request"
	apply(ProposeDocument{Doc: stale, ProposalID: "stale-stage"}, false)
	for _, m := range machines {
		snapshot, err := m.Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Restore(t.Context(), snapshot); err != nil {
			t.Fatal(err)
		}
		restored, err := m.Snapshot(t.Context())
		if err != nil || !bytes.Equal(snapshot, restored) {
			t.Fatal("restart changed deterministic checkpoint", err)
		}
	}
}

func TestMachineHistoryOrderingPagingAndOwnedExactLookup(t *testing.T) {
	t.Parallel()
	m := mustMachine(t, testRules(), nil)
	for version := range uint64(4) {
		doc := baseDoc()
		doc.ParentVersion = version
		if result := applyMachine(t, m, ProposeDocument{Doc: doc, ExpectedVersion: version}); !result.Accepted {
			t.Fatal(result)
		}
	}
	for _, tc := range []struct {
		offset, limit int
		versions      []uint64
	}{
		{0, 0, []uint64{4, 3, 2, 1}}, {0, 2, []uint64{4, 3}}, {2, 2, []uint64{2, 1}},
		{1, 0, []uint64{3, 2, 1}}, {3, 2, []uint64{1}}, {4, 2, nil}, {99, 2, nil}, {-1, 2, []uint64{4, 3}},
	} {
		var versions []uint64
		for _, doc := range m.History(tc.offset, tc.limit) {
			versions = append(versions, doc.Version)
		}
		if !reflect.DeepEqual(versions, tc.versions) {
			t.Fatal(tc, versions)
		}
	}
	doc, found := m.Get(2)
	if !found || doc.Version != 2 {
		t.Fatal(doc, found)
	}
	doc.Nodes[0].Name = "caller"
	history := m.History(0, 1)
	history[0].Groups[0].Replicas[0].ReplicaID = 99
	if doc, _ := m.Get(2); doc.Nodes[0].Name != "n1" || m.scoped.Latest.MemberID("n1") != 1 {
		t.Fatal("history aliases metadata")
	}
	if _, found := m.Get(99); found {
		t.Fatal("unknown version found")
	}
}

func TestMachineInvalidDocumentAndSessionRebindPreserveEvidence(t *testing.T) {
	t.Parallel()
	m := restoreMachineFixture(t, scopedGraphFixture())
	invalid := m.Latest()
	invalid.Groups = nil
	before := m.Latest()
	if result := applyMachine(t, m, ProposeDocument{Doc: invalid, ExpectedVersion: before.Version}); result.Accepted || !reflect.DeepEqual(before, m.Latest()) {
		t.Fatal("invalid document installed", result)
	}
	for _, parent := range []uint64{before.Version + 1, before.Version + 100} {
		invalid = before.Clone()
		invalid.ParentVersion = parent
		result := applyMachine(t, m, ProposeDocument{Doc: invalid, ExpectedVersion: before.Version})
		if result.Accepted || result.Code != proposalErrorCode(fault.Invalid) || !reflect.DeepEqual(before, m.Latest()) {
			t.Fatal("invalid caller ancestry was not a deterministic refusal", result)
		}
	}
	p := GrantSession{SessionID: "s", NodeName: "n1", InstanceID: "i", TTLSeconds: 10}
	if result := applyMachine(t, m, p); !result.Accepted {
		t.Fatal(result)
	}
	retained, found := m.Session("s")
	if !found {
		t.Fatal("missing session")
	}
	for _, mutate := range []func(*GrantSession){
		func(p *GrantSession) { p.NodeName = "other" },
		func(p *GrantSession) { p.InstanceID = "other" },
		func(p *GrantSession) { p.TTLSeconds++ },
	} {
		bad := p
		mutate(&bad)
		if result := applyMachine(t, m, bad); result.Accepted {
			t.Fatal("session rebound", result)
		}
		if got, found := m.Session("s"); !found || got != retained {
			t.Fatal("refusal changed session identity", got)
		}
	}
	if result := applyMachine(t, m, p); !result.Accepted {
		t.Fatal("exact session replay refused", result)
	}
	// A valid unrelated command still applies after each deterministic refusal.
	if result := applyMachine(t, m, AcquireLock{Key: "node/n1", SessionID: p.SessionID}); !result.Accepted {
		t.Fatal(result)
	}
}
