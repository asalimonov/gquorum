//go:build functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Start with actual committed founding/deletion plans and exact consumed stop
// receipts. Scale independent groups in the same two approvals, preserving all
// proof links and renumbering immutable steps. Restore validates the full graph;
// simply appending arbitrary cleanup rows would not measure a reachable state.
func cleanupDebtSeed(tb testing.TB) *Machine {
	tb.Helper()
	m := mustMachine(tb, testRules(), nil)
	doc := baseDoc()
	doc.Origin = topology.OriginFounding
	seed := raftpb.ConfState{Voters: []uint64{1}}
	for version := uint64(1); version <= 2; version++ {
		if version == 2 {
			doc = m.Latest()
			doc.Groups = doc.Groups[:1]
			doc.Origin = topology.OriginOperator
		}
		benchSubmit(tb, m, seed, ProposeDocument{ProposalID: "benchmark", Doc: doc, ExpectedVersion: version - 1})
		read := snapshotPlanRead(tb, m, version, seed)
		plan, err := topology.BuildPlanPreferred(read.Input, read.Target, topology.PlanOptions{Rules: m.rules})
		if err != nil {
			tb.Fatal(err)
		}
		at := time.Date(2026, 10, 17, 0, 0, int(version), 0, time.UTC)
		op := benchSubmit(tb, m, seed, BeginOperationProposal{DeletionSemantics: DeletionSemanticsDetached, ProposalID: "benchmark", Version: version, Plan: plan, Scopes: read.Scopes, Actor: "benchmark", Origin: topology.OriginOperator, At: at})
		for _, block := range plan.Blocks {
			for i, seq := range block.Steps {
				step := plan.Steps[seq-1]
				transition := topology.OperationTransition{OperationID: op.OperationID, Version: version, Attempt: 1, GroupID: block.GroupID, StepSeq: seq, State: topology.OperationRunning, At: at}
				benchSubmit(tb, m, seed, TransitionOperationProposal{ProposalID: "benchmark", Transition: transition})
				action := topology.AuthorizedStep{OperationID: op.OperationID, Version: version, Attempt: 1, StepSeq: seq, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
				grant := benchSubmit(tb, m, seed, BeginEffectProposal{ProposalID: "benchmark", Action: action}).Grant
				if grant == nil {
					tb.Fatal("seed omitted grant")
				}
				for _, receipt := range fixtureEffectReceipts(*grant) {
					benchSubmit(tb, m, seed, RecordEffectProposal{ProposalID: "benchmark", Receipt: receipt, At: at})
				}
				if i == len(block.Steps)-1 {
					transition.State = topology.OperationSucceeded
					benchSubmit(tb, m, seed, TransitionOperationProposal{ProposalID: "benchmark", Transition: transition})
				}
			}
		}
	}
	work, err := m.ReadyCleanup(storage.Identity{}, 1)
	if err != nil || len(work) != 1 || work[0].Action.Kind != topology.StepStopReplica {
		tb.Fatal("seed lacks deferred stop", work, err)
	}
	grant := benchSubmit(tb, m, seed, BeginCleanupEffectProposal{ProposalID: "benchmark",
		Identity: work[0].Identity, Action: work[0].Action}).Grant
	for _, receipt := range fixtureEffectReceipts(*grant) {
		benchSubmit(tb, m, seed, RecordEffectProposal{ProposalID: "benchmark", Receipt: receipt,
			At: time.Date(2026, 10, 17, 0, 0, 3, 0, time.UTC)})
	}
	if len(m.scoped.Cleanup) != 1 || m.scoped.Cleanup[0].Stopped == nil || m.scoped.Cleanup[0].Retired != nil {
		tb.Fatal("seed lacks detached unresolved retirement")
	}
	return m
}

func benchSubmit(tb testing.TB, m *Machine, conf raftpb.ConfState, proposal any) ApplyResult {
	tb.Helper()
	data, err := Encode(proposal)
	if err != nil {
		tb.Fatal(err)
	}
	index, term := m.AppliedState()
	result, err := m.Apply(tb.Context(), raftengine.Command{Index: index + 1, Term: max(term, 1), ConfState: conf, Data: data})
	var applied ApplyResult
	if err != nil || json.Unmarshal(result.Data, &applied) != nil || !applied.Accepted {
		tb.Fatalf("%T: %+v %v", proposal, applied, err)
	}
	return applied
}

func renameDebtValue[T any](tb testing.TB, source T, id string) T {
	tb.Helper()
	data, err := json.Marshal(source)
	if err != nil {
		tb.Fatal(err)
	}
	var cloned T
	if err := json.Unmarshal(bytes.ReplaceAll(data, []byte(`"shard0"`), []byte(fmt.Sprintf("%q", id))), &cloned); err != nil {
		tb.Fatal(err)
	}
	return cloned
}

func scaleDebtRows[T any](tb testing.TB, rows []T, count int, group func(T) string) []T {
	var scaled []T
	for _, row := range rows {
		if group(row) != "shard0" {
			scaled = append(scaled, row)
			continue
		}
		for i := range count {
			scaled = append(scaled, renameDebtValue(tb, row, fmt.Sprintf("debt%05d", i)))
		}
	}
	return scaled
}

func cleanupDebtFixture(tb testing.TB, count int) *Machine {
	tb.Helper()
	seed := cleanupDebtSeed(tb)
	s := *seed.scoped
	s.AppliedIndex *= uint64(count + 1)
	for _, doc := range append([]*topology.Document{&s.Latest, &s.Target}, documentPointers(s.History)...) {
		doc.Groups = scaleDebtRows(tb, doc.Groups, count, func(g topology.Group) string { return g.ID })
	}
	sequences := map[uint64]map[string]map[int]int{}
	for i := range s.Operations {
		op := &s.Operations[i]
		old := op.Clone()
		if len(old.Plan.Dependencies) != 0 {
			tb.Fatal("seed gained cross-group dependencies")
		}
		op.Plan.Blocks = scaleDebtRows(tb, old.Plan.Blocks, count, func(b topology.PlanBlock) string { return b.GroupID })
		op.Status.Blocks = scaleDebtRows(tb, old.Status.Blocks, count, func(b topology.BlockStatus) string { return b.GroupID })
		op.Status.TotalBlocks = len(op.Status.Blocks)
		op.Plan.VoterBases = scaleDebtRows(tb, old.Plan.VoterBases, count, func(b topology.PlanVoterBase) string { return b.GroupID })
		op.Plan.Steps = nil
		sequences[op.Status.Version] = map[string]map[int]int{}
		for j := range op.Plan.Blocks {
			block := &op.Plan.Blocks[j]
			mapping := map[int]int{}
			seqs := append(slices.Clone(block.Steps), block.Cleanup...)
			slices.Sort(seqs)
			for _, seq := range seqs {
				step := old.Plan.Steps[seq-1]
				step.Seq, step.GroupID = len(op.Plan.Steps)+1, block.GroupID
				mapping[seq] = step.Seq
				op.Plan.Steps = append(op.Plan.Steps, step)
			}
			for k, seq := range block.Steps {
				block.Steps[k] = mapping[seq]
			}
			for k, seq := range block.Cleanup {
				block.Cleanup[k] = mapping[seq]
			}
			op.Status.Blocks[j].CurrentStep = mapping[op.Status.Blocks[j].CurrentStep]
			sequences[op.Status.Version][block.GroupID] = mapping
		}
	}
	s.Groups = scaleDebtRows(tb, s.Groups, count, func(r groupConvergence) string { return r.GroupID })
	s.Cleanup = scaleDebtRows(tb, s.Cleanup, count, func(r cleanupRecord) string { return r.Identity.GroupID })
	s.RetiredGroups = scaleDebtRows(tb, s.RetiredGroups, count, func(r retiredGroup) string { return r.ID })
	s.Allocations = scaleDebtRows(tb, s.Allocations, count, func(r allocationRecord) string { return r.GroupID })
	s.AuthorityFloors = scaleDebtRows(tb, s.AuthorityFloors, count, func(r resourceFloor) string { return r.Resource.GroupID })
	s.AuthorityRecords = scaleDebtRows(tb, s.AuthorityRecords, count, func(r scopedAuthorityRecord) string { return r.Grant.Key.GroupID })
	s.Revocations = scaleDebtRows(tb, s.Revocations, count, func(r scopedRevocationRecord) string { return r.GroupID })
	rewrite := func(action *topology.AuthorizedStep) {
		if action != nil {
			action.StepSeq = sequences[action.Version][action.GroupID][action.StepSeq]
		}
	}
	for i := range s.AuthorityRecords {
		record := &s.AuthorityRecords[i]
		rewrite(record.Grant.Action)
		for j := range record.Effects {
			rewrite(record.Effects[j].Action)
		}
	}
	for i := range s.Cleanup {
		r := &s.Cleanup[i]
		rewrite(r.StopAction)
		rewrite(r.RetireAction)
		for _, receipt := range []*topology.EffectReceipt{r.Removed, r.Stopped, r.Retired} {
			if receipt != nil {
				rewrite(receipt.Action)
			}
		}
	}
	m := restoreMachineFixture(tb, s)
	// A genuinely approved new group competes with the retained cleanup workload.
	doc := m.Latest()
	group := baseDoc().Groups[1]
	group.ID = "healthy"
	doc.Groups = append(doc.Groups, group)
	conf := raftpb.ConfState{Voters: []uint64{1}}
	staged := benchSubmit(tb, m, conf, ProposeDocument{ProposalID: "benchmark", Doc: doc, ExpectedVersion: doc.Version})
	read := snapshotPlanRead(tb, m, staged.Doc.Version, conf)
	plan, err := topology.BuildPlanPreferred(read.Input, read.Target, topology.PlanOptions{Rules: m.rules})
	if err != nil {
		tb.Fatal(err)
	}
	benchSubmit(tb, m, conf, BeginOperationProposal{DeletionSemantics: DeletionSemanticsDetached, ProposalID: "benchmark", Version: staged.Doc.Version, Plan: plan, Scopes: read.Scopes, Actor: "benchmark", Origin: topology.OriginOperator, At: time.Date(2026, 10, 17, 0, 1, 0, 0, time.UTC)})
	if len(m.scoped.Cleanup) != count {
		tb.Fatal("unresolved debt count changed", len(m.scoped.Cleanup), count)
	}
	return m
}

func documentPointers(documents []topology.Document) []*topology.Document {
	pointers := make([]*topology.Document, len(documents))
	for i := range documents {
		pointers[i] = &documents[i]
	}
	return pointers
}

type debtBenchmarkPersister struct {
	store        statePersister
	bytes, calls uint64
}

func (p *debtBenchmarkPersister) SaveState(data []byte) error {
	p.bytes += uint64(len(data))
	p.calls++
	if p.store != nil {
		return p.store.SaveState(data)
	}
	return nil
}

func BenchmarkCleanupDebtApply(b *testing.B) {
	for _, count := range []int{0, 1000, 10000} {
		b.Run(fmt.Sprintf("debts-%d", count), func(b *testing.B) {
			m := cleanupDebtFixture(b, count)
			snapshot, err := m.Snapshot(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			for _, disk := range []bool{false, true} {
				b.Run(map[bool]string{false: "encode-apply", true: "bolt-apply"}[disk], func(b *testing.B) {
					owned := mustMachine(b, m.rules, nil)
					if err := owned.Restore(b.Context(), snapshot); err != nil {
						b.Fatal(err)
					}
					p := &debtBenchmarkPersister{}
					if disk {
						store, err := metastore.Open(filepath.Join(b.TempDir(), "meta.bolt"))
						if err != nil {
							b.Fatal(err)
						}
						defer store.Close()
						p.store = store
					}
					owned.persister = p
					grant, _ := Encode(GrantSession{ProposalID: "benchmark", SessionID: "benchmark", NodeName: "n1", InstanceID: "bench", TTLSeconds: 30})
					revoke, _ := Encode(RevokeSession{ProposalID: "benchmark", SessionID: "benchmark"})
					index, _ := owned.AppliedState()
					b.ReportAllocs()
					for b.Loop() {
						data := grant
						if p.calls%2 != 0 {
							data = revoke
						}
						index++
						result, err := owned.Apply(b.Context(), raftengine.Command{Index: index, Term: 1, Data: data})
						if err != nil || result.Code != "" {
							b.Fatal(result, err)
						}
					}
					b.ReportMetric(float64(p.bytes)/float64(p.calls), "persisted_bytes/op")
				})
			}
			b.Run("snapshot", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := m.Snapshot(b.Context()); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(snapshot)), "snapshot_bytes")
			})
			b.Run("restore", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					restored := mustMachine(b, m.rules, nil)
					if err := restored.Restore(b.Context(), snapshot); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("progress-25", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					page, err := m.Progress(topology.ProgressQuery{Limit: 25})
					if err != nil || progressRows(page) > 25 {
						b.Fatal(page, err)
					}
				}
			})
			b.Run("cleanup-2", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rows, err := m.ReadyCleanup(storage.Identity{}, 2)
					if err != nil || len(rows) != min(count, 2) {
						b.Fatal(rows, err)
					}
				}
			})
			b.Run("ordinary-4", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rows, err := m.ReadyBlocks("", 4)
					if err != nil || len(rows) != 1 || rows[0].GroupID != "healthy" {
						b.Fatal(rows, err)
					}
				}
			})
		})
	}
}

func progressRows(p topology.ProgressPage) int {
	count := len(p.Groups) + len(p.Nodes) + len(p.Cleanup)
	for _, operation := range p.Operations {
		count += len(operation.Blocks)
	}
	return count
}

func TestCleanupDebtQueriesStayBoundedAndPermitIndependentGrant(t *testing.T) {
	t.Parallel()
	m := cleanupDebtFixture(t, 1000)
	page, err := m.Progress(topology.ProgressQuery{Limit: 17})
	if err != nil || progressRows(page) != 17 || page.Next == "" {
		t.Fatal("progress did not retain its page bound", progressRows(page), err)
	}
	first, err := m.ReadyCleanup(storage.Identity{}, 2)
	if err != nil || len(first) != 2 {
		t.Fatal(first, err)
	}
	next, err := m.ReadyCleanup(first[1].Identity, 2)
	if err != nil || len(next) != 2 || next[0].Identity == first[0].Identity || next[0].Identity == first[1].Identity {
		t.Fatal("cleanup did not rotate exact debt", next, err)
	}
	work, err := m.ReadyBlocks("", 4)
	if err != nil || len(work) != 1 || work[0].GroupID != "healthy" {
		t.Fatal(work, err)
	}
	op := work[0].Operation
	step := op.Plan.Steps[0]
	at := op.UpdatedAt.Add(time.Second)
	conf := raftpb.ConfState{Voters: []uint64{1}}
	benchSubmit(t, m, conf, TransitionOperationProposal{ProposalID: "running", Transition: topology.OperationTransition{OperationID: op.Status.ID, Version: op.Status.Version, Attempt: op.Status.Attempt, GroupID: step.GroupID, StepSeq: step.Seq, State: topology.OperationRunning, At: at}})
	action := topology.AuthorizedStep{OperationID: op.Status.ID, Version: op.Status.Version, Attempt: op.Status.Attempt, GroupID: step.GroupID, StepSeq: step.Seq, Kind: step.Kind, NodeName: step.NodeName, ReplicaID: step.ReplicaID, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
	result := benchSubmit(t, m, conf, BeginEffectProposal{ProposalID: "independent", Action: action})
	if result.Grant == nil || result.Grant.Key.GroupID != "healthy" || len(m.scoped.Cleanup) != 1000 {
		t.Fatal("unresolved cleanup blocked or was discarded for an ordinary grant", result)
	}
}
