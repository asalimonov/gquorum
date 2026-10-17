//go:build functional

package raftengine

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestObservedEffectsAndTransferIntentSurviveRestart(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	initial := foundingAuthority()
	g := newManagedTestGroup(t, s, &machine{}, n, &initial)
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	installTestAuthority(t, g, initial)
	observed, err := g.ObserveAuthorizedEffect(testContext(t), initial)
	must(t, err)
	must(t, observed.Validate(initial))
	if observed.Outcome != topology.EffectAlreadySatisfied {
		t.Fatal(observed)
	}
	native, err := g.ReadNative(testContext(t))
	must(t, err)
	if native.Grant == nil || !native.Grant.Equal(initial) || native.Receipt == nil || native.Receipt.Validate(initial) != nil {
		t.Fatalf("native read lost exact durable founding evidence: %+v", native)
	}
	native.Grant.Action.ResultVotersDigest = "altered"
	againNative, err := g.ReadNative(testContext(t))
	must(t, err)
	if !againNative.Grant.Equal(initial) {
		t.Fatal("native read aliased owner authority")
	}
	grant := learnerAuthority(20, 2)
	installTestAuthority(t, g, grant)
	if _, err := g.ObserveAuthorizedEffect(testContext(t), grant); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("invented native effect", err)
	}
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}}}
	original, err := g.ProposeAuthorizedConfChange(testContext(t), grant, cc, g.Status().ConfState)
	must(t, err)
	originalGrant := grant.Clone()
	grant.Key.Revision++
	grant.Action.Attempt++
	grant.Action.OperationID = topology.MakeOperationID(grant.Action.Version, grant.Action.Attempt)
	grant.OperationID = grant.Action.OperationID
	if _, err := g.ProposeAuthorizedConfChange(testContext(t), grant, cc, g.Status().ConfState); !errors.Is(err, fault.ErrAuthorityFenced) && !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("observed membership bypassed the new floor", err)
	}
	// An already-applied exact effect is judged by its result, even if its
	// historical predecessor is no longer the native configuration.
	grant.Action.ExpectedVotersDigest = topology.VoterSetDigest([]uint64{99})
	installTestAuthority(t, g, grant)
	// A fresh grant observing existing membership consumes an ordinary entry.
	observed, err = g.ProposeAuthorizedConfChange(testContext(t), grant, cc, g.Status().ConfState)
	must(t, err)
	if observed.Outcome != topology.EffectAlreadySatisfied {
		t.Fatal(observed)
	}
	must(t, original.Validate(originalGrant))
	must(t, observed.Validate(grant))
	if observed.Key == original.Key || observed.Action.OperationID == original.Action.OperationID || observed.AppliedIndex <= original.AppliedIndex {
		t.Fatal("new attempt relabelled old receipt", original, observed)
	}
	transfer := initial.Clone()
	transfer.Key.Revision = 30
	transfer.Resources = transfer.Resources[:1]
	transfer.Action.Kind, transfer.Action.NodeName, transfer.Action.ReplicaID = topology.StepTransferLeader, "1", 1
	transfer.Action.ExpectedVotersDigest, transfer.Action.ResultVotersDigest = "", ""
	installTestAuthority(t, g, transfer)
	must(t, g.TransferAuthorized(testContext(t), transfer))
	receipt, err := g.AuthorityReceipt(testContext(t), transfer.Key)
	must(t, err)
	if !receipt.Found || receipt.Receipt.Outcome != topology.EffectTransferStarted {
		t.Fatal(receipt.Receipt, receipt.Found)
	}
	must(t, g.Snapshot(testContext(t)))
	must(t, g.Stop(testContext(t)))
	restarted := newManagedTestGroup(t, s, &machine{}, n, nil)
	must(t, restarted.Start(t.Context()))
	must(t, restarted.WaitReady(testContext(t)))
	installTestAuthority(t, restarted, transfer)
	must(t, restarted.TransferAuthorized(testContext(t), transfer))
	again, err := restarted.AuthorityReceipt(testContext(t), transfer.Key)
	must(t, err)
	if !again.Found || !reflect.DeepEqual(again.Receipt, receipt.Receipt) {
		t.Fatal("retry repeated intent", again.Receipt, receipt.Receipt)
	}
	fence := transfer.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	installTestAuthority(t, restarted, fence)
	if err := restarted.TransferAuthorized(testContext(t), transfer); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
	if _, err := restarted.ObserveAuthorizedEffect(testContext(t), grant); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("native state refreshed old authority", err)
	}
}

func TestTransferIntentResumesPerLeaderTermAndStopsAtFence(t *testing.T) {
	t.Parallel()
	for _, demotion := range []bool{false, true} {
		t.Run(map[bool]string{false: "transfer", true: "demotion"}[demotion], func(t *testing.T) {
			t.Parallel()
			testTransferResumption(t, demotion)
		})
	}
}

func testTransferResumption(t *testing.T, demotion bool) {
	t.Helper()
	ms := raft.NewMemoryStorage()
	cs := raftpb.ConfState{Voters: []uint64{1, 2}}
	must(t, ms.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}))
	must(t, ms.SetHardState(raftpb.HardState{Term: 1, Commit: 1}))
	rn, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: ms, Applied: 1, MaxInflightMsgs: 16, MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 1 << 20})
	must(t, err)
	grant := learnerAuthority(20, 2)
	grant.Action.Kind = topology.StepTransferLeader
	grant.Action.ExpectedVotersDigest, grant.Action.ResultVotersDigest = "", ""
	ready := make(chan struct{})
	close(ready)
	g := &Group{rn: rn, ms: ms, cs: cs, applied: 1, ready: ready, opts: Options{ManagedAuthority: true, ReplicaID: 1, GroupID: "load", ClusterID: "cluster"}, changes: make(map[string]*request)}
	var receipt topology.EffectReceipt
	g.authority = raftstate.AuthorityState{Grant: &grant, InstallIndex: 1, InstallTerm: 1}
	if demotion {
		grant.Action.Kind, grant.Action.NodeName, grant.Action.ReplicaID = topology.StepDemoteVoter, "1", 1
		grant.Action.TransferNodeName, grant.Action.TransferReplicaID = "other", 2
		grant.Action.ExpectedVotersDigest, grant.Action.ResultVotersDigest = topology.VoterSetDigest([]uint64{1, 2}), topology.VoterSetDigest([]uint64{2})
	} else {
		receipt, err = g.effectReceipt(raftpb.Entry{Index: 1, Term: 1}, grant, topology.EffectTransferStarted)
		must(t, err)
		g.authority.Receipt = &receipt
	}
	// This is a single RawNode with protocol responses, not a multi-node fixture.
	drain := func() []raftpb.Message {
		var messages []raftpb.Message
		for rn.HasReady() {
			rd := rn.Ready()
			must(t, ms.Append(rd.Entries))
			if !raft.IsEmptyHardState(rd.HardState) {
				must(t, ms.SetHardState(rd.HardState))
				g.durableHS = rd.HardState
			}
			for _, entry := range rd.CommittedEntries {
				if len(entry.Data) != 0 {
					e, err := raftstate.DecodeEntry(entry.Data)
					must(t, err)
					must(t, g.applyAuthority(entry, e))
				}
				g.applied = entry.Index
			}
			messages = append(messages, rd.Messages...)
			rn.Advance(rd)
			for _, reply := range g.adminNotify {
				answer(reply.req, reply.result, reply.err)
			}
			g.adminNotify = nil
		}
		return messages
	}
	elect := func() {
		must(t, rn.Campaign())
		drain()
		must(t, rn.Step(raftpb.Message{Type: raftpb.MsgVoteResp, From: 2, To: 1, Term: rn.BasicStatus().Term}))
		drain()
		last, err := ms.LastIndex()
		must(t, err)
		must(t, rn.Step(raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: rn.BasicStatus().Term, Index: last}))
		drain()
		if !g.currentTermCommitted() {
			t.Fatal("leader barrier absent")
		}
	}
	count := func(messages []raftpb.Message) int {
		n := 0
		for _, m := range messages {
			if m.Type == raftpb.MsgTimeoutNow && m.To == 2 {
				n++
			}
		}
		return n
	}
	elect()
	if demotion {
		g.authority.DemotionTransferIndex, g.authority.DemotionTransferTerm = g.applied, rn.BasicStatus().Term
	}
	g.resumeAuthorizedTransfer()
	if count(drain()) != 1 {
		t.Fatal("retained intent did not initiate transfer", rn.Status(), g.applied, g.transferIssuedTerm, g.authorityReady(), g.pendingAdministrativeEntry(), receipt)
	}
	g.resumeAuthorizedTransfer()
	if count(drain()) != 0 {
		t.Fatal("intent initiated twice in one term")
	}
	for range 10 {
		rn.Tick()
	}
	drain()
	if rn.BasicStatus().LeadTransferee != 0 {
		t.Fatal("native transfer did not expire")
	}
	retryEntry := raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.TransferIntent, Correlation: "retry", Grant: &grant, Action: grant.Action}
	retry := &request{ctx: t.Context(), correlation: retryEntry.Correlation, admin: &retryEntry, reply: make(chan response, 1)}
	if demotion {
		retryEntry.Kind = raftstate.ManagedConfiguration
		retry.change = &raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 1}}}
		retry.expected = &cs
	}
	g.handleAuthority(retry)
	var retryResult response
	select {
	case retryResult = <-retry.reply:
	case <-testContext(t).Done():
		t.Fatal("exact retry did not resolve without another intent entry")
	}
	if demotion && !errors.Is(retryResult.err, fault.ErrNotLeader) || !demotion && retryResult.err != nil {
		t.Fatal("exact retry failed", retryResult.err)
	}
	g.resumeAuthorizedTransfer()
	if count(drain()) != 1 {
		t.Fatal("exact retry could not resume an aborted attempt in the same term")
	}
	must(t, rn.Step(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: rn.BasicStatus().Term + 1}))
	drain()
	elect()
	g.resumeAuthorizedTransfer()
	if count(drain()) != 1 {
		t.Fatal("replacement leader did not resume intent")
	}
	fence := grant.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	entry := raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.InstallAuthority, Correlation: "fence", Grant: &fence}
	req := &request{ctx: t.Context(), correlation: entry.Correlation, admin: &entry, reply: make(chan response, 1)}
	g.handleAuthority(req)
	drain()
	last, err := ms.LastIndex()
	must(t, err)
	must(t, rn.Step(raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: rn.BasicStatus().Term, Index: last}))
	drain()
	select {
	case reply := <-req.reply:
		must(t, reply.err)
	default:
		t.Fatal("active transfer prevented a newer fence from committing")
	}
	g.resumeAuthorizedTransfer()
	if count(drain()) != 0 || g.authority.Receipt != nil || g.authority.DemotionTransferIndex != 0 || rn.Status().LeadTransferee != 0 {
		t.Fatal("fenced intent resumed")
	}
}

func TestDemotionTransferIsDurablePrerequisiteNotCompletion(t *testing.T) {
	t.Parallel()
	ms := raft.NewMemoryStorage()
	cs := raftpb.ConfState{Voters: []uint64{1, 2}}
	must(t, ms.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}))
	must(t, ms.SetHardState(raftpb.HardState{Term: 1, Commit: 1}))
	rn, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: ms, Applied: 1, MaxInflightMsgs: 16, MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 1 << 20})
	must(t, err)
	grant := learnerAuthority(20, 2)
	grant.Action.Kind, grant.Action.NodeName, grant.Action.ReplicaID = topology.StepDemoteVoter, "1", 1
	grant.Action.TransferNodeName, grant.Action.TransferReplicaID = "other", 2
	grant.Action.ExpectedVotersDigest, grant.Action.ResultVotersDigest = topology.VoterSetDigest([]uint64{1, 2}), topology.VoterSetDigest([]uint64{2})
	ready := make(chan struct{})
	close(ready)
	g := &Group{rn: rn, ms: ms, cs: cs, applied: 1, ready: ready, opts: Options{ManagedAuthority: true, ReplicaID: 1, GroupID: "load", ClusterID: "cluster"}, changes: make(map[string]*request), authority: raftstate.AuthorityState{Grant: &grant, InstallIndex: 1, InstallTerm: 1}}
	// One RawNode with explicit peer responses exercises the queued owner path.
	drain := func() []raftpb.Message {
		var messages []raftpb.Message
		for rn.HasReady() {
			rd := rn.Ready()
			must(t, ms.Append(rd.Entries))
			if !raft.IsEmptyHardState(rd.HardState) {
				must(t, ms.SetHardState(rd.HardState))
				g.durableHS = rd.HardState
			}
			for _, entry := range rd.CommittedEntries {
				if len(entry.Data) != 0 {
					if entry.Type == raftpb.EntryConfChangeV2 {
						var cc raftpb.ConfChangeV2
						must(t, cc.Unmarshal(entry.Data))
						e, err := raftstate.DecodeEntry(cc.Context)
						must(t, err)
						must(t, g.applyAuthorizedConfChange(entry, cc, e))
					} else {
						e, err := raftstate.DecodeEntry(entry.Data)
						must(t, err)
						must(t, g.applyAuthority(entry, e))
					}
				}
				g.applied = entry.Index
			}
			messages = append(messages, rd.Messages...)
			rn.Advance(rd)
			for _, reply := range g.adminNotify {
				answer(reply.req, reply.result, reply.err)
			}
			g.adminNotify = nil
		}
		return messages
	}
	ack := func() {
		last, err := ms.LastIndex()
		must(t, err)
		must(t, rn.Step(raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: rn.BasicStatus().Term, Index: last}))
		drain()
	}
	must(t, rn.Campaign())
	drain()
	must(t, rn.Step(raftpb.Message{Type: raftpb.MsgVoteResp, From: 2, To: 1, Term: rn.BasicStatus().Term}))
	drain()
	ack()
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 1}}}
	e := raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.ManagedConfiguration, Correlation: "demote", Grant: &grant, Action: grant.Action}
	req := &request{ctx: t.Context(), correlation: e.Correlation, admin: &e, change: &cc, expected: &cs, reply: make(chan response, 1)}
	g.handleAuthority(req)
	drain()
	ack()
	select {
	case result := <-req.reply:
		if !errors.Is(result.err, fault.ErrNotLeader) {
			t.Fatal("prerequisite returned unexpected result", result.err)
		}
	default:
		t.Fatal("committed prerequisite did not resolve request")
	}
	if g.authority.Receipt != nil || g.authority.DemotionTransferIndex == 0 || !sameConfiguration(g.cs, cs) {
		t.Fatal("transfer intent became membership completion", g.authority, g.cs)
	}
	data, err := encodeSnapshot(g.opts, []byte("application"), g.authority)
	must(t, err)
	restored, err := raftstate.DecodeRaftSnapshot(raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: g.applied, Term: rn.BasicStatus().Term}}, "cluster", "load")
	must(t, err)
	g.authority = restored.Authority
	g.resumeAuthorizedTransfer()
	messages := drain()
	if !slices.ContainsFunc(messages, func(m raftpb.Message) bool { return m.Type == raftpb.MsgTimeoutNow && m.To == 2 }) {
		t.Fatal("restored demotion prerequisite did not initiate transfer", messages)
	}
	// The target becomes leader and commits the original exact demotion grant.
	e.Kind = raftstate.ManagedConfiguration
	cc.Context, err = raftstate.EncodeEntry(e)
	must(t, err)
	encoded, err := cc.Marshal()
	must(t, err)
	last, err := ms.LastIndex()
	must(t, err)
	term, err := ms.Term(last)
	must(t, err)
	nextTerm := rn.BasicStatus().Term + 1
	must(t, rn.Step(raftpb.Message{Type: raftpb.MsgApp, From: 2, To: 1, Term: nextTerm, Index: last, LogTerm: term, Commit: last + 1, Entries: []raftpb.Entry{{Type: raftpb.EntryConfChangeV2, Index: last + 1, Term: nextTerm, Data: encoded}}}))
	drain()
	if g.authority.Receipt == nil || g.authority.Receipt.Outcome != topology.EffectApplied || !slices.Equal(g.cs.Voters, []uint64{2}) || !slices.Equal(g.cs.Learners, []uint64{1}) || g.activeTransferTarget() != 0 {
		t.Fatal("exact demotion did not consume and stop prerequisite", g.authority, g.cs)
	}
	must(t, raftstate.ValidateAuthority(g.authority, g.applied))
}
