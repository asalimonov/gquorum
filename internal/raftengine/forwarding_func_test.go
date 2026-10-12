//go:build functional

package raftengine

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"io"
	"log/slog"
	"testing"
	"time"
)

// A protocol double captures forwarding without starting a second replica.
// Multi-node consensus coverage belongs to the Docker process suite.
type proposalCapture struct{ proposals chan raftpb.Message }

func (c proposalCapture) Send(ctx context.Context, messages []raftpb.Message) error {
	for _, m := range messages {
		if m.Type == raftpb.MsgProp {
			select {
			case c.proposals <- m:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}
func learnerForwardingFixture(t *testing.T, timeout time.Duration) (*Group, *machine, proposalCapture) {
	t.Helper()
	m := &machine{}
	capture := proposalCapture{proposals: make(chan raftpb.Message, 4)}
	g, err := New(Options{GroupID: "load", NodeName: "2", ClusterID: "cluster", ReplicaID: 2, Mode: Founding, Peers: []raft.Peer{{ID: 1}}, FoundingLearners: []raft.Peer{{ID: 2}}, Store: openStore(t, t.TempDir(), "2"), Machine: m, Transport: capture, Timing: Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 200}, ProposalTimeout: timeout, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(t, err)
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	return g, m, capture
}
func setForwardingLeader(t *testing.T, g *Group) {
	t.Helper()
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 2, Commit: 1}))
	eventually(t, func() bool { return g.Status().LeaderID == 1 })
}
func capturedProposal(t *testing.T, c proposalCapture) raftpb.Message {
	t.Helper()
	select {
	case m := <-c.proposals:
		return m
	case <-testContext(t).Done():
		t.Fatal("proposal not forwarded")
		return raftpb.Message{}
	}
}

func TestLearnerForwardsAndWaitsForLocalAppliedResult(t *testing.T) {
	t.Parallel()
	g, m, c := learnerForwardingFixture(t, time.Second)
	setForwardingLeader(t, g)
	done := make(chan response, 1)
	go func() {
		r, e := g.Propose(testContext(t), Proposal{RequestID: "stable-request", Data: []byte("value")})
		done <- response{r, e}
	}()
	forward := capturedProposal(t, c)
	if forward.From != 2 || forward.To != 1 || forward.Type != raftpb.MsgProp {
		t.Fatal(forward)
	}
	var command envelope
	must(t, json.Unmarshal(forward.Entries[0].Data, &command))
	if command.RequestID != "stable-request" || command.Correlation == "" {
		t.Fatal(command)
	}
	select {
	case early := <-done:
		t.Fatal("answered before local apply", early)
	default:
	}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 2, Term: 2, Data: forward.Entries[0].Data}
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgApp, From: 1, To: 2, Term: 2, Index: 1, LogTerm: 1, Commit: 2, Entries: []raftpb.Entry{entry}}))
	select {
	case r := <-done:
		if r.err != nil || r.result.Code != "ok" || string(r.result.Data) != "1" {
			t.Fatal(r)
		}
	case <-testContext(t).Done():
		t.Fatal("local apply failed to answer origin")
	}
	if len(m.values()) != 1 || g.IsLeader() {
		t.Fatal("forwarding changed membership/leadership or lost result")
	}
}

func TestForwardedWaitIsBoundedAndNeverAmbiguouslyRetried(t *testing.T) {
	t.Parallel()
	g, _, c := learnerForwardingFixture(t, 80*time.Millisecond)
	setForwardingLeader(t, g)
	done := make(chan error, 1)
	go func() { _, err := g.Propose(context.Background(), Proposal{Data: []byte("uncertain")}); done <- err }()
	_ = capturedProposal(t, c)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-testContext(t).Done():
		t.Fatal("proposal timeout ignored")
	}
	select {
	case m := <-c.proposals:
		t.Fatal("ambiguous proposal automatically retried", m)
	default:
	}
	eventually(t, func() bool { return g.ProposalStats().AwaitingCommit == 0 && g.ProposalStats().InFlight == 0 })
}

func TestUnknownLeaderRetryIsBoundedAndOwned(t *testing.T) {
	t.Parallel()
	g, _, c := learnerForwardingFixture(t, time.Second)
	_, err := g.Propose(testContext(t), Proposal{RequestID: "one", Data: []byte("no leader")})
	if !errors.Is(err, fault.ErrUnavailable) || !errors.Is(err, raft.ErrProposalDropped) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return g.ProposalStats().AwaitingCommit == 0 && g.ProposalStats().InFlight == 0 })
	select {
	case m := <-c.proposals:
		t.Fatal("forwarded without a leader", m)
	default:
	}
}

func TestForwardingSnapshotReturnsUncertainOutcome(t *testing.T) {
	t.Parallel()
	g, m, c := learnerForwardingFixture(t, time.Second)
	setForwardingLeader(t, g)
	done := make(chan error, 1)
	go func() {
		_, err := g.Propose(testContext(t), Proposal{RequestID: "pending", Data: []byte("value")})
		done <- err
	}()
	_ = capturedProposal(t, c)
	ss := raftpb.Snapshot{Data: []byte(`{"commands":["from snapshot"]}`), Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &ss}))
	select {
	case err := <-done:
		if !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal(err)
		}
	case <-testContext(t).Done():
		t.Fatal("snapshot fabricated or stranded result")
	}
	eventually(t, func() bool { return g.Status().Applied >= 4 })
	if got := m.values(); len(got) != 1 || got[0] != "from snapshot" {
		t.Fatal(got)
	}
}

func TestStaleDestinationDoesNotRelayAnotherSender(t *testing.T) {
	t.Parallel()
	g, _, c := learnerForwardingFixture(t, time.Second)
	setForwardingLeader(t, g)
	data, _ := json.Marshal(envelope{Schema: 1, Correlation: "peer", RequestID: "request", Data: []byte("x")})
	err := g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgProp, From: 1, To: 2, Entries: []raftpb.Entry{{Type: raftpb.EntryNormal, Data: data}}})
	if !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	select {
	case m := <-c.proposals:
		t.Fatal("relayed under foreign identity", m)
	default:
	}
}

func TestLeaderAcceptsOrdinaryProposalOnlyFromNativeMember(t *testing.T) {
	t.Parallel()
	g, m := singleton(t, openStore(t, t.TempDir(), "1"))
	must(t, g.ProposeConfChange(testContext(t), raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}}}))
	data, _ := json.Marshal(envelope{Schema: 1, Correlation: "peer-1", RequestID: "application-request", Data: []byte("forwarded")})
	msg := raftpb.Message{Type: raftpb.MsgProp, From: 3, To: 1, Entries: []raftpb.Entry{{Type: raftpb.EntryNormal, Data: data}}}
	if err := g.Step(testContext(t), msg); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("unknown native sender accepted", err)
	}
	msg.From = 2
	must(t, g.Step(testContext(t), msg))
	eventually(t, func() bool { return len(m.values()) == 1 })
	if got := m.values(); got[0] != "forwarded" {
		t.Fatal(got)
	}
	if stats := g.ProposalStats(); stats.AwaitingCommit != 0 || stats.InFlight != 0 {
		t.Fatal("leader created a foreign result waiter", stats)
	}
}
