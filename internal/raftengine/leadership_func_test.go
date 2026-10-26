//go:build functional

package raftengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3/raftpb"
)

func TestScopedProposalChecksTermOnOwnerLoop(t *testing.T) {
	t.Parallel()
	store := openStore(t, t.TempDir(), "1")
	g, m := singleton(t, store)
	term := g.Status().Term
	if _, err := g.ProposeInTerm(testContext(t), term-1, Proposal{Data: []byte("stale")}); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	result, err := g.ProposeInTerm(testContext(t), term, Proposal{Data: []byte("current")})
	must(t, err)
	if result.Code != "ok" || len(m.values()) != 1 || m.values()[0] != "current" {
		t.Fatal(result, m.values())
	}
}

func TestScopedDroppedRetryNeverForwardsAfterLeadershipLoss(t *testing.T) {
	t.Parallel()
	store := openStore(t, t.TempDir(), "1")
	m := &machine{}
	network := &network{nodes: map[uint64]*Group{}, disconnected: map[uint64]bool{}}
	g := newTestGroup(t, store, m, network, 1, []uint64{1}, Founding)
	// Drive one owner synchronously as a protocol fixture. No engine worker is
	// started; the queued retry simulates a prior definitive ErrProposalDropped.
	must(t, g.rn.Campaign())
	term := g.rn.BasicStatus().Term
	close(g.ready)
	req := &request{ctx: context.Background(), correlation: "retry", expectedTerm: term, reportSubmission: true,
		proposal: Proposal{RequestID: "scoped", Data: []byte("must not forward")}, reply: make(chan response, 1), retried: true, retryAt: time.Now()}
	g.props[req.correlation] = req
	must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: term + 1}))
	g.retryDroppedProposals(time.Now().Add(time.Second))
	result := <-req.reply
	if !errors.Is(result.err, fault.ErrNotLeader) || result.disposition != SubmissionNotSubmitted || len(g.props) != 0 {
		t.Fatal(result, len(g.props))
	}
	for _, msg := range g.rn.Ready().Messages {
		if msg.Type == raftpb.MsgProp {
			t.Fatal("guarded retry forwarded", msg)
		}
	}
}
