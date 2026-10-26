//go:build functional

package raftengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestSubmissionApplyAndBusinessRejection(t *testing.T) {
	t.Parallel()
	g, m := singleton(t, openStore(t, t.TempDir(), "1"))
	for _, data := range []string{"value", "reject"} {
		p := Proposal{RequestID: "logical/" + data, Data: []byte(data)}
		out, err := g.Submit(testContext(t), p, "attempt/"+data, 0)
		if err != nil || out.Disposition != SubmissionApplied || !out.ResultAvailable || out.Index == 0 || out.Term == 0 ||
			out.Identity != g.Identity() || out.RunID != g.RunID() || out.RequestID != p.RequestID || out.AttemptID != "attempt/"+data {
			t.Fatalf("applied receipt: %+v %v", out, err)
		}
		if data == "reject" && out.Result.Code != "rejected" {
			t.Fatal("business rejection became transport refusal", out)
		}
	}
	if got := m.values(); len(got) != 1 || got[0] != "value" {
		t.Fatal(got)
	}
}

func TestSubmissionPreDispatchRefusal(t *testing.T) {
	t.Parallel()
	g, m := singleton(t, openStore(t, t.TempDir(), "1"))
	for _, id := range []string{"", " \t", "invalid\x00id", string([]byte{0xff})} {
		out, err := g.Submit(testContext(t), Proposal{RequestID: id, Data: []byte("never")}, "attempt", 0)
		if !errors.Is(err, fault.ErrInvalid) || out.Disposition != SubmissionNotSubmitted || out.RequestID != id {
			t.Fatal(out, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	out, err := g.Submit(ctx, Proposal{RequestID: "canceled", Data: []byte("never")}, "attempt/canceled", 0)
	if !errors.Is(err, context.Canceled) || out.Disposition != SubmissionNotSubmitted || len(m.values()) != 0 {
		t.Fatal(out, err, m.values())
	}
}

func TestSubmissionApplyFailureStopsExactHandle(t *testing.T) {
	t.Parallel()
	g, _ := singleton(t, openStore(t, t.TempDir(), "1"))
	out, err := g.Submit(testContext(t), Proposal{RequestID: "fatal", Data: []byte("fatal")}, "attempt/fatal", 0)
	if err == nil || out.Disposition != SubmissionIndeterminate || out.ResultAvailable || out.Index != 0 {
		t.Fatal("failed Apply claimed completion", out, err)
	}
	must(t, g.Stop(testContext(t)))
	out, err = g.Submit(testContext(t), Proposal{RequestID: "next", Data: []byte("next")}, "attempt/next", 0)
	if !errors.Is(err, fault.ErrStopped) || !errors.Is(err, fault.ErrUnavailable) || out.Disposition != SubmissionNotSubmitted {
		t.Fatal("halted engine classified as transient", out, err)
	}
}

func TestSubmissionForwardingUsesOriginAppliedPosition(t *testing.T) {
	t.Parallel()
	g, _, capture := learnerForwardingFixture(t, time.Second)
	setForwardingLeader(t, g)
	type outcome struct {
		s   Submission
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		s, err := g.Submit(testContext(t), Proposal{RequestID: "logical", Data: []byte("value")}, "attempt", 0)
		done <- outcome{s: s, err: err}
	}()
	forward := capturedProposal(t, capture)
	e, err := raftstate.DecodeEntry(forward.Entries[0].Data)
	must(t, err)
	if e.Correlation != "attempt" || e.RequestID != "logical" {
		t.Fatal(e)
	}
	select {
	case early := <-done:
		t.Fatal("answered before local Apply", early)
	default:
	}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 2, Term: 2, Data: forward.Entries[0].Data}
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgApp, From: 1, To: 2,
		Term: 2, Index: 1, LogTerm: 1, Commit: 2, Entries: []raftpb.Entry{entry}}))
	select {
	case out := <-done:
		if out.err != nil || out.s.Disposition != SubmissionApplied || out.s.Index != 2 || out.s.Term != 2 ||
			out.s.Identity.ReplicaID != 2 || out.s.Result.Code != "ok" || !out.s.ResultAvailable {
			t.Fatal(out)
		}
	case <-testContext(t).Done():
		t.Fatal("no applied receipt")
	}
}

func TestSubmissionForwardedUnknownAndSnapshot(t *testing.T) {
	t.Parallel()
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprint(snapshot), func(t *testing.T) {
			t.Parallel()
			g, _, capture := learnerForwardingFixture(t, time.Second)
			setForwardingLeader(t, g)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type outcome struct {
				s   Submission
				err error
			}
			done := make(chan outcome, 1)
			go func() {
				s, err := g.Submit(ctx, Proposal{RequestID: "logical", Data: []byte("value")}, "attempt", 0)
				done <- outcome{s: s, err: err}
			}()
			_ = capturedProposal(t, capture)
			if snapshot {
				ss := raftpb.Snapshot{Data: []byte(`{"commands":["restored"]}`),
					Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 2,
						ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}
				data, err := encodeSnapshot(g.opts, ss.Data, g.authority)
				must(t, err)
				ss.Data = data
				must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &ss}))
			} else {
				cancel()
			}
			select {
			case out := <-done:
				if out.err == nil || out.s.Disposition != SubmissionIndeterminate || out.s.Index != 0 || out.s.ResultAvailable {
					t.Fatal("uncertain wait fabricated result", out)
				}
			case <-testContext(t).Done():
				t.Fatal("unbounded uncertain wait")
			}
			select {
			case extra := <-capture.proposals:
				t.Fatal("unknown forwarding automatically retried", extra)
			default:
			}
		})
	}
}

func TestSubmissionNativeDropReturnsProofWithoutForwarding(t *testing.T) {
	t.Parallel()
	g, m, capture := learnerForwardingFixture(t, time.Second)
	s, err := g.Submit(testContext(t), Proposal{RequestID: "logical", Data: []byte("no leader")}, "attempt", 0)
	if !errors.Is(err, raft.ErrProposalDropped) || s.Disposition != SubmissionNotSubmitted || len(m.values()) != 0 {
		t.Fatal(s, err)
	}
	select {
	case extra := <-capture.proposals:
		t.Fatal("forwarded without a leader", extra)
	default:
	}
}

func TestSubmissionAttributionMismatchDoesNotHaltValidApply(t *testing.T) {
	t.Parallel()
	m := &machine{}
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	g := newTestGroup(t, openStore(t, t.TempDir(), "1"), m, n, 1, []uint64{1}, Founding)
	// Drive one owner synchronously. No second participant or engine workers.
	req := &request{correlation: "collision", reportSubmission: true,
		proposal: Proposal{RequestID: "expected", Data: []byte("expected")}, reply: make(chan response, 1)}
	g.props[req.correlation] = req
	data, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application,
		Correlation: "collision", RequestID: "other", Data: []byte("valid command")})
	must(t, err)
	must(t, g.applyNormalEntry(raftpb.Entry{Type: raftpb.EntryNormal, Index: 2, Term: 1, Data: data}))
	response := <-req.reply
	if !errors.Is(response.err, fault.ErrIdentityMismatch) || response.disposition != SubmissionIndeterminate ||
		len(g.props) != 0 || len(m.values()) != 1 || m.values()[0] != "valid command" {
		t.Fatal(response, g.props, m.values())
	}
	select {
	case second := <-req.reply:
		t.Fatal("mismatched waiter received a second result", second)
	default:
	}
}

func TestSubmissionCancellationKeepsApplyOwnership(t *testing.T) {
	t.Parallel()
	m := &metricsBlockedMachine{entered: make(chan struct{}), release: make(chan struct{})}
	g := metricsSingleton(t, openStore(t, t.TempDir(), "1"), m)
	release := sync.OnceFunc(func() { close(m.release) })
	t.Cleanup(release)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type outcome struct {
		s   Submission
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		s, err := g.Submit(ctx, Proposal{RequestID: "logical", Data: []byte("late")}, "attempt", 0)
		done <- outcome{s: s, err: err}
	}()
	select {
	case <-m.entered:
	case <-testContext(t).Done():
		t.Fatal("Apply never entered")
	}
	cancel()
	out := <-done
	if !errors.Is(out.err, context.Canceled) || out.s.Disposition != SubmissionIndeterminate {
		t.Fatal(out)
	}
	release()
	eventually(t, func() bool { return len(m.values()) == 1 })
	if m.values()[0] != "late" || out.s.Disposition != SubmissionIndeterminate {
		t.Fatal("cancellation changed committed work or returned evidence", m.values(), out)
	}
}

func TestSubmissionCountsOnceAndDoesNotDeduplicate(t *testing.T) {
	t.Parallel()
	m := &machine{}
	g := metricsSingleton(t, openStore(t, t.TempDir(), "1"), m)
	p := Proposal{RequestID: "same-logical", Data: []byte("same")}
	first, err := g.Submit(testContext(t), p, "attempt/1", 0)
	must(t, err)
	second, err := g.Submit(testContext(t), p, "attempt/2", 0)
	must(t, err)
	_, err = g.Propose(testContext(t), Proposal{RequestID: "legacy", Data: []byte("legacy")})
	must(t, err)
	if first.Index == second.Index || len(m.values()) != 3 {
		t.Fatal(first, second, m.values())
	}
	eventually(t, func() bool { return g.Metrics().Sample.Counters.Results >= 3 })
	counts := g.Metrics().Sample.Counters
	if counts.Proposals != 3 || counts.Results != 3 {
		t.Fatal("double-counted proposals/results", counts)
	}
}

func TestSubmissionEncodedSizeRefusalKeepsLegacyClassification(t *testing.T) {
	t.Parallel()
	m := &machine{}
	g, err := New(Options{GroupID: "load", NodeName: "1", ClusterID: "cluster", ReplicaID: 1,
		Mode: Founding, Peers: []raft.Peer{{ID: 1}}, Store: openStore(t, t.TempDir(), "1"),
		Machine: m, Transport: endpoint{network: &network{}}, MaxUncommittedBytes: 1024,
		Timing: Timing{TickInterval: 10 * time.Millisecond}})
	must(t, err)
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	eventually(t, g.IsLeader)
	p := Proposal{RequestID: "oversized", Data: bytes.Repeat([]byte("x"), 2048)}
	s, err := g.Submit(testContext(t), p, "attempt", 0)
	if !errors.Is(err, fault.ErrInvalid) || s.Disposition != SubmissionNotSubmitted || len(m.values()) != 0 {
		t.Fatal(s, err)
	}
	if _, err := g.Propose(testContext(t), p); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
}
