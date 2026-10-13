//go:build functional

package raftengine

import (
	"context"
	"errors"
	"fmt"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type queryMachine struct {
	machine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*queryMachine) SupportsQuery() bool { return true }
func (m *queryMachine) Query(_ context.Context, query []byte) (Result, error) {
	if string(query) == "error" {
		return Result{}, errors.New("query rejected")
	}
	if string(query) == "large" {
		return Result{Data: make([]byte, 64)}, nil
	}
	if m.entered != nil {
		m.once.Do(func() { close(m.entered) })
		<-m.release
	}
	return Result{Code: "ok", Data: []byte(fmt.Sprint(len(m.values())))}, nil
}

type readCapture struct{ requests chan raftpb.Message }

func (c readCapture) Send(ctx context.Context, messages []raftpb.Message) error {
	for _, msg := range messages {
		if msg.Type == raftpb.MsgReadIndex {
			select {
			case c.requests <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}
func readFixture(t *testing.T, m Machine, options ReadOptions, learner bool) (*Group, readCapture) {
	t.Helper()
	capture := readCapture{requests: make(chan raftpb.Message, 16)}
	id := uint64(1)
	var learners []raft.Peer
	if learner {
		id = 2
		learners = []raft.Peer{{ID: 2}}
	}
	g, err := New(Options{GroupID: "load", NodeName: fmt.Sprint(id), ClusterID: "cluster", ReplicaID: id, Mode: Founding,
		Peers: []raft.Peer{{ID: 1}}, FoundingLearners: learners, Reads: options,
		Store: openStore(t, t.TempDir(), fmt.Sprint(id)), Machine: m, Transport: capture,
		Timing: Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 50}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(t, err)
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	if learner {
		setForwardingLeader(t, g)
	} else {
		eventually(t, g.IsLeader)
	}
	return g, capture
}
func capturedRead(t *testing.T, c readCapture) raftpb.Message {
	t.Helper()
	select {
	case msg := <-c.requests:
		return msg
	case <-testContext(t).Done():
		t.Fatal("read round not forwarded")
		return raftpb.Message{}
	}
}
func answerRead(t *testing.T, g *Group, msg raftpb.Message, index uint64) {
	t.Helper()
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgReadIndexResp, From: 1, To: 2, Term: 2, Index: index, Entries: msg.Entries}))
}

func TestConsistentReadObservesAppliedStateAndQueryErrorsDoNotHalt(t *testing.T) {
	t.Parallel()
	m := &queryMachine{}
	g, _ := readFixture(t, m, ReadOptions{MaxRequestBytes: 8, MaxResultBytes: 8}, false)
	must(t, g.ReadBarrier(testContext(t)))
	propose(t, g, "one")
	result, err := g.Read(testContext(t), []byte("count"))
	must(t, err)
	if result.Code != "ok" || string(result.Data) != "1" {
		t.Fatal(result)
	}
	if _, err := g.Read(testContext(t), []byte("error")); err == nil {
		t.Fatal("query error disappeared")
	}
	if _, err := g.Read(testContext(t), []byte("large")); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	if _, err := g.Read(testContext(t), make([]byte, 9)); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	if !g.Running() || g.Error() != nil {
		t.Fatal("query error halted replica", g.Error())
	}
	propose(t, g, "two")
	result, err = g.Read(testContext(t), nil)
	must(t, err)
	if string(result.Data) != "2" {
		t.Fatal(result)
	}
}

func TestCanceledQueryRemainsOwnedUntilReturn(t *testing.T) {
	t.Parallel()
	m := &queryMachine{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(m.release) })
	g, _ := readFixture(t, m, ReadOptions{MaxPending: 1}, false)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := g.Read(ctx, nil); done <- err }()
	select {
	case <-m.entered:
	case <-testContext(t).Done():
		t.Fatal("query not entered")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := g.ReadBarrier(testContext(t)); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("callback lost its admission slot", err)
	}
	stop, stopCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stopCancel()
	if err := g.Stop(stop); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("stop did not join query", err)
	}
	release.Do(func() { close(m.release) })
	must(t, g.Stop(testContext(t)))
	if len(g.readSlots) != 0 {
		t.Fatal("completed callback leaked slot")
	}
}

func TestLearnerReadWaitsForApplyAndNewReadNeedsNewRound(t *testing.T) {
	t.Parallel()
	g, c := readFixture(t, &queryMachine{}, ReadOptions{}, true)
	first := make(chan response, 1)
	go func() { r, err := g.Read(testContext(t), nil); first <- response{r, err} }()
	round := capturedRead(t, c)
	if round.From != 2 || round.To != 1 || len(round.Entries[0].Data) != readContextSize {
		t.Fatal(round)
	}
	answerRead(t, g, round, 2)
	select {
	case r := <-first:
		t.Fatal("answered before local application", r)
	default:
	}
	data, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application, Correlation: "remote-write", RequestID: "write", Data: []byte("one")})
	must(t, err)
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgApp, From: 1, To: 2, Term: 2, Index: 1, LogTerm: 1, Commit: 2, Entries: []raftpb.Entry{{Index: 2, Term: 2, Data: data}}}))
	select {
	case r := <-first:
		if r.err != nil || string(r.result.Data) != "1" {
			t.Fatal(r)
		}
	case <-testContext(t).Done():
		t.Fatal("applied read did not finish")
	}
	second := make(chan error, 1)
	go func() { second <- g.ReadBarrier(testContext(t)) }()
	next := capturedRead(t, c)
	if string(next.Entries[0].Data) == string(round.Entries[0].Data) {
		t.Fatal("new read reused an earlier confirmation")
	}
	answerRead(t, g, next, 2)
	must(t, <-second)
}

func TestCanceledReadRoundsStayBoundedAndRecoverAfterLostReply(t *testing.T) {
	t.Parallel()
	g, c := readFixture(t, &queryMachine{}, ReadOptions{MaxPending: 2}, true)
	firstCtx, cancel := context.WithCancel(testContext(t))
	first := make(chan error, 1)
	go func() { first <- g.ReadBarrier(firstCtx) }()
	round := capturedRead(t, c)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// New caller timeouts cannot allocate more upstream rounds while this one is unresolved.
	for range 8 {
		ctx, done := context.WithTimeout(context.Background(), 15*time.Millisecond)
		err := g.ReadBarrier(ctx)
		done()
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, fault.ErrOverloaded) {
			t.Fatal(err)
		}
	}
	retry := capturedRead(t, c) // same round retries after an election timeout
	if string(retry.Entries[0].Data) != string(round.Entries[0].Data) {
		t.Fatal("cancellation created another upstream round")
	}
	eventually(t, func() bool { return g.Metrics().Sample.ReadQueue.LocalRounds == 1 })
	queue := g.Metrics().Sample.ReadQueue
	if queue.Limit != 2 || queue.InFlight > queue.Limit || queue.Queued > queue.Limit || queue.AwaitingQuorum > queue.Limit || queue.RemoteRounds > queue.Limit {
		t.Fatal("read diagnostics lost bounded owner state", queue)
	}
	answerRead(t, g, retry, 1)
	last := make(chan error, 1)
	lastCtx := testContext(t)
	go func() { last <- g.ReadBarrier(lastCtx) }()
	var next raftpb.Message
fresh:
	for {
		select {
		case next = <-c.requests:
			// The metrics sample wait allows retransmissions already owned by
			// the send worker to remain queued. A new read must still produce
			// a new context and cannot finish from any of those older replies.
			if string(next.Entries[0].Data) != string(round.Entries[0].Data) {
				break fresh
			}
		case err := <-last:
			t.Fatal("fresh read completed without its own confirmation", err)
		case <-lastCtx.Done():
			t.Fatal("fresh read attached to old round", lastCtx.Err())
		}
	}
	answerRead(t, g, next, 1)
	must(t, <-last)
}

func TestReadCohortIsFrozenAndLeaderChangeFailsWaiters(t *testing.T) {
	t.Parallel()
	g, c := readFixture(t, &queryMachine{}, ReadOptions{}, true)
	first := make(chan error, 1)
	go func() { first <- g.ReadBarrier(testContext(t)) }()
	round := capturedRead(t, c)
	second := make(chan error, 1)
	go func() { second <- g.ReadBarrier(testContext(t)) }()
	// A reply to the earlier round cannot cover a read submitted afterward.
	answerRead(t, g, round, 1)
	must(t, <-first)
	next := capturedRead(t, c)
	if string(next.Entries[0].Data) == string(round.Entries[0].Data) {
		t.Fatal("later arrival joined frozen cohort")
	}
	select {
	case err := <-second:
		t.Fatal("later read completed from earlier confirmation", err)
	default:
	}
	// A stale term or a context from the previous round cannot complete it.
	if err := g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgReadIndexResp, From: 1, To: 2, Term: 1, Index: 1, Entries: next.Entries}); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	if err := g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgReadIndexResp, From: 1, To: 2, Term: 2, Index: 1, Entries: round.Entries}); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 3, Commit: 1}))
	if err := <-second; !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("old-term waiter survived leadership reset", err)
	}
}

func TestWriteOnlyMachineSupportsBarrier(t *testing.T) {
	t.Parallel()
	g, _ := readFixture(t, &machine{}, ReadOptions{}, false)
	must(t, g.ReadBarrier(testContext(t)))
	if _, err := g.Read(testContext(t), nil); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestRemoteReadAdmissionBeforeCurrentTermCommit(t *testing.T) {
	t.Parallel()
	ms := raft.NewMemoryStorage()
	cs := raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3, 4}}
	must(t, ms.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}))
	must(t, ms.SetHardState(raftpb.HardState{Term: 1, Commit: 1}))
	rn, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: ms, Applied: 1, MaxInflightMsgs: 16, ReadOnlyOption: raft.ReadOnlySafe})
	must(t, err)
	must(t, rn.Campaign())
	rd := rn.Ready()
	must(t, ms.Append(rd.Entries))
	if !raft.IsEmptyHardState(rd.HardState) {
		must(t, ms.SetHardState(rd.HardState))
	}
	rn.Advance(rd) // release the durable self vote before the remote vote
	must(t, rn.Step(raftpb.Message{Type: raftpb.MsgVoteResp, From: 2, To: 1, Term: rn.BasicStatus().Term}))
	if rn.BasicStatus().RaftState != raft.StateLeader {
		t.Fatal("election did not finish")
	}
	g := &Group{rn: rn, ms: ms, cs: cs, opts: Options{ReplicaID: 1, Reads: ReadOptions{MaxPending: 1}}}
	g.resetReadRounds()
	if g.currentTermCommitted() {
		t.Fatal("fixture unexpectedly committed current term")
	}
	source := &Group{nonce: "00000000000000000000000000000001", opts: Options{ReplicaID: 3}}
	msg := raftpb.Message{Type: raftpb.MsgReadIndex, From: 3, To: 1, Entries: []raftpb.Entry{{Data: source.newReadContext()}}}
	for range 100 {
		must(t, g.validateRemoteRead(msg))
	}
	if len(g.readState.remote) != 1 || g.readState.remote[3].submitted {
		t.Fatal("duplicate entered Raft before current-term commit")
	}
	different := msg
	different.Entries = []raftpb.Entry{{Data: source.newReadContext()}}
	if err := g.validateRemoteRead(different); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("source allocated a second upstream round", err)
	}
	source.opts.ReplicaID = 4
	different.From = 4
	different.Entries = []raftpb.Entry{{Data: source.newReadContext()}}
	if err := g.validateRemoteRead(different); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("global round bound not enforced", err)
	}
	different.From = 9
	if err := g.validateRemoteRead(different); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("source/context mismatch accepted", err)
	}
	must(t, rn.Step(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: rn.BasicStatus().Term + 1}))
	g.resetReadRounds()
	if len(g.readState.remote) != 0 {
		t.Fatal("leadership reset retained obsolete source slots")
	}
}
