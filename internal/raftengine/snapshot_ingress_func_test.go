//go:build functional

package raftengine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/memstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type ingressCompletion struct {
	outcome SnapshotInstallOutcome
	err     error
}

type ingressStoreGate struct {
	storage.Store
	entered chan raftpb.Snapshot
	release chan struct{}
	ctxs    chan context.Context
}

func (s *ingressStoreGate) SaveSnapshot(ctx context.Context, id string, ss raftpb.Snapshot) error {
	s.ctxs <- ctx
	s.entered <- ss
	<-s.release // Deliberately ignores cancellation: ownership must wait for return.
	return s.Store.SaveSnapshot(ctx, id, ss)
}

type ingressRestoreGate struct {
	machine
	entered chan []byte
	release chan struct{}
}

func (m *ingressRestoreGate) Restore(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return m.machine.Restore(ctx, data)
	}
	m.entered <- data
	<-m.release
	return m.machine.Restore(ctx, data)
}

func ingressMemoryStore(t *testing.T) *memstore.Store {
	t.Helper()
	s, err := memstore.New(memstore.Options{NodeName: "2", ClusterID: "cluster"})
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	return s
}
func ingressGroup(t *testing.T, s storage.Store, m Machine) *Group {
	t.Helper()
	g, err := New(Options{GroupID: "load", NodeName: "2", ClusterID: "cluster", ReplicaID: 2, Mode: Joining,
		Store: s, Machine: m, Transport: &captureTransport{}, Timing: Timing{WALWriteWait: 10 * time.Millisecond},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(t, err)
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	return g
}
func ingressMessage(t *testing.T, g *Group, index uint64) SnapshotIngress {
	t.Helper()
	data, err := encodeSnapshot(g.opts, []byte(`{"commands":["restored"]}`), raftstate.AuthorityState{})
	must(t, err)
	m := raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: index, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}}
	decoded, err := g.PrepareOwnedSnapshot(t.Context(), m)
	must(t, err)
	return SnapshotIngress{Message: m, Decoded: decoded}
}
func ingressDone(in *SnapshotIngress) <-chan ingressCompletion {
	ch := make(chan ingressCompletion, 2)
	in.Done = func(o SnapshotInstallOutcome, err error) { ch <- ingressCompletion{o, err} }
	return ch
}
func expectIngressHeld(t *testing.T, ch <-chan ingressCompletion) {
	t.Helper()
	select {
	case result := <-ch:
		t.Fatalf("released early: %+v", result)
	default:
	}
}
func awaitIngressDone(t *testing.T, ch <-chan ingressCompletion) ingressCompletion {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-testContext(t).Done():
		t.Fatal("ingress did not complete")
		return ingressCompletion{}
	}
}
func closeIngressGate(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return release
}

func TestSnapshotIngress_InstalledAfterWriteAndRestore(t *testing.T) {
	t.Parallel()
	base := ingressMemoryStore(t)
	s := &ingressStoreGate{Store: base, entered: make(chan raftpb.Snapshot, 1), ctxs: make(chan context.Context, 1), release: make(chan struct{})}
	m := &ingressRestoreGate{entered: make(chan []byte, 1), release: make(chan struct{})}
	g := ingressGroup(t, s, m)
	releaseWrite := closeIngressGate(t, s.release)
	releaseRestore := closeIngressGate(t, m.release)
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	in := ingressMessage(t, g, 9)
	done := ingressDone(&in)
	must(t, g.StepOwnedSnapshot(testContext(t), in))
	var written raftpb.Snapshot
	select {
	case written = <-s.entered:
	case <-testContext(t).Done():
		t.Fatal("writer not reached")
	}
	if &written.Data[0] != &in.Message.Snapshot.Data[0] {
		t.Fatal("encoded buffer cloned")
	}
	if _, bounded := (<-s.ctxs).Deadline(); bounded {
		t.Fatal("snapshot persistence has per-attempt timeout")
	}
	busy := ingressMessage(t, g, 10)
	busyDone := ingressDone(&busy)
	if err := g.StepOwnedSnapshot(testContext(t), busy); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	if result := awaitIngressDone(t, busyDone); result.outcome != SnapshotInstallFailed {
		t.Fatal(result)
	}
	header := busy.Message
	ss := *header.Snapshot
	ss.Data = nil
	header.Snapshot = &ss
	if _, err := g.PreflightSnapshot(testContext(t), header); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	expectIngressHeld(t, done)
	releaseWrite()
	select {
	case data := <-m.entered:
		if &data[0] != &in.Decoded.Application[0] {
			t.Fatal("decoded state cloned")
		}
	case <-testContext(t).Done():
		t.Fatal("restore not reached")
	}
	expectIngressHeld(t, done)
	releaseRestore()
	result := awaitIngressDone(t, done)
	if result.err != nil || result.outcome != SnapshotInstalled {
		t.Fatal(result)
	}
	eventually(t, func() bool { return g.Status().Applied == 9 })
	stored, err := base.Load(t.Context(), "load")
	must(t, err)
	if !reflect.DeepEqual(stored.Snapshot, written) || !reflect.DeepEqual(m.values(), []string{"restored"}) {
		t.Fatal("state not durable/restored")
	}
	bounds := g.SnapshotBounds()
	if bounds.RunID == "" || bounds.Term < 2 || bounds.Commit < 9 {
		t.Fatal(bounds)
	}
	must(t, g.Stop(testContext(t)))
	expectIngressHeld(t, done)
}

func TestSnapshotIngress_StopJoinsBlockedWriter(t *testing.T) {
	t.Parallel()
	s := &ingressStoreGate{Store: ingressMemoryStore(t), entered: make(chan raftpb.Snapshot, 1), ctxs: make(chan context.Context, 1), release: make(chan struct{})}
	g := ingressGroup(t, s, &machine{})
	release := closeIngressGate(t, s.release)
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	in := ingressMessage(t, g, 9)
	done := ingressDone(&in)
	must(t, g.StepOwnedSnapshot(testContext(t), in))
	select {
	case <-s.entered:
	case <-testContext(t).Done():
		t.Fatal("write not reached")
	}
	g.RequestStop()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := g.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	expectIngressHeld(t, done)
	release()
	must(t, g.Stop(testContext(t)))
	result := awaitIngressDone(t, done)
	if result.outcome != SnapshotInstallFailed || result.err == nil {
		t.Fatal(result)
	}
	expectIngressHeld(t, done)
}

func TestSnapshotIngress_CanceledQueuedWaitRetainsOwnership(t *testing.T) {
	t.Parallel()
	for _, stop := range []bool{false, true} {
		t.Run(map[bool]string{false: "discard on loop", true: "discard after join"}[stop], func(t *testing.T) {
			t.Parallel()
			m := &ingressRestoreGate{entered: make(chan []byte, 1), release: make(chan struct{})}
			g := ingressGroup(t, ingressMemoryStore(t), m)
			release := closeIngressGate(t, m.release)
			must(t, g.Start(t.Context()))
			must(t, g.WaitReady(testContext(t)))
			first := ingressMessage(t, g, 9)
			firstDone := ingressDone(&first)
			must(t, g.StepOwnedSnapshot(testContext(t), first))
			select {
			case <-m.entered:
			case <-testContext(t).Done():
				t.Fatal("restore not reached")
			}
			second := ingressMessage(t, g, 10)
			done := ingressDone(&second)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			returned := make(chan error, 1)
			go func() { returned <- g.StepOwnedSnapshot(ctx, second) }()
			eventually(t, func() bool { return len(g.snapshotIngress) == 1 })
			cancel()
			if err := <-returned; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			expectIngressHeld(t, done)
			if stop {
				g.RequestStop()
			}
			expectIngressHeld(t, firstDone)
			release()
			result := awaitIngressDone(t, done)
			if result.outcome != SnapshotInstallFailed || result.err == nil {
				t.Fatal(result)
			}
			firstResult := awaitIngressDone(t, firstDone)
			if stop && firstResult.outcome != SnapshotInstallFailed {
				t.Fatal(firstResult)
			}
			if !stop && firstResult.outcome != SnapshotInstalled {
				t.Fatal(firstResult)
			}
			must(t, g.Stop(testContext(t)))
			expectIngressHeld(t, done)
			expectIngressHeld(t, firstDone)
		})
	}
}

func TestSnapshotIngress_PreflightZeroTermAndBoundaryCollision(t *testing.T) {
	t.Parallel()
	g := ingressGroup(t, ingressMemoryStore(t), &machine{})
	in := ingressMessage(t, g, 9)
	header := in.Message
	ss := *header.Snapshot
	ss.Data = nil
	header.Snapshot = &ss
	before := g.rn.BasicStatus()
	attack := header
	attack.Term = 0
	if _, err := g.PreflightSnapshot(t.Context(), attack); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, g.rn.BasicStatus()) {
		t.Fatal("zero-term mutated native state")
	}
	for _, gate := range []string{"timer", "ticket", "header ticket", "pending", "ready"} {
		t.Run(gate, func(t *testing.T) {
			state := loopState{}
			switch gate {
			case "timer":
				state.readyTimer = time.NewTimer(time.Hour)
				defer state.readyTimer.Stop()
			case "ticket":
				g.activeSnapshot = &ownedSnapshotTicket{}
				defer func() { g.activeSnapshot = nil }()
			case "header ticket":
				g.headerOnlyTicket = &request{}
				defer func() { g.headerOnlyTicket = nil }()
			case "pending":
				g.pending = &writeJob{}
				defer func() { g.pending = nil }()
			case "ready":
				must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 2}))
			}
			before := g.rn.BasicStatus()
			req := &request{ctx: t.Context(), messages: []raftpb.Message{header}, preflightReply: make(chan snapshotPreflightReply, 1)}
			g.handleSnapshotPreflight(req, &state)
			result := <-req.preflightReply
			if !errors.Is(result.err, fault.ErrOverloaded) || result.value.HeaderStepped {
				t.Fatal(result)
			}
			if !reflect.DeepEqual(before, g.rn.BasicStatus()) {
				t.Fatal("busy preflight stepped")
			}
		})
	}
}

// Drive the same single Ready consumer synchronously without starting workers.
func settleIngressReady(t *testing.T, g *Group) {
	t.Helper()
	for g.rn.HasReady() {
		must(t, g.beginReady())
		if g.pending != nil {
			job := <-g.writeC
			must(t, g.persistJob(t.Context(), job))
			must(t, g.finishWrite(job.kind))
		}
	}
}

func TestSnapshotIngress_PreflightNativeSkipAndImmediateCapture(t *testing.T) {
	t.Parallel()
	g := ingressGroup(t, ingressMemoryStore(t), &machine{})
	g.ctx = t.Context()
	in := ingressMessage(t, g, 9)
	done := ingressDone(&in)
	req := &request{ctx: t.Context(), ownedSnapshot: &ownedSnapshotTicket{ingress: in}, reply: make(chan response, 1)}
	state := loopState{lastEntriesWrite: time.Now()}
	g.handleOwnedSnapshot(req, &state)
	must(t, (<-req.reply).err)
	// A freshly stepped owned ticket bypasses WALWriteWait. No second Step can
	// become associated with its Ready during the batching timer window.
	must(t, g.prepareReady(&state))
	if state.readyTimer != nil || g.pending == nil || g.pending.ingress != req.ownedSnapshot {
		t.Fatal("ticket delayed/unbound")
	}
	job := <-g.writeC
	must(t, g.persistJob(t.Context(), job))
	must(t, g.finishWrite(job.kind))
	if result := awaitIngressDone(t, done); result.outcome != SnapshotInstalled {
		t.Fatal(result)
	}
	settleIngressReady(t, g)
	for _, term := range []uint64{1, 3} {
		header := in.Message
		ss := *header.Snapshot
		ss.Data = nil
		header.Snapshot = &ss
		header.Term = term
		req := &request{ctx: t.Context(), messages: []raftpb.Message{header}, preflightReply: make(chan snapshotPreflightReply, 1)}
		g.handleSnapshotPreflight(req, &loopState{})
		result := <-req.preflightReply
		if result.err != nil || !result.value.NotNeeded || !result.value.HeaderStepped {
			t.Fatal(result)
		}
		settleIngressReady(t, g)
		if g.headerOnlyTicket != nil {
			t.Fatal("header ticket leaked")
		}
	}
	if g.rn.BasicStatus().Term != 3 || g.rn.BasicStatus().Commit != 9 {
		t.Fatal(g.rn.BasicStatus())
	}
	stored, err := g.opts.Store.Load(t.Context(), "load")
	must(t, err)
	if stored.HardState.Term != 3 || len(stored.Snapshot.Data) == 0 {
		t.Fatal("higher term not durable or data lost")
	}
}

func TestSnapshotIngress_MatchingLogSkipsOfferedApplication(t *testing.T) {
	t.Parallel()
	g := ingressGroup(t, ingressMemoryStore(t), &machine{})
	g.ctx = t.Context()
	must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgApp, From: 1, To: 2, Term: 2,
		Entries: []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}}))
	settleIngressReady(t, g)
	// Snapshot index 2 matches the existing log; Raft fast-forwards commit and
	// emits normal Ready entries without selecting the offered application.
	in := ingressMessage(t, g, 2)
	done := ingressDone(&in)
	req := &request{ctx: t.Context(), ownedSnapshot: &ownedSnapshotTicket{ingress: in}, reply: make(chan response, 1)}
	g.handleOwnedSnapshot(req, &loopState{})
	must(t, (<-req.reply).err)
	must(t, g.beginReady())
	if result := awaitIngressDone(t, done); result.outcome != SnapshotSkipped || result.err != nil {
		t.Fatal(result)
	}
	if g.pending == nil || !raft.IsEmptySnap(g.pending.rd.Snapshot) {
		t.Fatal("fast-forward installed snapshot")
	}
	job := <-g.writeC
	must(t, g.persistJob(t.Context(), job))
	must(t, g.finishWrite(job.kind))
	if g.applied != 2 || len(g.opts.Machine.(*machine).values()) != 0 {
		t.Fatal("fast-forward lost native commit or restored offered data")
	}
}
