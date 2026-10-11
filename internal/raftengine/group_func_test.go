//go:build functional

package raftengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type machine struct {
	mu       sync.Mutex
	commands []string
}

func (m *machine) Apply(ctx context.Context, c Command) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if string(c.Data) == "fatal" {
		return Result{}, errors.New("machine failure")
	}
	if string(c.Data) == "reject" {
		return Result{Code: "rejected"}, nil
	}
	m.commands = append(m.commands, string(c.Data))
	return Result{Code: "ok", Data: []byte(fmt.Sprint(len(m.commands)))}, nil
}

func (m *machine) Snapshot(context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Marshal(struct {
		Commands []string `json:"commands"`
	}{m.commands})
}

func (m *machine) Restore(ctx context.Context, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = nil
	if len(data) == 0 {
		return nil
	}
	var state struct {
		Commands []string `json:"commands"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	m.commands = state.Commands
	return nil
}

func (m *machine) values() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.commands...)
}

type network struct {
	mu           sync.RWMutex
	nodes        map[uint64]*Group
	disconnected map[uint64]bool
}

type endpoint struct{ network *network }

func (e endpoint) Send(ctx context.Context, messages []raftpb.Message) error {
	for _, msg := range messages {
		e.network.mu.RLock()
		target := e.network.nodes[msg.To]
		blocked := e.network.disconnected[msg.To] || e.network.disconnected[msg.From]
		e.network.mu.RUnlock()
		if target == nil || blocked {
			continue
		}
		if err := target.Step(ctx, msg); err != nil && !errors.Is(err, fault.ErrStopped) {
			return err
		}
		if msg.Type == raftpb.MsgSnap {
			e.network.mu.RLock()
			sender := e.network.nodes[msg.From]
			e.network.mu.RUnlock()
			if sender != nil {
				sender.ReportSnapshot(msg.To, raft.SnapshotFinish)
			}
		}
	}
	return nil
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%v (cause: %v)", err, errors.Unwrap(err))
	}
}

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	ctx := testContext(t)
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal("condition did not converge")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func openStore(t *testing.T, root, node string) *walstore.Store {
	t.Helper()
	prewrite := false
	s, err := walstore.Open(walstore.Options{RootDir: root, NodeName: node, ClusterID: "cluster", SegmentBytes: 32 << 10, Prewrite: &prewrite})
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	return s
}

func newTestGroup(t *testing.T, s storage.Store, m *machine, n *network, id uint64, peers []uint64, mode Mode, waits ...time.Duration) *Group {
	t.Helper()
	var initial []raft.Peer
	for _, id := range peers {
		initial = append(initial, raft.Peer{ID: id})
	}
	var wait time.Duration
	if len(waits) != 0 {
		wait = waits[0]
	}
	g, err := New(Options{GroupID: "load", NodeName: fmt.Sprint(id), ClusterID: "cluster", ReplicaID: id, Mode: mode, Peers: initial, Timing: Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 50, HeartbeatTicks: 1, WALWriteWait: wait}, Store: s, Machine: m, Transport: endpoint{n}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(t, err)
	n.mu.Lock()
	n.nodes[id] = g
	n.mu.Unlock()
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	return g
}

func singleton(t *testing.T, s storage.Store) (*Group, *machine) {
	t.Helper()
	m := &machine{}
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	g := newTestGroup(t, s, m, n, 1, []uint64{1}, Founding)
	must(t, g.Start(context.Background()))
	must(t, g.WaitReady(testContext(t)))
	eventually(t, g.IsLeader)
	return g, m
}

func propose(t *testing.T, g *Group, data string) Result {
	t.Helper()
	result, err := g.Propose(testContext(t), Proposal{Data: []byte(data)})
	must(t, err)
	return result
}

func TestFoundingStorage_SeedHasFullMachineAndVoters(t *testing.T) {
	s := openStore(t, t.TempDir(), "1")
	g, _ := singleton(t, s)
	recovery, err := s.Load(testContext(t), "load")
	must(t, err)
	if len(recovery.Snapshot.Data) == 0 || !reflect.DeepEqual(recovery.Snapshot.Metadata.ConfState.Voters, []uint64{1}) || g.Status().Applied < 1 {
		t.Fatalf("founding snapshot: %+v", recovery.Snapshot)
	}
}

func TestLoad_ProposeSnapshotRestartReplaysCommittedTail(t *testing.T) {
	root := t.TempDir()
	s := openStore(t, root, "1")
	g, m := singleton(t, s)
	propose(t, g, "a")
	must(t, g.Snapshot(testContext(t)))
	propose(t, g, "b")
	propose(t, g, "c")
	if !reflect.DeepEqual(m.values(), []string{"a", "b", "c"}) {
		t.Fatal("apply result")
	}
	// Persist an advisory index that is ahead of the last complete snapshot.
	status := g.Status()
	eventually(t, func() bool { return g.Status().Applied >= status.Commit })
	must(t, s.SaveApplied(testContext(t), "load", g.Status().Applied, g.Status().ConfState))
	must(t, g.Stop(testContext(t)))
	must(t, s.Close())
	s = openStore(t, root, "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	restored := &machine{}
	g = newTestGroup(t, s, restored, n, 1, nil, Joining)
	must(t, g.Start(context.Background()))
	must(t, g.WaitReady(testContext(t)))
	if !reflect.DeepEqual(restored.values(), []string{"a", "b", "c"}) {
		t.Fatalf("applied hint skipped reconstruction: %v", restored.values())
	}
	eventually(t, g.IsLeader)
	if got := propose(t, g, "d"); string(got.Data) != "4" {
		t.Fatalf("restart write: %+v", got)
	}
}

func TestLoad_BusinessRejectionConsumesEntryAndMachineFailureHalts(t *testing.T) {
	g, m := singleton(t, openStore(t, t.TempDir(), "1"))
	if result := propose(t, g, "reject"); result.Code != "rejected" {
		t.Fatalf("business refusal: %+v", result)
	}
	propose(t, g, "ok")
	if !reflect.DeepEqual(m.values(), []string{"ok"}) {
		t.Fatal("business refusal changed machine")
	}
	_, err := g.Propose(testContext(t), Proposal{Data: []byte("fatal")})
	if !errors.Is(err, fault.ErrUnavailable) {
		t.Fatalf("machine failure: %v", err)
	}
	must(t, g.Stop(testContext(t)))
	if g.Status().Found {
		t.Fatal("halted group remained found")
	}
}

func TestLoad_ThreeNodeConsensusAndLeaderReplacement(t *testing.T) {
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	var groups []*Group
	var machines []*machine
	for id := uint64(1); id <= 3; id++ {
		m := &machine{}
		machines = append(machines, m)
		groups = append(groups, newTestGroup(t, openStore(t, t.TempDir(), fmt.Sprint(id)), m, n, id, []uint64{1, 2, 3}, Founding))
	}
	for _, g := range groups {
		must(t, g.Start(context.Background()))
		must(t, g.WaitReady(testContext(t)))
	}
	var leader *Group
	eventually(t, func() bool {
		for _, g := range groups {
			if g.IsLeader() {
				leader = g
				return true
			}
		}
		return false
	})
	propose(t, leader, "first")
	eventually(t, func() bool {
		for _, m := range machines {
			if len(m.values()) != 1 {
				return false
			}
		}
		return true
	})
	n.mu.Lock()
	n.disconnected[leader.ReplicaID()] = true
	n.mu.Unlock()
	eventually(t, func() bool {
		if leader.IsLeader() {
			return false
		}
		for _, g := range groups {
			if g != leader && g.IsLeader() {
				return true
			}
		}
		return false
	})
	for _, g := range groups {
		if g.IsLeader() {
			propose(t, g, "second")
			break
		}
	}
	n.mu.Lock()
	delete(n.disconnected, leader.ReplicaID())
	n.mu.Unlock()
	eventually(t, func() bool {
		for _, m := range machines {
			if !reflect.DeepEqual(m.values(), []string{"first", "second"}) {
				return false
			}
		}
		return true
	})
}

type failingStore struct {
	storage.Store
	failures atomic.Int32
	attempts atomic.Int32
	blocked  atomic.Bool
	entered  chan struct{}
	once     sync.Once
}

func (s *failingStore) SaveReady(ctx context.Context, id string, w storage.ReadyWrite) error {
	s.attempts.Add(1)
	if s.blocked.Load() {
		s.once.Do(func() { close(s.entered) })
		<-ctx.Done()
		return ctx.Err()
	}
	for {
		left := s.failures.Load()
		if left <= 0 {
			break
		}
		if s.failures.CompareAndSwap(left, left-1) {
			return errors.New("temporary write failure")
		}
	}
	return s.Store.SaveReady(ctx, id, w)
}

func TestWAL_TransientFailureRecoversWithoutHalt(t *testing.T) {
	s := &failingStore{Store: openStore(t, t.TempDir(), "1")}
	g, m := singleton(t, s)
	s.failures.Store(2)
	before := s.attempts.Load()
	propose(t, g, "retry")
	if s.attempts.Load()-before < 3 || !reflect.DeepEqual(m.values(), []string{"retry"}) || g.Error() != nil {
		t.Fatal("retry lost/duplicated command or halted")
	}
}

func TestWAL_StepNeverBlocksAndShutdownDuringStallIsQuiet(t *testing.T) {
	s := &failingStore{Store: openStore(t, t.TempDir(), "1"), entered: make(chan struct{})}
	g, m := singleton(t, s)
	s.blocked.Store(true)
	result := make(chan error, 1)
	go func() { _, err := g.Propose(testContext(t), Proposal{Data: []byte("blocked")}); result <- err }()
	select {
	case <-s.entered:
	case <-testContext(t).Done():
		t.Fatal("writer did not stall")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	must(t, g.Step(ctx, raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: g.RaftStatus().Term + 1}))
	if len(m.values()) != 0 {
		t.Fatal("machine applied undurable entry")
	}
	must(t, g.Stop(testContext(t)))
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("undurable proposal acknowledged")
		}
	case <-testContext(t).Done():
		t.Fatal("proposal waiter leaked")
	}
	if !errors.Is(g.Error(), fault.ErrStopped) {
		t.Fatalf("shutdown caused a fatal halt: %v", g.Error())
	}
}

func TestLoad_RejectsLocalMessagesAndUnsupportedMembership(t *testing.T) {
	g, _ := singleton(t, openStore(t, t.TempDir(), "1"))
	for _, typ := range []raftpb.MessageType{raftpb.MsgHup, raftpb.MsgBeat, raftpb.MsgProp, raftpb.MsgUnreachable, raftpb.MsgSnapStatus} {
		if err := g.Step(testContext(t), raftpb.Message{Type: typ, From: 2, To: 1}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("local message %v accepted: %v", typ, err)
		}
	}
	for _, cc := range []raftpb.ConfChangeV2{{}, {Transition: raftpb.ConfChangeTransitionJointExplicit, Changes: []raftpb.ConfChangeSingle{{NodeID: 2}}}, {Changes: []raftpb.ConfChangeSingle{{NodeID: 2}, {NodeID: 3}}}} {
		if err := g.ProposeConfChange(testContext(t), cc); !errors.Is(err, fault.ErrUnsafeMembership) {
			t.Fatalf("unsupported change accepted: %v", err)
		}
	}
}

func TestLoad_StopBeforeStartAndRegistryIdentity(t *testing.T) {
	t.Parallel()
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	g := newTestGroup(t, openStore(t, t.TempDir(), "1"), &machine{}, n, 1, []uint64{1}, Founding)
	r := NewRegistry()
	must(t, r.Add(g))
	if err := r.Remove("load", 2); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale removal accepted")
	}
	if err := r.Remove("load", 1); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("live registration removed")
	}
	must(t, g.Stop(testContext(t)))
	must(t, g.Stop(testContext(t)))
	must(t, r.Remove("load", 1))
	if err := g.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatalf("stopped constructor started: %v", err)
	}
}

type liveNetwork struct{ network *network }

func (l liveNetwork) Live(string) (map[uint64]bool, bool) {
	l.network.mu.RLock()
	defer l.network.mu.RUnlock()
	live := make(map[uint64]bool)
	for id, g := range l.network.nodes {
		live[id] = g.Status().Found && !l.network.disconnected[id]
	}
	return live, true
}

func TestLoad_MembershipLearnerPromotionTransferAndRemoval(t *testing.T) {
	for _, wait := range []time.Duration{0, 5 * time.Millisecond} {
		t.Run(wait.String(), func(t *testing.T) { loadMembership(t, wait) })
	}
}

func loadMembership(t *testing.T, wait time.Duration) {
	t.Helper()
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	a, b := &machine{}, &machine{}
	one := newTestGroup(t, openStore(t, t.TempDir(), "1"), a, n, 1, []uint64{1}, Founding, wait)
	two := newTestGroup(t, openStore(t, t.TempDir(), "2"), b, n, 2, nil, Joining, wait)
	must(t, one.Start(context.Background()))
	must(t, two.Start(context.Background()))
	must(t, one.WaitReady(testContext(t)))
	must(t, two.WaitReady(testContext(t)))
	eventually(t, one.IsLeader)
	propose(t, one, "before learner")
	coordinator := membership.NewCoordinator()
	health := liveNetwork{n}
	must(t, coordinator.Apply(testContext(t), one.Membership(), health, membership.AddLearner(2)))
	eventually(t, func() bool {
		status := one.Status()
		progress := status.Progress[2]
		return progress.Match >= status.Commit && progress.Match > 0 && reflect.DeepEqual(b.values(), a.values())
	})
	if status := two.Status(); !reflect.DeepEqual(status.ConfState.Learners, []uint64{2}) {
		t.Fatalf("joining learner snapshot membership: %+v", status.ConfState)
	}
	must(t, coordinator.Apply(testContext(t), one.Membership(), health, membership.PromoteLearner(2)))
	propose(t, one, "after promotion")
	eventually(t, func() bool { return reflect.DeepEqual(a.values(), b.values()) })
	must(t, one.TransferLeader(testContext(t), 2))
	eventually(t, two.IsLeader)
	must(t, coordinator.Apply(testContext(t), two.Membership(), health, membership.RemoveReplica(1)))
	eventually(t, func() bool { return reflect.DeepEqual(two.Status().ConfState.Voters, []uint64{2}) })
	// The departing host must learn the committed removal too, even though the
	// leader has already discarded its normal replication progress.
	eventually(t, func() bool { return reflect.DeepEqual(one.Status().ConfState.Voters, []uint64{2}) })
	propose(t, two, "after removal")
}

func TestRecover_UncommittedTailIsNotAppliedAtReady(t *testing.T) {
	s := openStore(t, t.TempDir(), "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	g := newTestGroup(t, s, &machine{}, n, 1, []uint64{1}, Founding)
	must(t, g.Stop(testContext(t)))
	var entries []raftpb.Entry
	for i, value := range []string{"a", "b", "uncommitted"} {
		data, err := json.Marshal(envelope{Schema: 1, Correlation: value, RequestID: value, Data: []byte(value)})
		must(t, err)
		entries = append(entries, raftpb.Entry{Index: uint64(i + 2), Term: 2, Data: data})
	}
	must(t, s.SaveReady(testContext(t), "load", storage.ReadyWrite{Entries: entries, HardState: raftpb.HardState{Term: 2, Commit: 3}}))
	m := &machine{}
	g = newTestGroup(t, s, m, n, 1, nil, Joining)
	must(t, g.Start(context.Background()))
	must(t, g.WaitReady(testContext(t)))
	if !reflect.DeepEqual(m.values(), []string{"a", "b"}) {
		t.Fatalf("uncommitted recovery entry applied: %v", m.values())
	}
	eventually(t, g.IsLeader)
	eventually(t, func() bool { return reflect.DeepEqual(m.values(), []string{"a", "b", "uncommitted"}) })
}

type snapshotGate struct {
	storage.Store
	entered, release chan struct{}
	once             sync.Once
}

func (s *snapshotGate) SaveSnapshot(ctx context.Context, id string, ss raftpb.Snapshot) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return s.Store.SaveSnapshot(ctx, id, ss)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type captureTransport struct{ count atomic.Int32 }

func (c *captureTransport) Send(ctx context.Context, m []raftpb.Message) error {
	for _, msg := range m {
		if msg.Type == raftpb.MsgAppResp {
			c.count.Add(1)
		}
	}
	return nil
}

func TestWAL_ReceivedSnapshotIsPersistedBeforeRestoreAndAck(t *testing.T) {
	base := openStore(t, t.TempDir(), "2")
	gate := &snapshotGate{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	m := &machine{}
	transport := &captureTransport{}
	g, err := New(Options{GroupID: "load", NodeName: "2", ClusterID: "cluster", ReplicaID: 2, Mode: Joining, Store: gate, Machine: m, Transport: transport, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(t, err)
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	must(t, g.Start(context.Background()))
	must(t, g.WaitReady(testContext(t)))
	data, err := json.Marshal(struct {
		Commands []string `json:"commands"`
	}{[]string{"restored"}})
	must(t, err)
	ss := raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &ss}))
	select {
	case <-gate.entered:
	case <-testContext(t).Done():
		t.Fatal("snapshot was not handed to writer")
	}
	if len(m.values()) != 0 || transport.count.Load() != 0 {
		t.Fatal("undurable snapshot was restored or acknowledged")
	}
	close(gate.release)
	eventually(t, func() bool { return g.Status().Applied == 9 && transport.count.Load() > 0 })
	r, err := base.Load(testContext(t), "load")
	must(t, err)
	if !reflect.DeepEqual(r.Snapshot, ss) || !reflect.DeepEqual(m.values(), []string{"restored"}) {
		t.Fatal("received snapshot lost state or membership")
	}
}

func TestLoad_MultipleGroupsShareStoreAndStopIndependently(t *testing.T) {
	s := openStore(t, t.TempDir(), "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	var groups []*Group
	var machines []*machine
	for i := range 3 {
		m := &machine{}
		machines = append(machines, m)
		g, err := New(Options{GroupID: fmt.Sprintf("work-%d", i), NodeName: "1", ClusterID: "cluster", ReplicaID: 1, Mode: Founding, Peers: []raft.Peer{{ID: 1}}, Store: s, Machine: m, Transport: endpoint{n}, Timing: Timing{TickInterval: 10 * time.Millisecond}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		must(t, err)
		groups = append(groups, g)
		t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
		must(t, g.Start(context.Background()))
		must(t, g.WaitReady(testContext(t)))
	}
	for _, g := range groups {
		eventually(t, g.IsLeader)
		propose(t, g, g.GroupID())
	}
	must(t, groups[0].Stop(testContext(t)))
	for i := 1; i < 3; i++ {
		propose(t, groups[i], "still running")
		if !reflect.DeepEqual(machines[i].values(), []string{groups[i].GroupID(), "still running"}) {
			t.Fatal("shared store leaked another group's state")
		}
	}
}

func TestPropose_OverloadedWhileDegradedAndCancellationIsBounded(t *testing.T) {
	s := &failingStore{Store: openStore(t, t.TempDir(), "1"), entered: make(chan struct{})}
	g, m := singleton(t, s)
	s.blocked.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	pending := make(chan error, 1)
	go func() { _, err := g.Propose(ctx, Proposal{Data: []byte("uncertain")}); pending <- err }()
	select {
	case <-s.entered:
	case <-testContext(t).Done():
		t.Fatal("writer did not block")
	}
	eventually(t, func() bool { return g.WALStall() > time.Second })
	if _, err := g.Propose(testContext(t), Proposal{Data: []byte("shed")}); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatalf("slow write did not apply backpressure: %v", err)
	}
	cancel()
	select {
	case err := <-pending:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled caller: %v", err)
		}
	case <-testContext(t).Done():
		t.Fatal("cancelled waiter leaked")
	}
	must(t, g.Stop(testContext(t)))
	if len(m.values()) != 0 {
		t.Fatal("undurable canceled command was applied")
	}
}

func TestLoad_ConcurrentProposalsHaveExactResults(t *testing.T) {
	for _, wait := range []time.Duration{0, time.Millisecond} {
		t.Run(fmt.Sprint(wait), func(t *testing.T) {
			s := openStore(t, t.TempDir(), "1")
			n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
			m := &machine{}
			g, err := New(Options{GroupID: "load", NodeName: "1", ClusterID: "cluster", ReplicaID: 1, Mode: Founding, Peers: []raft.Peer{{ID: 1}}, Store: s, Machine: m, Transport: endpoint{n}, Timing: Timing{TickInterval: 10 * time.Millisecond, WALWriteWait: wait}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			must(t, err)
			t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
			must(t, g.Start(context.Background()))
			must(t, g.WaitReady(testContext(t)))
			eventually(t, g.IsLeader)
			ctx := testContext(t)
			results := make(chan string, 50)
			errorsC := make(chan error, 50)
			var wg sync.WaitGroup
			for i := range 50 {
				wg.Go(func() {
					result, err := g.Propose(ctx, Proposal{RequestID: fmt.Sprint(i), Data: []byte(fmt.Sprint(i))})
					if err != nil {
						errorsC <- err
					} else {
						results <- string(result.Data)
					}
				})
			}
			wg.Wait()
			close(errorsC)
			close(results)
			for err := range errorsC {
				t.Error(err)
			}
			seen := make(map[string]bool)
			for result := range results {
				if seen[result] {
					t.Fatalf("duplicate result %q", result)
				}
				seen[result] = true
			}
			commands := m.values()
			unique := make(map[string]bool)
			for _, c := range commands {
				unique[c] = true
			}
			if len(seen) != 50 || len(commands) != 50 || len(unique) != 50 {
				t.Fatalf("batching changed commands or results: %d/%d/%d", len(seen), len(commands), len(unique))
			}
		})
	}
}

func TestOptions_RejectUnsafeInputsBeforeCreatingState(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	valid := Options{GroupID: "load", NodeName: "1", ClusterID: "cluster", ReplicaID: 1, Mode: Founding, Peers: []raft.Peer{{ID: 1}}, Store: s, Machine: &machine{}, Transport: endpoint{n}}
	for _, change := range []func(*Options){
		func(o *Options) { o.GroupID = "_meta" }, func(o *Options) { o.ReplicaID = 0 }, func(o *Options) { o.Mode = "" }, func(o *Options) { o.Store = nil }, func(o *Options) { o.Machine = nil }, func(o *Options) { o.Transport = nil },
		func(o *Options) { o.Timing.TickInterval = -time.Second }, func(o *Options) { o.Timing.TickInterval = time.Millisecond }, func(o *Options) { o.Timing.ElectionTicks = 1 }, func(o *Options) { o.Timing.ElectionTicks = 2; o.Timing.HeartbeatTicks = 2 },
		func(o *Options) { o.Timing.TickInterval = time.Duration(1<<63 - 1); o.Timing.ElectionTicks = 2 },
		func(o *Options) { o.Timing.WALWriteWait = -time.Millisecond }, func(o *Options) { o.Timing.WALWriteWait = 11 * time.Millisecond }, func(o *Options) { o.MaxPendingProposals = -1 }, func(o *Options) { o.Peers = []raft.Peer{{ID: 1}, {ID: 1}} }, func(o *Options) { o.Peers = []raft.Peer{{ID: 2}} }, func(o *Options) { o.Mode = Joining },
	} {
		candidate := valid
		change(&candidate)
		if _, err := New(candidate); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("invalid options accepted: %+v: %v", candidate, err)
		}
	}
	p, err := s.Probe(testContext(t), "load")
	must(t, err)
	if p.Exists {
		t.Fatal("rejected constructor created state")
	}
}

func TestRecoveredUnappliedConfigurationBlocksSecondChange(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	original := newTestGroup(t, s, &machine{}, n, 1, []uint64{1}, Founding)
	if err := original.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}}}
	data, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SaveReady(context.Background(), "load", storage.ReadyWrite{Entries: []raftpb.Entry{{Index: 2, Term: 1, Type: raftpb.EntryConfChangeV2, Data: data}}, HardState: raftpb.HardState{Term: 1, Commit: 1}}); err != nil {
		t.Fatal(err)
	}
	recovered := newTestGroup(t, s, &machine{}, n, 1, nil, Joining)
	if err = recovered.rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	// Pre-vote and election self votes are released after successive Ready batches.
	for i := 0; i < 4 && recovered.rn.BasicStatus().RaftState != raft.StateLeader; i++ {
		rd := recovered.rn.Ready()
		if err = s.SaveReady(context.Background(), "load", storage.ReadyWrite{Entries: rd.Entries, HardState: rd.HardState}); err != nil {
			t.Fatal(err)
		}
		if err = recovered.ms.Append(rd.Entries); err != nil {
			t.Fatal(err)
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			if err = recovered.ms.SetHardState(rd.HardState); err != nil {
				t.Fatal(err)
			}
		}
		recovered.rn.Advance(rd)
	}
	if recovered.rn.BasicStatus().RaftState != raft.StateLeader {
		t.Fatal("test election did not finish")
	}

	change := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}}}
	request := &request{ctx: context.Background(), change: &change, reply: make(chan response, 1)}
	recovered.handleControl(request)
	result := <-request.reply
	if !errors.Is(result.err, fault.ErrOverloaded) {
		t.Fatal("second change accepted before recovered configuration applied", result.err)
	}
}
