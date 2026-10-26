//go:build functional

package raftengine

import (
	"context"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/tracker"

	"go.etcd.io/raft/v3/raftpb"
)

type orderedTransport struct {
	mu        sync.Mutex
	indexes   map[uint64][]uint64
	delivered chan uint64
}

func outboundTestMeter(t *testing.T) *internalmetrics.Meter {
	t.Helper()
	m, err := internalmetrics.NewMeter(metrics.ReplicaIdentity{ClusterID: "cluster", GroupID: "load", NodeName: "1", ReplicaID: 1, RunID: "sender-test"}, internalmetrics.Options{})
	must(t, err)
	return m
}

func (o *orderedTransport) Send(_ context.Context, messages []raftpb.Message) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, message := range messages {
		o.indexes[message.To] = append(o.indexes[message.To], message.Index)
		o.delivered <- message.To
	}
	return nil
}
func TestOutboundQueuesPreserveFIFOAndRetireIdleMembershipWorkers(t *testing.T) {
	t.Parallel()
	transport := &orderedTransport{indexes: map[uint64][]uint64{}, delivered: make(chan uint64, 20)}
	ctx, cancel := context.WithCancel(t.Context())
	group := &Group{ctx: ctx, opts: Options{Transport: transport, Timing: Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 10}}, sendC: make(chan []raftpb.Message, 20), unreachable: make(chan uint64, 20), cached: raft.Status{Config: tracker.Config{Voters: quorum.JointConfig{quorum.MajorityConfig{1: {}, 2: {}}, nil}}}}
	group.meter = outboundTestMeter(t)
	group.wg.Add(1)
	go group.sender()
	t.Cleanup(func() { cancel(); group.wg.Wait() })
	for index := uint64(1); index <= 10; index++ {
		group.sendC <- []raftpb.Message{{To: 1, Index: index}, {To: 2, Index: index}}
	}
	deadline, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	for range 20 {
		select {
		case <-transport.delivered:
		case <-deadline.Done():
			t.Fatal("sender failed to deliver the nonempty FIFO workload")
		}
	}
	transport.mu.Lock()
	expected := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, peer := range []uint64{1, 2} {
		if !reflect.DeepEqual(transport.indexes[peer], expected) {
			t.Errorf("peer %d FIFO was reordered: %v", peer, transport.indexes[peer])
		}
	}
	transport.mu.Unlock()
	if group.sendWorkers.Load() != 2 {
		t.Fatal("membership destinations did not own separate workers", group.sendWorkers.Load())
	}
	group.mu.Lock()
	delete(group.cached.Config.Voters[0], 2)
	group.mu.Unlock()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for group.sendWorkers.Load() != 1 {
		select {
		case <-ticker.C:
		case <-deadline.Done():
			t.Fatal("removed idle destination leaked its worker", group.sendWorkers.Load())
		}
	}
	cancel()
	group.wg.Wait()
	if group.sendWorkers.Load() != 0 {
		t.Fatal("sender teardown left owned peer workers", group.sendWorkers.Load())
	}
}

type retiringTransport struct {
	entered chan struct{}
	final   chan struct{}
	calls   atomic.Int64
}

func (r *retiringTransport) Send(ctx context.Context, messages []raftpb.Message) error {
	r.calls.Add(1)
	for _, message := range messages {
		if message.Type == raftpb.MsgApp && len(message.Entries) > 0 {
			close(r.final)
			return nil
		}
	}
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}
func TestRemovedDestinationDropsStaleBacklogAndAttemptsFinalCommit(t *testing.T) {
	t.Parallel()
	transport := &retiringTransport{entered: make(chan struct{}, 1), final: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	group := &Group{ctx: ctx, opts: Options{Transport: transport, Timing: Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 20}}, sendC: make(chan []raftpb.Message, 64), unreachable: make(chan uint64, 64), cached: raft.Status{Config: tracker.Config{Voters: quorum.JointConfig{quorum.MajorityConfig{1: {}, 2: {}}, nil}}}}
	group.meter = outboundTestMeter(t)
	group.wg.Add(1)
	go group.sender()
	t.Cleanup(func() { cancel(); group.wg.Wait() })
	group.sendC <- []raftpb.Message{{To: 2, Type: raftpb.MsgHeartbeat}}
	deadline, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	select {
	case <-transport.entered:
	case <-deadline.Done():
		t.Fatal("transport stall never entered")
	}
	for range 40 {
		group.sendC <- []raftpb.Message{{To: 2, Type: raftpb.MsgHeartbeat}}
	}
	change := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{NodeID: 2, Type: raftpb.ConfChangeRemoveNode}}}
	encoded, err := change.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	group.sendC <- []raftpb.Message{{To: 2, Type: raftpb.MsgApp, Commit: 42, Entries: []raftpb.Entry{{Index: 42, Type: raftpb.EntryConfChangeV2, Data: encoded}}}}
	group.mu.Lock()
	delete(group.cached.Config.Voters[0], 2)
	group.mu.Unlock()
	select {
	case <-transport.final:
	case <-deadline.Done():
		t.Fatal("stale backlog delayed the final committed removal")
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for group.sendWorkers.Load() != 0 {
		select {
		case <-ticker.C:
		case <-deadline.Done():
			t.Fatal("retired peer retained a backlog worker")
		}
	}
	if transport.calls.Load() != 2 {
		t.Fatal("removed peer replayed stale queued traffic", transport.calls.Load())
	}
}
