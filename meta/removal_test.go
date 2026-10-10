package meta

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func removalDocument(state topology.NodeState, member uint64) topology.Document {
	doc := topology.Document{Version: 4, ClusterID: "cluster", Nodes: []topology.Node{{Name: "n1", Type: "worker", State: topology.NodeActive}, {Name: "n2", Type: "worker", State: state}}, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: max(3, member+1), Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}}}}
	if state != topology.NodeExcluded {
		doc.Groups[0].Replicas = append(doc.Groups[0].Replicas, topology.Replica{NodeName: "n2", ReplicaID: member, Role: topology.ReplicaVoter})
	}
	return doc
}
func removalFixture(t *testing.T, callback func(context.Context, string) error) (*Group, <-chan string) {
	t.Helper()
	root, cancel := context.WithCancel(context.Background())
	work, stop := context.WithCancel(root)
	fired := make(chan string, 4)
	g := &Group{identity: Identity{NodeName: "n2", ClusterID: "cluster", InstanceID: "instance", MemberID: 2}, memberSeen: true, started: true, ctx: root, workCtx: work, workCancel: stop, removalSignal: make(chan struct{}, 1)}
	g.cancel = cancel
	g.quiesced, g.stopped = make(chan struct{}), make(chan struct{})
	g.opts.OnSelfRemoved = func(ctx context.Context, reason string) error {
		fired <- reason
		if callback != nil {
			return callback(ctx, reason)
		}
		return nil
	}
	g.removalWG.Add(1)
	go g.notifyRemoval(root)
	t.Cleanup(func() { cancel(); g.removalWG.Wait() })
	return g, fired
}
func awaitRemoval(t *testing.T, fired <-chan string) string {
	t.Helper()
	select {
	case reason := <-fired:
		return reason
	case <-time.After(time.Second):
		t.Fatal("missing removal notification")
		return ""
	}
}
func refuteRemoval(t *testing.T, g *Group) {
	t.Helper()
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.selfRemoval != nil {
		t.Fatal("unexpected self-removal", g.selfRemoval)
	}
}
func TestSelfRemoval_FiresOnceWhenExcluded(t *testing.T) {
	t.Parallel()
	g, fired := removalFixture(t, nil)
	doc := removalDocument(topology.NodeExcluded, 2)
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}}, 42)
	if reason := awaitRemoval(t, fired); !strings.Contains(reason, `"n2" member 2`) || !strings.Contains(reason, "v4") {
		t.Fatal(reason)
	}
	g.removalWG.Wait()
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}}, 43)
	select {
	case r := <-fired:
		t.Fatal("duplicate removal", r)
	default:
	}
	if g.ensureRunning(context.Background()) != fault.ErrStopped {
		t.Fatal("removed process retained mutable authority")
	}
}
func TestSelfRemoval_OtherMemberRemovedDoesNotFire(t *testing.T) {
	t.Parallel()
	g, _ := removalFixture(t, nil)
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{1, 2}}, 42)
	refuteRemoval(t, g)
}
func TestSelfRemoval_DemotionToLearnerDoesNotFire(t *testing.T) {
	t.Parallel()
	g, _ := removalFixture(t, nil)
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}, 42)
	refuteRemoval(t, g)
}
func TestSelfRemoval_NotExcludedDoesNotFire(t *testing.T) {
	t.Parallel()
	for _, state := range []topology.NodeState{topology.NodeActive, topology.NodeDraining, topology.NodeDrained} {
		g, _ := removalFixture(t, nil)
		g.observeRemoval(g.identity, removalDocument(state, 2), raftpb.ConfState{Voters: []uint64{1}}, 42)
		refuteRemoval(t, g)
	}
}
func TestSelfRemoval_SupersededMemberIDFires(t *testing.T) {
	t.Parallel()
	g, fired := removalFixture(t, nil)
	g.observeRemoval(g.identity, removalDocument(topology.NodeActive, 7), raftpb.ConfState{Voters: []uint64{1, 7}}, 42)
	if r := awaitRemoval(t, fired); !strings.Contains(r, "replacement with member 7") {
		t.Fatal(r)
	}
}
func TestSelfRemoval_JointDemotionDoesNotFire(t *testing.T) {
	t.Parallel()
	g, _ := removalFixture(t, nil)
	doc := removalDocument(topology.NodeExcluded, 2)
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}, VotersOutgoing: []uint64{1, 2}, LearnersNext: []uint64{2}}, 42)
	refuteRemoval(t, g)
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}, 43)
	refuteRemoval(t, g)
}
func TestSelfRemoval_JointRemovalFiresOnLeavingJoint(t *testing.T) {
	t.Parallel()
	g, fired := removalFixture(t, nil)
	doc := removalDocument(topology.NodeExcluded, 2)
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}, VotersOutgoing: []uint64{1, 2}}, 42)
	refuteRemoval(t, g)
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}}, 43)
	awaitRemoval(t, fired)
}
func TestSelfRemoval_NoMembershipDeltaIsInert(t *testing.T) {
	t.Parallel()
	g, _ := removalFixture(t, nil)
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{2, 1}}, 42)
	refuteRemoval(t, g)
}
func TestSelfRemoval_UnknownMemberIDDoesNotFire(t *testing.T) {
	t.Parallel()
	g, _ := removalFixture(t, nil)
	g.identity.MemberID = 0
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{1}}, 42)
	refuteRemoval(t, g)
}
func TestSelfRemoval_NilCallbackIsInert(t *testing.T) {
	t.Parallel()
	g := &Group{identity: Identity{NodeName: "n2", MemberID: 2}, memberSeen: true, ready: make(chan struct{})}
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{1}}, 42)
	if g.selfRemoval == nil || g.selfRemoval.Notified || !g.selfRemoval.Fenced {
		t.Fatal("nil hook should fence without notification", g.selfRemoval)
	}
	if err := g.WaitReady(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("removed handle did not release readiness waiters", err)
	}
}
func TestConfStateContains(t *testing.T) {
	t.Parallel()
	cs := raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}, VotersOutgoing: []uint64{4}, LearnersNext: []uint64{5}}
	for id := uint64(0); id <= 6; id++ {
		if got := containsMember(cs, id); got != (id >= 1 && id <= 5) {
			t.Fatal(id, got)
		}
	}
}
func TestSelfRemovalRequiresApprovedIntentAndPreviouslyAppliedMembership(t *testing.T) {
	t.Parallel()
	g, _ := removalFixture(t, nil)
	doc := removalDocument(topology.NodeExcluded, 2)
	g.memberSeen = false
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}}, 42)
	refuteRemoval(t, g)
	g.memberSeen = true
	g.observeRemoval(g.identity, removalDocument(topology.NodeActive, 2), raftpb.ConfState{Voters: []uint64{1}}, 42)
	refuteRemoval(t, g)
	stale := g.identity
	stale.MemberID = 9
	g.observeRemoval(stale, doc, raftpb.ConfState{Voters: []uint64{1}}, 42)
	refuteRemoval(t, g)
}

func TestRetiredOutboundRouteDoesNotRestoreIncomingMembership(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Node("n2").State = topology.NodeExcluded
	removeNodeReplicas(&doc, "n2", true)
	f.commit(t, doc, true)
	f.g.addresses = map[string]string{"n2": "127.0.0.1:2002"}
	if owner := f.g.memberNode(2); owner != "" {
		t.Fatal("retired inbound identity retained", owner)
	}
	address, err := f.g.resolveMember(context.Background(), 2)
	if err != nil || address != "127.0.0.1:2002" {
		t.Fatal("lost final-commit destination", address, err)
	}
	if owner := f.g.memberNode(2); owner != "" {
		t.Fatal("outbound resolution reauthorized retired sender", owner)
	}
	if _, err = f.g.resolveMember(context.Background(), 999); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("invented historical identity", err)
	}
}

func TestSelfRemovalQuiesceJoinsCanceledCallback(t *testing.T) {
	t.Parallel()
	canceled, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	g, fired := removalFixture(t, func(ctx context.Context, _ string) error {
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	})
	defer finish()
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{1}}, 42)
	awaitRemoval(t, fired)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := g.Quiesce(ctx)
	cancel()
	if !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("quiescence did not join callback", err)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("callback did not receive cancellation")
	}
	finish()
	if err = g.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = g.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type removalPersister struct {
	err   error
	saved bool
}

func (*removalPersister) SaveState([]byte) error { return nil }
func (s *removalPersister) SaveApplication(uint64, raftpb.ConfState, []byte) error {
	if s.err != nil {
		return s.err
	}
	s.saved = true
	return nil
}
func TestSelfRemoval_ApplyEntryDoesNotDeliverBeforeDurable(t *testing.T) {
	t.Parallel()
	g, fired := removalFixture(t, nil)
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Node("n2").State = topology.NodeExcluded
	removeNodeReplicas(&doc, "n2", true)
	f.commit(t, doc, true)
	data, err := f.g.machine.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store := &removalPersister{err: errors.New("disk failure")}
	m := metaengine.NewMachine(f.g.rules, store)
	if err = m.Restore(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	m.OnApplied = func(index uint64, cs raftpb.ConfState) {
		if !store.saved {
			t.Error("hook precedes persistence")
		}
		g.observeRemoval(g.identity, m.Target(), cs, index)
	}
	// The real shared-engine metadata boundary must not call the hook after a
	// failed transaction; a recovered committed tail can retry it later.
	if err = m.AfterApply(context.Background(), f.index+1, 1, raftpb.ConfState{Voters: []uint64{1, 3}}); err == nil {
		t.Fatal("ignored failed persistence")
	}
	refuteRemoval(t, g)
	store.err = nil
	if err = m.AfterApply(context.Background(), f.index+1, 1, raftpb.ConfState{Voters: []uint64{1, 3}}); err != nil {
		t.Fatal(err)
	}
	awaitRemoval(t, fired)
}
