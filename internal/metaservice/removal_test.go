//go:build !functional

package metaservice

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
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

func TestSelfRemoval_ApprovedNodeDeletionFencesSeenLearner(t *testing.T) {
	t.Parallel()
	g, fired := removalFixture(t, nil)
	doc := removalDocument(topology.NodeExcluded, 2)
	doc.Nodes = doc.Nodes[:1]
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}, 41)
	refuteRemoval(t, g)
	g.observeRemoval(g.identity, doc, raftpb.ConfState{Voters: []uint64{1}}, 42)
	if reason := awaitRemoval(t, fired); !strings.Contains(reason, "approved node deletion v4") {
		t.Fatal(reason)
	}
	if err := g.ensureRunning(t.Context()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("deleted learner retained ordinary participation", err)
	}
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
	g := &Group{identity: topology.Identity{NodeName: "n2", MemberID: 2}, memberSeen: true, ready: make(chan struct{})}
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
