//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

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
