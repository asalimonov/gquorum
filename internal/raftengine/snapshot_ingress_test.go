//go:build !functional

package raftengine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestSnapshotIngressTicketCompletesOnce(t *testing.T) {
	t.Parallel()
	var count atomic.Int32
	ticket := &ownedSnapshotTicket{ingress: SnapshotIngress{Done: func(SnapshotInstallOutcome, error) { count.Add(1) }}}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { ticket.finish(SnapshotInstallFailed, fault.ErrStopped) })
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatal(count.Load())
	}
}

func TestSnapshotIngressPrepareAndShape(t *testing.T) {
	t.Parallel()
	g := &Group{opts: Options{ClusterID: "cluster", GroupID: "load", ReplicaID: 2}}
	data, err := encodeSnapshot(g.opts, []byte("application"), raftstate.AuthorityState{})
	if err != nil {
		t.Fatal(err)
	}
	base := raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: &raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}}
	decoded, err := g.PrepareOwnedSnapshot(t.Context(), base)
	if err != nil || string(decoded.Application) != "application" {
		t.Fatal(decoded, err)
	}
	cases := map[string]func(*raftpb.Message){
		"zero term":         func(m *raftpb.Message) { m.Term = 0 },
		"zero index":        func(m *raftpb.Message) { m.Snapshot.Metadata.Index = 0 },
		"future term":       func(m *raftpb.Message) { m.Snapshot.Metadata.Term = 4 },
		"entries":           func(m *raftpb.Message) { m.Entries = []raftpb.Entry{{Index: 1}} },
		"responses":         func(m *raftpb.Message) { m.Responses = []raftpb.Message{{}} },
		"wrong destination": func(m *raftpb.Message) { m.To = 3 },
		"missing snapshot":  func(m *raftpb.Message) { m.Snapshot = nil },
		"oversized context": func(m *raftpb.Message) { m.Context = make([]byte, snapshotIngressHeaderLimit) },
		"foreign envelope": func(m *raftpb.Message) {
			m.Snapshot.Data = []byte(`{"schema":2,"cluster_id":"other","group_id":"load","application":"YQ=="}`)
		},
		"duplicate voter": func(m *raftpb.Message) { m.Snapshot.Metadata.ConfState.Voters = []uint64{1, 1} },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := base
			ss := *base.Snapshot
			m.Snapshot = &ss
			change(&m)
			if _, err := g.PrepareOwnedSnapshot(t.Context(), m); err == nil {
				t.Fatal("accepted malformed snapshot")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := g.PrepareOwnedSnapshot(ctx, base); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	header := base
	ss := *base.Snapshot
	ss.Data = nil
	header.Snapshot = &ss
	if err := g.validateOwnedSnapshotShape(header, snapshotHeaderOnly); err != nil {
		t.Fatal(err)
	}
	if err := g.validateOwnedSnapshotShape(base, snapshotHeaderOnly); err == nil {
		t.Fatal("header carried payload")
	}
}

func TestSnapshotIngressSkipDoesNotValidateUnusedAuthority(t *testing.T) {
	t.Parallel()
	var outcome SnapshotInstallOutcome
	g := &Group{authority: raftstate.AuthorityState{Grant: &topology.AuthorityGrant{Key: topology.AuthorityKey{Revision: 2}}}}
	offered := raftpb.Snapshot{Data: []byte("owned"), Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2}}
	g.activeSnapshot = &ownedSnapshotTicket{ingress: SnapshotIngress{Message: raftpb.Message{Snapshot: &offered}, Done: func(o SnapshotInstallOutcome, _ error) { outcome = o }}}
	if err := g.classifyOwnedSnapshotReady(&raft.Ready{}); err != nil || outcome != SnapshotSkipped {
		t.Fatal(outcome, err)
	}
	g.activeSnapshot = &ownedSnapshotTicket{ingress: SnapshotIngress{Message: raftpb.Message{Snapshot: &offered}}}
	if err := g.classifyOwnedSnapshotReady(&raft.Ready{Snapshot: offered}); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("selected authority regression accepted", err)
	}
	if g.activeSnapshot == nil {
		t.Fatal("failure released before join")
	}
}
