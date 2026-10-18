//go:build !functional

package metaengine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func assertLoadPeerIndex(t *testing.T, m *Machine) {
	t.Helper()
	groups := map[string]bool{"unknown": true}
	s := m.scoped
	for _, doc := range s.indexes.documents {
		for _, g := range doc.Groups {
			if g.ID != topology.MetaGroupID {
				groups[g.ID] = true
			}
		}
	}
	for _, debt := range s.Cleanup {
		groups[debt.Identity.GroupID] = true
	}
	for _, record := range s.AuthorityRecords {
		if record.Grant.Key.GroupID != topology.MetaGroupID {
			groups[record.Grant.Key.GroupID] = true
		}
	}
	want := make(map[storage.Identity]struct{})
	for group := range groups {
		ids, err := m.LoadCatalog(t.Context(), group)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			want[id] = struct{}{}
			found, err := m.HasLoadPeer(t.Context(), id)
			if err != nil || !found {
				t.Fatal("missing route", id, err)
			}
		}
	}
	if !maps.Equal(want, s.indexes.loadPeers) {
		t.Fatal("lookup differs from retained catalog", want, s.indexes.loadPeers)
	}
	for id := range want {
		for _, other := range []storage.Identity{
			{ClusterID: "foreign", NodeName: id.NodeName, GroupID: id.GroupID, ReplicaID: id.ReplicaID},
			{ClusterID: id.ClusterID, NodeName: "foreign", GroupID: id.GroupID, ReplicaID: id.ReplicaID},
			{ClusterID: id.ClusterID, NodeName: id.NodeName, GroupID: id.GroupID, ReplicaID: 99999},
		} {
			if found, err := m.HasLoadPeer(t.Context(), other); err != nil || found {
				t.Fatal("foreign route admitted", other, err)
			}
		}
	}
}

func TestLoadPeerIndexRetainsExactCatalogThroughRestoreAndPruning(t *testing.T) {
	t.Parallel()
	for name, fixture := range map[string]func(*testing.T) scopedState{
		"retry":              func(*testing.T) scopedState { return scopedGraphFixture() },
		"cleanup and grants": func(*testing.T) scopedState { return scopedCodecFixture() },
		"admission":          func(t *testing.T) scopedState { return loadMembershipScopeFixture(t, topology.StepAddLearner) },
		"retained history":   completedRetentionFixture,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := fixture(t)
			m := restoreMachineFixture(t, s)
			assertLoadPeerIndex(t, m)
			pruneScopedFixture(t, m.scoped)
			assertLoadPeerIndex(t, m)
			restored := restoreMachineFixture(t, *m.scoped)
			assertLoadPeerIndex(t, restored)
			old := maps.Clone(restored.scoped.indexes.loadPeers)
			if err := restored.Restore(t.Context(), []byte(`{"schema_version":3}`)); err == nil {
				t.Fatal("accepted old snapshot")
			}
			if !maps.Equal(old, restored.scoped.indexes.loadPeers) {
				t.Fatal("refused restore changed lookup")
			}
		})
	}
}

func TestLoadPeerIndexPublishesOnlyAppliedApproval(t *testing.T) {
	t.Parallel()
	s, approval := foundingApprovalFixture(t)
	m := restoreMachineFixture(t, s)
	id := storage.Identity{ClusterID: s.Latest.ClusterID, NodeName: "n1", GroupID: "shard0", ReplicaID: 1}
	if found, err := m.HasLoadPeer(t.Context(), id); err != nil || found {
		t.Fatal("staging authorized peer", found, err)
	}
	_, err := m.Apply(t.Context(), raftengine.Command{Index: s.AppliedIndex + 1, Term: 1, Data: mustEncode(t, approval), ConfState: raftpb.ConfState{Voters: []uint64{1}}})
	if err != nil {
		t.Fatal(err)
	}
	if found, err := m.HasLoadPeer(t.Context(), id); err != nil || !found {
		t.Fatal("approval did not publish peer", found, err)
	}
	assertLoadPeerIndex(t, m)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.HasLoadPeer(ctx, id); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	invalid := id
	invalid.GroupID = topology.MetaGroupID
	if _, err := m.HasLoadPeer(t.Context(), invalid); err == nil {
		t.Fatal("accepted meta ID")
	}
	if err := m.Restore(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if found, err := m.HasLoadPeer(t.Context(), id); err != nil || found {
		t.Fatal("restore kept obsolete index", found, err)
	}
}

func BenchmarkLoadPeerLookup(b *testing.B) {
	for _, groups := range []int{1, 64} {
		b.Run(fmt.Sprintf("groups-%d", groups), func(b *testing.B) {
			s := scopedState{Target: topology.Document{ClusterID: "c"}, indexes: &scopedIndexes{groups: map[string]*groupConvergence{}, documents: map[uint64]topology.Document{}}}
			for n := range groups {
				s.Target.Groups = append(s.Target.Groups, topology.Group{ID: fmt.Sprintf("g%d", n), Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1}, {NodeName: "n2", ReplicaID: 2}, {NodeName: "n3", ReplicaID: 3}}})
			}
			s.indexes.loadPeers = indexLoadPeers(&s, s.indexes)
			m := &Machine{scoped: &s}
			id := storage.Identity{ClusterID: "c", NodeName: "n2", GroupID: fmt.Sprintf("g%d", groups-1), ReplicaID: 2}
			for _, indexed := range []bool{false, true} {
				b.Run(fmt.Sprintf("indexed-%t", indexed), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						var found bool
						var err error
						if indexed {
							found, err = m.HasLoadPeer(b.Context(), id)
						} else {
							var ids []storage.Identity
							ids, err = m.LoadCatalog(b.Context(), id.GroupID)
							found = slices.Contains(ids, id)
						}
						if err != nil || !found {
							b.Fatal(found, err)
						}
					}
				})
			}
		})
	}
}
