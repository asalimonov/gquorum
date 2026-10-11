//go:build functional

package metaservice

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func singleMetaFixture(t *testing.T) []*Group {
	return singleMetaFixtureWith(t, nil)
}

func singleMetaFixtureWith(t *testing.T, modify func(*Options)) []*Group {
	t.Helper()
	const count = 1
	listeners := make([]net.Listener, count)
	founders := make([]FoundingMember, count)
	for i := range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		founders[i] = FoundingMember{Node: topology.Node{Name: fmt.Sprintf("n%d", i+1), Type: "worker"}, MemberID: uint64(i + 1), Address: listener.Addr().String()}
	}
	groups := make([]*Group, count)
	for i := range count {
		opts := Options{Identity: topology.Identity{NodeName: founders[i].Node.Name, ClusterID: "cluster", InstanceID: fmt.Sprintf("instance-%d", i), MemberID: founders[i].MemberID}, DBPath: filepath.Join(t.TempDir(), "meta.bolt"), Founders: founders, Limits: topology.Limits{MaxNodes: count}, Timing: Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 50}, SessionTTL: 3 * time.Second, Network: NetworkOptions{Listener: listeners[i], AdvertiseAddress: founders[i].Address, AllowPlaintext: true, ClusterCredential: []byte("test-cluster-credential")}}
		if modify != nil {
			modify(&opts)
		}
		group, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		groups[i] = group
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := group.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	for _, group := range groups {
		if err := group.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return groups
}
