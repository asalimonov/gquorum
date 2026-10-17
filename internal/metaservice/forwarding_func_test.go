//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaMemberRoutingSeparatesApprovedInboundFromRetainedOutbound(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	g := f.g
	g.addresses = map[string]string{"n1": "meta-one", "n2": "meta-two", "n3": "meta-three", "n4": "meta-four"}
	for _, tc := range []struct {
		id      uint64
		address string
		err     error
	}{
		{1, "meta-one", nil}, {2, "meta-two", nil}, {0, "", fault.ErrIdentityMismatch}, {99, "", fault.ErrIdentityMismatch},
	} {
		got, err := g.resolveMember(t.Context(), tc.id)
		if got != tc.address || !errors.Is(err, tc.err) {
			t.Fatal(tc.id, got, err)
		}
	}
	doc := g.state().Latest()
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n4", Type: "worker", State: topology.NodeActive})
	doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas, topology.Replica{NodeName: "n4", ReplicaID: 4, Role: topology.ReplicaVoter})
	doc.MetaGroup().NextReplicaID = 5
	f.commit(t, doc, false)
	if g.memberNode(4) != "" {
		t.Fatal("staged-only identity authorized inbound")
	}
	// History can resolve outbound destinations, including retained staged rows;
	// that route never admits their incoming identity before approval.
	if address, err := g.resolveMember(t.Context(), 4); err != nil || address != "meta-four" {
		t.Fatal("retained outbound route absent", address, err)
	}
	f.command(t, f.fixtureApproval(t, f.g.machine.Latest()))
	if address, err := g.resolveMember(t.Context(), 4); err != nil || address != "meta-four" {
		t.Fatal("approved Target route absent", address, err)
	}
	delete(g.addresses, "n4")
	if _, err := g.resolveMember(t.Context(), 4); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("unresolved address accepted", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := g.resolveMember(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
