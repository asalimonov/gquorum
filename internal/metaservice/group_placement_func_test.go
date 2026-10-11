//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestGroupPlacementRetainsEligibleIDsAndUsesFreshIDsWhenExpanded(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	ctx := context.Background()
	result, err := f.g.SetGroupPlacement(ctx, "work", topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2}, preview())
	doc := mutationDoc(t, result, err)
	if len(doc.VotersOf("work")) != 2 || doc.Group("work").Replicas[0].ReplicaID != 1 || doc.Group("work").Replicas[1].ReplicaID != 2 || len(doc.MetaGroup().Replicas) != 3 {
		t.Fatal(doc)
	}
	f.commit(t, doc, true)
	result, err = f.g.SetGroupPlacement(ctx, "work", topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}, preview())
	doc = mutationDoc(t, result, err)
	if len(doc.VotersOf("work")) != 3 || doc.Group("work").Replicas[2].ReplicaID != 4 {
		t.Fatal("expanded group reused retired identity", doc)
	}
}
func TestGroupPlacementRefusesIneligibleMinimumWithoutChangingState(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	before := f.g.machine.Latest()
	if _, err := f.g.SetGroupPlacement(t.Context(), "work", topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2, AllowedNodeTypes: []topology.NodeType{"observer"}}, preview()); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal(err)
	}
	after := f.g.machine.Latest()
	if after.Version != before.Version || after.Group("work").NextReplicaID != before.Group("work").NextReplicaID {
		t.Fatal("refusal changed state")
	}
	if _, err := f.g.SetGroupPlacement(t.Context(), topology.MetaGroupID, topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2}, preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}
