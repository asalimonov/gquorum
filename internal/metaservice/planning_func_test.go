//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaUnknownLoadControllerRefusesApprovalWithoutChangingTarget(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc, _ := g.Latest(ctx)
	doc.Origin = topology.OriginOperator
	doc.Groups = append(doc.Groups, topology.Group{ID: "load-1", NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	staged, err := g.Stage(ctx, doc, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g.Approve(ctx, staged.Version); !errors.Is(err, fault.ErrNoController) {
		t.Fatal("approval has no load controller", err)
	}
	if g.state().Target().Version != 1 || g.state().Latest().Version != 2 {
		t.Fatal("failed approval rolled back stage or changed Target")
	}
}
