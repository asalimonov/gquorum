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

func TestStageUsesApprovedPlacementAndPreservesCAS(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	before := g.state().Latest()
	forged := before.Clone()
	forged.Node("n1").Labels = map[string]string{topology.LabelReserved: "true"}
	forged.Groups = append(forged.Groups, topology.Group{ID: "work", NextReplicaID: 2,
		Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	forged.ChangedBy = "operator"
	if _, err := g.Stage(ctx, forged, before.Version); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("staged assignment gained its own reservation exception: %v", err)
	} else {
		var placement *topology.PlacementError
		if !errors.As(err, &placement) || len(placement.Report.Groups) == 0 {
			t.Fatalf("committed rejection lost typed placement report: %v", err)
		}
	}
	views := g.state().Views()
	latest, target := views.Latest, views.Target
	if latest.Version != before.Version || target.Version != before.Version || views.FullyConverged.Version != before.Version {
		t.Fatal("rejected stage published a topology view", latest.Version, target.Version, views.FullyConverged.Version)
	}

	valid := before.Clone()
	valid.Node("n1").Labels = map[string]string{"region": "one"}
	valid.ChangedBy = "operator"
	staged, err := g.Stage(ctx, valid, before.Version)
	if err != nil || staged.Version != before.Version+1 {
		t.Fatalf("valid label stage: %+v %v", staged, err)
	}
	if _, err := g.Stage(ctx, forged, before.Version); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatalf("stale stage bypassed CAS: %v", err)
	}
	if g.state().Latest().Version != staged.Version || g.state().Target().Version != before.Version {
		t.Fatal("CAS refusal lost staged placement")
	}
}
