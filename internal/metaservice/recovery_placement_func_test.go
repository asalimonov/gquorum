//go:build functional

package metaservice

import (
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/recoveryplacement"
	"github.com/asalimonov/gquorum/topology"
)

func TestOrdinaryServicePlacementKeepsZeroRecoveryContextAndStrictCounts(t *testing.T) {
	t.Parallel()
	fixture := newMutationFixture(t)
	view := fixture.g.state().Views()
	placementCtx, err := fixture.g.state().RecoveryPlacementContext()
	if err != nil {
		t.Fatal(err)
	}
	edited := view.Latest.Clone()
	edited.Comment = "recovery import audit text cannot authorize a deficit"
	edited.Group("work").Placement = &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}
	edited.Group("work").Replicas = edited.Group("work").Replicas[:1]
	if err := recoveryplacement.CheckLoadBounds(placementCtx, view.Target, edited); err == nil {
		t.Fatal("ordinary service count gate accepted a deficit")
	}
	before := view.Latest.Clone()
	valid := view.Latest.Clone()
	valid.ChangedBy = "operator"
	valid.Group("work").Labels = map[string]string{"description": "unchanged policy"}
	expected, err := topology.PreparePlacement(topology.ReadView{Latest: before, Target: view.Target}, valid, fixture.g.rules, time.Unix(100, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	actual := valid.Clone()
	if err := fixture.g.preparePlacementEdit(before, &actual, time.Unix(100, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected.Document, actual) {
		t.Fatal("absent recovery context changed ordinary preparation")
	}
	if !reflect.DeepEqual(before, fixture.g.state().Latest()) {
		t.Fatal("service preparation mutated the owned FSM view")
	}
}
