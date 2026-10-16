//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestOperationLookupOwnsFullExactAggregate(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	id := s.Operations[0].Status.ID
	failure := scopedTransitionFor(s.Operations[0], "shard0", 3)
	failure.State, failure.Error = topology.OperationFailed, "unreachable"
	applyScopedTransitionFixture(t, &s, failure)
	got, err := queryOperationStatus(&s, id)
	if err != nil || got.ID != id || got.State.Terminal() || got.BlocksTruncated || len(got.Blocks) != 2 || got.TotalBlocks != 2 {
		t.Fatal(got, err)
	}
	got.Blocks[1].Error = "caller"
	again, err := queryOperationStatus(&s, id)
	if err != nil || again.Blocks[1].Error != "unreachable" {
		t.Fatal("lookup borrowed state", again, err)
	}
	for _, tc := range []struct {
		id   topology.OperationID
		want error
	}{{"", fault.ErrInvalid}, {"unknown", fault.ErrHistoryUnavailable}} {
		if _, err := queryOperationStatus(&s, tc.id); !errors.Is(err, tc.want) {
			t.Fatal(tc, err)
		}
	}
	if _, err := NewMachine(testRules(), nil).Operation(id); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("schema3 exposed scoped state", err)
	}
}
