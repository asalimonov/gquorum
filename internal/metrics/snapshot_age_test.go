//go:build !functional

package metrics

import (
	"math"
	"testing"
	"time"

	api "github.com/asalimonov/gquorum/metrics"
)

func TestSnapshotObservationAgesEveryRelativeBound(t *testing.T) {
	t.Parallel()
	source := api.ReplicaObservation{Sample: api.ReplicaSample{Progress: []api.PeerProgress{{Snapshot: &api.SnapshotCatchUp{
		ProgressAge: time.Second, RemainingTotal: 5 * time.Second, RemainingGrace: 3 * time.Second, EpisodeRemaining: 4 * time.Second}}}}}
	aged := AgeObservation(source, 2*time.Second, time.Minute).Sample.Progress[0].Snapshot
	if aged.ProgressAge != 3*time.Second || aged.RemainingTotal != 3*time.Second || aged.RemainingGrace != time.Second || aged.EpisodeRemaining != 2*time.Second {
		t.Fatal(aged)
	}
	if source.Sample.Progress[0].Snapshot.ProgressAge != time.Second {
		t.Fatal("aging mutated cached producer data")
	}
	AgeSnapshot(aged, 10*time.Second)
	if aged.RemainingTotal != 0 || aged.RemainingGrace != 0 || aged.EpisodeRemaining != 0 {
		t.Fatal(aged)
	}
	invalid := &api.SnapshotCatchUp{ProgressAge: -1, RemainingTotal: time.Duration(math.MinInt64), RemainingGrace: -1, EpisodeRemaining: -1}
	AgeSnapshot(invalid, time.Second)
	if invalid.ProgressAge >= 0 || invalid.RemainingTotal != 0 || invalid.RemainingGrace != 0 || invalid.EpisodeRemaining != 0 {
		t.Fatal("invalid duration wrapped into valid protection", invalid)
	}
}
