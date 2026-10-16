//go:build functional

package observe

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func multiGroupMetricsRound(doc topology.Document, seq uint64) (Round, []metrics.ReplicaObservation) {
	round, local := evidenceRound(doc, seq)
	for _, group := range doc.Groups[1:] {
		single := doc.Clone()
		single.Groups = []topology.Group{group}
		other, _ := evidenceRound(single, seq)
		samples := other.Groups[topology.MetaGroupID]
		for i := range samples.Probes {
			samples.Probes[i].Identity.GroupID = group.ID
			samples.Probes[i].Observation.Sample.Identity.GroupID = group.ID
		}
		round.Groups[group.ID] = samples
	}
	return round, local
}

func metricsGroup(t *testing.T, snapshot metrics.ClusterSnapshot, id string) metrics.GroupObservation {
	t.Helper()
	for _, group := range snapshot.Groups {
		if group.GroupID == id {
			return group
		}
	}
	t.Fatalf("missing group %s: %+v", id, snapshot.Groups)
	return metrics.GroupObservation{}
}

func TestMetricsIndependentSourceChangeKeepsSiblingHistory(t *testing.T) {
	t.Parallel()
	c, source, now := collectorFixture(t)
	for _, id := range []string{"a", "b"} {
		group := source.doc.Clone().Groups[0]
		group.ID = id
		source.doc.Groups = append(source.doc.Groups, group)
	}
	page := evidenceTopology(source.doc)
	source.page = &page
	source.round, source.local = multiGroupMetricsRound(source.doc, 1)
	refreshMetrics(t, c)
	initial := c.Snapshot()
	first := metricsGroup(t, initial, "b")
	victim, standby := first.Replicas[2], first.Replicas[3]
	for i := 1; i <= 62; i++ {
		*now = now.Add(time.Second)
		source.round, source.local = multiGroupMetricsRound(source.doc, uint64(i+1))
		for _, row := range page.Groups {
			samples := source.round.Groups[row.GroupID]
			samples.SourceVersion = row.ConvergedVersion
			if row.GroupID == "b" {
				samples.Probes[2].Reachability = "unreachable"
				samples.Probes[2].Observation = metrics.ReplicaObservation{}
			}
			source.round.Groups[row.GroupID] = samples
		}
		if i == 31 {
			source.collect = func(context.Context, *ReadPool) (Round, error) {
				source.mu.Lock()
				defer source.mu.Unlock()
				page.TargetVersion = 2
				page.Groups[1].ConvergedVersion = 2
				page.Groups[1].Converged.Replicas[2].ReplicaID = 33
				source.doc.Groups[1] = *page.Groups[1].Clone().Converged
				return source.round, nil
			}
		} else {
			source.collect = nil
		}
		refreshMetrics(t, c)
		snapshot := c.Snapshot()
		b := metricsGroup(t, snapshot, "b")
		failed, ready := b.Replicas[2], b.Replicas[3]
		if snapshot.CollectorRunID != initial.CollectorRunID || b.SourceVersion != 1 ||
			failed.SeriesID != victim.SeriesID || ready.SeriesID != standby.SeriesID ||
			failed.Sample.Identity != victim.Sample.Identity || failed.Sample.Meter.Sequence != 1 ||
			!failed.ReceivedAt.Equal(victim.ReceivedAt) || failed.State == metrics.Current {
			t.Fatal("sibling source/history or cached receive time changed", i, failed.SeriesID, failed.ReceivedAt, failed.State)
		}
		if failed.Evidence.UnavailableFor.State != metrics.Current || failed.Evidence.UnavailableFor.Value != time.Duration(i-1)*time.Second ||
			ready.Evidence.StandbyReadyFor.State != metrics.Current || ready.Evidence.StandbyReadyFor.Value != time.Duration(i)*time.Second ||
			ready.Sample.Meter.Sequence != uint64(i+1) || !ready.ReceivedAt.Equal(*now) || ready.Sequence != uint64(i+1) {
			t.Fatal("independent failure/catch-up window reset", i, failed, ready)
		}
		if i == 31 && slices.ContainsFunc(snapshot.Groups, func(g metrics.GroupObservation) bool { return g.GroupID == "a" }) {
			t.Fatal("round crossed changed assignment/source", snapshot)
		}
		if i > 31 {
			a := metricsGroup(t, snapshot, "a")
			if a.SourceVersion != 2 || !slices.ContainsFunc(a.Replicas, func(r metrics.ReplicaObservation) bool { return r.Sample.Identity.ReplicaID == 33 }) ||
				a.Replicas[2].Evidence.StandbyReadyFor.Value >= ready.Evidence.StandbyReadyFor.Value {
				t.Fatal("changed group inherited old window/identity", a)
			}
		}
	}
	history, err := c.History(metrics.HistoryQuery{SeriesID: standby.SeriesID})
	if err != nil || len(history.Points) != 3 || history.Next != 63 || !history.Truncated {
		t.Fatal("sibling cursor/history lost", history, err)
	}
	cached, err := c.History(metrics.HistoryQuery{SeriesID: victim.SeriesID})
	if err != nil || len(cached.Points) != 3 || !cached.Points[2].ReceivedAt.Equal(victim.ReceivedAt) {
		t.Fatal("cached history renewed sample receive time", cached, err)
	}
}
