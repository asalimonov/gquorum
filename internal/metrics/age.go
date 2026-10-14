package metrics

import (
	"time"

	api "github.com/asalimonov/gquorum/metrics"
)

// AgeObservation advances conservative source ages without manufacturing a
// producer generation or extending a confirmed predicate duration. Callers pass
// elapsed monotonic request/cache time, never a remote wall-clock difference.
func AgeObservation(s api.ReplicaObservation, elapsed, bound time.Duration) api.ReplicaObservation {
	aged := s.Clone()
	aged.Sample.Meter = ageStamp(aged.Sample.Meter, elapsed, bound)
	aged.Sample.Raft = ageStamp(aged.Sample.Raft, elapsed, bound)
	aged.Sample.Writer = ageStamp(aged.Sample.Writer, elapsed, bound)
	if aged.Sample.WALStall > 0 {
		aged.Sample.WALStall = AddDuration(aged.Sample.WALStall, elapsed)
	}
	for _, latency := range []*api.Latency{&aged.Sample.ResultLatency, &aged.Sample.PersistenceLatency,
		&aged.Sample.ApplyLatency} {
		latency.LastAge = AddDuration(latency.LastAge, elapsed)
		*latency = staleLatency(*latency, bound)
	}
	for i := range aged.Sample.Progress {
		aged.Sample.Progress[i].AckAge = AddDuration(aged.Sample.Progress[i].AckAge, elapsed)
	}
	if aged.Sample.Meter.State != api.Current && aged.Sample.Rates.State == api.Current {
		aged.Sample.Rates.State = api.Stale
	}
	if aged.Sample.Raft.State != api.Current {
		aged.ApplyBacklog.State = aged.Sample.Raft.State
	}
	if aged.Sample.Meter.State == api.Stale || aged.Sample.Raft.State == api.Stale ||
		aged.Sample.Writer.State == api.Stale {
		aged.State = api.Stale
	}
	return aged
}
