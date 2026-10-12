package metrics

import (
	api "github.com/asalimonov/gquorum/metrics"
	"time"
)

// AgeObservation advances conservative source ages without manufacturing a
// producer generation or extending a confirmed predicate duration. Callers pass
// elapsed monotonic request/cache time, never a remote wall-clock difference.
func AgeObservation(s api.ReplicaObservation, elapsed, bound time.Duration) api.ReplicaObservation {
	s = s.Clone()
	s.Sample.Meter = ageStamp(s.Sample.Meter, elapsed, bound)
	s.Sample.Raft = ageStamp(s.Sample.Raft, elapsed, bound)
	s.Sample.Writer = ageStamp(s.Sample.Writer, elapsed, bound)
	if s.Sample.WALStall > 0 {
		s.Sample.WALStall = AddDuration(s.Sample.WALStall, elapsed)
	}
	for _, latency := range []*api.Latency{&s.Sample.ResultLatency, &s.Sample.PersistenceLatency, &s.Sample.ApplyLatency} {
		latency.LastAge = AddDuration(latency.LastAge, elapsed)
		*latency = staleLatency(*latency, bound)
	}
	for i := range s.Sample.Progress {
		s.Sample.Progress[i].AckAge = AddDuration(s.Sample.Progress[i].AckAge, elapsed)
	}
	if s.Sample.Meter.State != api.Current && s.Sample.Rates.State == api.Current {
		s.Sample.Rates.State = api.Stale
	}
	if s.Sample.Raft.State != api.Current {
		s.ApplyBacklog.State = s.Sample.Raft.State
	}
	if s.Sample.Meter.State == api.Stale || s.Sample.Raft.State == api.Stale || s.Sample.Writer.State == api.Stale {
		s.State = api.Stale
	}
	return s
}
