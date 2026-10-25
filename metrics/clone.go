package metrics

// cloneSlice always returns an array-shaped slice, including for absent input.
func cloneSlice[T any](in []T) []T {
	return append(make([]T, 0, len(in)), in...)
}

func validity(v Validity) Validity {
	if v == "" {
		return Unknown
	}
	return v
}

// Clone copies sample slices and normalizes absent validity states.
func (s ReplicaSample) Clone() ReplicaSample {
	s.Meter.State = validity(s.Meter.State)
	s.Raft.State = validity(s.Raft.State)
	s.Writer.State = validity(s.Writer.State)
	s.Rates.State = validity(s.Rates.State)
	s.ResultLatency.State = validity(s.ResultLatency.State)
	s.PersistenceLatency.State = validity(s.PersistenceLatency.State)
	s.ApplyLatency.State = validity(s.ApplyLatency.State)
	s.Voters = cloneSlice(s.Voters)
	s.Learners = cloneSlice(s.Learners)
	s.Progress = cloneSlice(s.Progress)
	for i := range s.Progress {
		if p := s.Progress[i].Snapshot; p != nil {
			value := *p
			s.Progress[i].Snapshot = &value
		}
	}
	return s
}

// Clone owns the sample and evidence slices and normalizes unknown states.
func (s ReplicaObservation) Clone() ReplicaObservation {
	s.State = validity(s.State)
	s.ReplicationLag.State = validity(s.ReplicationLag.State)
	s.ApplyBacklog.State = validity(s.ApplyBacklog.State)
	s.Evidence.UnavailableFor.State = validity(s.Evidence.UnavailableFor.State)
	s.Evidence.ReplicationLagFor.State = validity(s.Evidence.ReplicationLagFor.State)
	s.Evidence.NoProgressFor.State = validity(s.Evidence.NoProgressFor.State)
	s.Evidence.StandbyReadyFor.State = validity(s.Evidence.StandbyReadyFor.State)
	if s.Reachability == "" {
		s.Reachability = "unknown"
	}
	s.Sample = s.Sample.Clone()
	s.Evidence.Reasons = cloneSlice(s.Evidence.Reasons)
	return s
}

// Clone copies all replica observations and normalizes group validity.
func (s GroupObservation) Clone() GroupObservation {
	s.State = validity(s.State)
	s.Replicas = cloneSlice(s.Replicas)
	for i := range s.Replicas {
		s.Replicas[i] = s.Replicas[i].Clone()
	}
	return s
}

// Clone copies every local and group observation into owned slices.
func (s ClusterSnapshot) Clone() ClusterSnapshot {
	s.Local = cloneSlice(s.Local)
	for i := range s.Local {
		s.Local[i] = s.Local[i].Clone()
	}
	s.Groups = cloneSlice(s.Groups)
	for i := range s.Groups {
		s.Groups[i] = s.Groups[i].Clone()
	}
	return s
}

// Clone copies the page and its nested observation data.
func (s HistoryPage) Clone() HistoryPage {
	s.Points = cloneSlice(s.Points)
	for i := range s.Points {
		s.Points[i] = s.Points[i].Clone()
	}
	return s
}
