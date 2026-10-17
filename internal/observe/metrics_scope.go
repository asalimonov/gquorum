package observe

import (
	"reflect"

	"github.com/asalimonov/gquorum/topology"
)

// A round may straddle an independent approval. Keep only unchanged captured
// sources. Do not use TargetVersion, node checkpoints or a composite document
// as a freshness fence; exact process/run identity is checked by each probe.
func filterMetricsRound(before, after topology.ProgressPage, round Round) Round {
	prior := make(map[string]topology.GroupProgress, len(before.Groups))
	for _, row := range before.Groups {
		prior[row.GroupID] = row
	}
	filtered := Round{Epoch: round.Epoch, Groups: make(map[string]GroupSamples)}
	for _, row := range after.Groups {
		old, found := prior[row.GroupID]
		samples, sampled := round.Groups[row.GroupID]
		if !found || !sampled || row.ConvergedVersion == 0 || row.Converged == nil ||
			old.ConvergedVersion != row.ConvergedVersion || samples.SourceVersion != row.ConvergedVersion ||
			!reflect.DeepEqual(old.Converged, row.Converged) {
			continue
		}
		filtered.Groups[row.GroupID] = samples
	}
	return filtered
}
