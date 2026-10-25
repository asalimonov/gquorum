package inspect

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/internal/recovery/model"
)

// ClassifyReplicas projects metadata-proven local classes. The owned WAL scan
// separately matches physical pins and rejects every unrecognized live artifact;
// a declaration alone never establishes data availability or donor eligibility.
//
//revive:disable-next-line:function-result-limit The target separates classified rows from collected operator issues.
func ClassifyReplicas(ctx context.Context, state SourceState) ([]model.ReplicaEvidence, []model.Issue, error) {
	var rows []model.ReplicaEvidence
	for _, input := range state.inventory.Replicas {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if input.Identity.NodeName != state.identity.NodeName ||
			input.Identity.ClusterID != state.identity.ClusterID {
			continue
		}
		row := input
		if retiredByAuthority(state.authority, row.Identity) {
			row.Classification = classRetired
		}
		if slices.Contains(state.inventory.Retired, row.Identity.GroupID) {
			row.Classification = classDeleted
		}
		rows = append(rows, row)
	}
	return rows, nil, nil
}
