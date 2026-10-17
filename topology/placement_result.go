package topology

import (
	"github.com/asalimonov/gquorum/fault"
)

// PlacementReport summarizes group placement against captured desired and
// approved versions.
type PlacementReport struct {
	Version         uint64 `json:"version"`
	ApprovedVersion uint64 `json:"approved_version"`
	// Source labels status snapshots as approved intent or a proven checkpoint.
	// Mutation reports leave it empty; it is descriptive, never authorization.
	Source         string                 `json:"source,omitempty"`
	AffectedGroups []string               `json:"affected_groups,omitempty"`
	Groups         []GroupPlacementReport `json:"groups"`
}

// PlacementError carries classified placement failure with its detailed report.
type PlacementError struct {
	Code   fault.Code
	Report PlacementReport
}
