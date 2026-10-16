package scheduling

import (
	"github.com/asalimonov/gquorum/internal/metaengine"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

func policyInput(in schedulingapi.ScheduleInput) schedulingapi.ScheduleInput {
	owned := in.Clone()
	for id, progress := range owned.Groups {
		progress.Desired, progress.Observation = nil, nil
		owned.Groups[id] = progress
	}
	return owned
}

// Removed at Task78 scoped activation. This labels actual schema3 status, without creating
// a synthetic authoritative document.
func legacyScheduleInput(in schedulingapi.ScheduleInput, views topology.ReadView) schedulingapi.ScheduleInput {
	owned := in.Clone()
	owned.Groups = make(map[string]topology.GroupProgress)
	for _, row := range metaengine.LegacyGroupProgress(views) {
		owned.Groups[row.GroupID] = row
	}
	for id, status := range owned.Status {
		status.SourceVersion = views.Active.Version
		owned.Status[id] = status
	}
	return owned
}
