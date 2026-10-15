package scheduling

import schedulingapi "github.com/asalimonov/gquorum/scheduling"

func policyInput(in schedulingapi.ScheduleInput) schedulingapi.ScheduleInput {
	owned := in.Clone()
	for id, progress := range owned.Groups {
		progress.Desired = nil
		owned.Groups[id] = progress
	}
	return owned
}
