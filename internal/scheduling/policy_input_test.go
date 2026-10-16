//go:build !functional

package scheduling

import (
	"testing"

	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

func TestPolicyInputStripsStagedAndDuplicateObservationsWithoutChangingSource(t *testing.T) {
	t.Parallel()
	group := &topology.Group{ID: "g", Labels: map[string]string{"a": "b"}}
	in := schedulingapi.ScheduleInput{Groups: map[string]topology.GroupProgress{"g": {Desired: group, Approved: group, Observation: &topology.GroupStatus{GroupID: "g", LeaderID: 1}}}}
	owned := policyInput(in)
	if owned.Groups["g"].Desired != nil || owned.Groups["g"].Observation != nil ||
		in.Groups["g"].Desired != group || in.Groups["g"].Observation == nil {
		t.Fatal("policy received staged intent or input was changed")
	}
	owned.Groups["g"].Approved.Labels["a"] = "changed"
	if group.Labels["a"] != "b" {
		t.Fatal("policy received borrowed declarations")
	}
}
