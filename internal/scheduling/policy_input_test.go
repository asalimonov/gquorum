//go:build !functional

package scheduling

import (
	"testing"

	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

func TestPolicyInputStripsDesiredWithoutChangingSource(t *testing.T) {
	t.Parallel()
	group := &topology.Group{ID: "g", Labels: map[string]string{"a": "b"}}
	in := schedulingapi.ScheduleInput{Groups: map[string]topology.GroupProgress{"g": {Desired: group, Approved: group}}}
	owned := policyInput(in)
	if owned.Groups["g"].Desired != nil || in.Groups["g"].Desired != group {
		t.Fatal("policy received staged intent or input was changed")
	}
	owned.Groups["g"].Approved.Labels["a"] = "changed"
	if group.Labels["a"] != "b" {
		t.Fatal("policy received borrowed declarations")
	}
}
