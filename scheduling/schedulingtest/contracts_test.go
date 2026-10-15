//go:build !functional

package schedulingtest

import (
	"context"
	"testing"

	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

type callbackPolicy struct {
	call func(scheduling.ScheduleInput)
}

func (p callbackPolicy) Schedule(_ context.Context, in scheduling.ScheduleInput) (scheduling.ScheduleDecision, error) {
	p.call(in)
	return scheduling.ScheduleDecision{}, nil
}

func TestContractOwnsInputAndHidesStagedIntent(t *testing.T) {
	t.Parallel()
	group := &topology.Group{ID: "g", Labels: map[string]string{"a": "b"}}
	in := scheduling.ScheduleInput{Groups: map[string]topology.GroupProgress{"g": {Desired: group, Approved: group}}}
	factory := func() scheduling.RepairPolicy {
		return callbackPolicy{call: func(input scheduling.ScheduleInput) {
			if input.Groups["g"].Desired != nil {
				t.Fatal("conformance callback received staged intent")
			}
			input.Groups["g"].Approved.Labels["a"] = "scratch"
		}}
	}
	CheckRepairPolicy(t, factory, []RepairPolicyCase{{Name: "owned progress", Input: in}})
}
