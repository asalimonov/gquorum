//go:build !functional

package main

import (
	"testing"

	"github.com/google/pprof/profile"
)

func TestDecodeInlineAndUnknown(t *testing.T) {
	t.Parallel()
	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "cpu", Unit: "nanoseconds"}, {Type: "samples", Unit: "count"}},
		Sample: []*profile.Sample{{Value: []int64{20, 2}, Location: []*profile.Location{
			{Line: []profile.Line{
				{Function: &profile.Function{Name: "inner"}, Line: 10},
				{Function: &profile.Function{Name: "outer"}, Line: 20},
			}},
			{Address: 123},
		}}},
	}
	out, err := decode(p)
	if err != nil {
		t.Fatal(err)
	}
	s := out.Samples[0]
	if s.CPU != 20 || s.Count != 2 || len(s.Frames) != 3 || s.Frames[0].Name != "inner" ||
		s.Frames[1].Name != "outer" || s.Frames[2].Name != "[unknown@0x7b]" {
		t.Fatalf("unexpected sample: %+v", s)
	}
}
func TestRejectNonCPU(t *testing.T) {
	t.Parallel()
	if _, err := decode(&profile.Profile{}); err == nil {
		t.Fatal("accepted a non-CPU profile")
	}
}

func TestRejectDifferentialAndPreserveEmptyStack(t *testing.T) {
	t.Parallel()
	for _, values := range [][]int64{{-1, 1}, {1, -1}, {0, 0}} {
		p := &profile.Profile{
			SampleType: []*profile.ValueType{{Type: "cpu", Unit: "nanoseconds"}, {Type: "samples", Unit: "count"}},
			Sample:     []*profile.Sample{{Value: values}},
		}
		out, err := decode(p)
		if values[0] < 0 || values[1] < 0 {
			if err == nil {
				t.Fatal("accepted a differential sample")
			}
			continue
		}
		if err != nil || len(out.Samples) != 1 || len(out.Samples[0].Frames) != 1 ||
			out.Samples[0].Frames[0].Name != "[unknown stack]" {
			t.Fatalf("lost unknown stack: %+v, %v", out, err)
		}
	}
}
