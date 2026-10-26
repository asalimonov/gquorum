//go:build functional

package raftengine

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestSubmissionApplyAndBusinessRejection(t *testing.T) {
	t.Parallel()
	g, m := singleton(t, openStore(t, t.TempDir(), "1"))
	for _, data := range []string{"value", "reject"} {
		p := Proposal{RequestID: "logical/" + data, Data: []byte(data)}
		out, err := g.Submit(testContext(t), p, "attempt/"+data, 0)
		if err != nil || out.Disposition != SubmissionApplied || !out.ResultAvailable || out.Index == 0 || out.Term == 0 ||
			out.Identity != g.Identity() || out.RunID != g.RunID() || out.RequestID != p.RequestID || out.AttemptID != "attempt/"+data {
			t.Fatalf("applied receipt: %+v %v", out, err)
		}
		if data == "reject" && out.Result.Code != "rejected" {
			t.Fatal("business rejection became transport refusal", out)
		}
	}
	if got := m.values(); len(got) != 1 || got[0] != "value" {
		t.Fatal(got)
	}
}

func TestSubmissionPreDispatchRefusal(t *testing.T) {
	t.Parallel()
	g, m := singleton(t, openStore(t, t.TempDir(), "1"))
	for _, id := range []string{"", " \t", "invalid\x00id", string([]byte{0xff})} {
		out, err := g.Submit(testContext(t), Proposal{RequestID: id, Data: []byte("never")}, "attempt", 0)
		if !errors.Is(err, fault.ErrInvalid) || out.Disposition != SubmissionNotSubmitted || out.RequestID != id {
			t.Fatal(out, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	out, err := g.Submit(ctx, Proposal{RequestID: "canceled", Data: []byte("never")}, "attempt/canceled", 0)
	if !errors.Is(err, context.Canceled) || out.Disposition != SubmissionNotSubmitted || len(m.values()) != 0 {
		t.Fatal(out, err, m.values())
	}
}

func TestSubmissionApplyFailureStopsExactHandle(t *testing.T) {
	t.Parallel()
	g, _ := singleton(t, openStore(t, t.TempDir(), "1"))
	out, err := g.Submit(testContext(t), Proposal{RequestID: "fatal", Data: []byte("fatal")}, "attempt/fatal", 0)
	if err == nil || out.Disposition != SubmissionIndeterminate || out.ResultAvailable || out.Index != 0 {
		t.Fatal("failed Apply claimed completion", out, err)
	}
	must(t, g.Stop(testContext(t)))
	out, err = g.Submit(testContext(t), Proposal{RequestID: "next", Data: []byte("next")}, "attempt/next", 0)
	if !errors.Is(err, fault.ErrStopped) || !errors.Is(err, fault.ErrUnavailable) || out.Disposition != SubmissionNotSubmitted {
		t.Fatal("halted engine classified as transient", out, err)
	}
}
