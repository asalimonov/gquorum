//go:build !functional

package raftengine

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

func submissionTestBase() Submission {
	return Submission{AttemptID: "attempt", RequestID: "logical",
		Identity: storage.Identity{ClusterID: "c", NodeName: "n", GroupID: "g", ReplicaID: 1}, RunID: "run"}
}

func TestSubmissionPublishedReplyWinsCancellationAndStop(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	g := &Group{done: make(chan struct{}), err: fault.ErrUnavailable}
	close(g.done)
	for _, r := range []response{
		{disposition: SubmissionApplied, result: Result{Code: "rejected"}, resultAvailable: true, index: 8, term: 2},
		{disposition: SubmissionApplied, index: 8, term: 2, err: fault.ErrInvalid},
		{disposition: SubmissionNotSubmitted, err: fault.ErrNotLeader},
	} {
		req := &request{reply: make(chan response, 1)}
		req.reply <- r
		out, err := g.waitSubmission(ctx, submissionTestBase(), req)
		if out.Disposition != r.disposition || out.AttemptID != "attempt" ||
			out.RequestID != "logical" || out.Index != r.index || out.ResultAvailable != r.resultAvailable || err != r.err {
			t.Fatalf("published response lost: %+v %v", out, err)
		}
	}
}

func TestSubmissionUnpublishedOutcomeRemainsUnknown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	g := &Group{done: make(chan struct{})}
	req := &request{reply: make(chan response, 1), reportSubmission: true}
	out, err := g.waitSubmission(ctx, submissionTestBase(), req)
	if !errors.Is(err, context.Canceled) || out.Disposition != SubmissionIndeterminate || out.Index != 0 || out.ResultAvailable {
		t.Fatalf("cancellation fabricated evidence: %+v %v", out, err)
	}
	// A late owner publication fits without waiting for the caller to return.
	select {
	case req.reply <- response{disposition: SubmissionApplied, index: 9, term: 3, resultAvailable: true}:
	default:
		t.Fatal("owner publication blocked after cancellation")
	}
	if out.Disposition != SubmissionIndeterminate {
		t.Fatal("returned receipt changed after owner publication")
	}
}

func TestSubmissionResponsesPreserveProofAndOwnResults(t *testing.T) {
	t.Parallel()
	g := &Group{props: make(map[string]*request)}
	req := &request{correlation: "attempt", reportSubmission: true, reply: make(chan response, 1)}
	g.props[req.correlation] = req
	g.refuseProposal(req, fault.ErrOverloaded)
	refused, err := submissionResponse(submissionTestBase(), <-req.reply)
	if refused.Disposition != SubmissionNotSubmitted || !errors.Is(err, fault.ErrOverloaded) || len(g.props) != 0 {
		t.Fatal(refused, err)
	}
	answer(req, Result{}, fault.ErrOverloaded)
	unknown, err := submissionResponse(submissionTestBase(), <-req.reply)
	if unknown.Disposition != SubmissionIndeterminate || !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("generic fault supplied refusal proof", unknown, err)
	}
	result := Result{Code: "ok", Data: []byte("owned")}
	g.answerApplied(req, raftpb.Entry{Index: 7, Term: 2}, result)
	result.Data[0] = 'x'
	applied, err := submissionResponse(submissionTestBase(), <-req.reply)
	if err != nil || !applied.ResultAvailable || applied.Index != 7 || string(applied.Result.Data) != "owned" {
		t.Fatal("applied response lost position or ownership", applied, err)
	}
	g.answerApplied(req, raftpb.Entry{Index: 8, Term: 2}, Result{})
	empty, err := submissionResponse(submissionTestBase(), <-req.reply)
	if err != nil || !empty.ResultAvailable || empty.Disposition != SubmissionApplied {
		t.Fatal("empty result confused with unavailable", empty, err)
	}
}
