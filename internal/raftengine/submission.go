package raftengine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// SubmissionDisposition is conservative evidence for one application attempt.
type SubmissionDisposition uint8

// Submission dispositions are intentionally zero-conservative.
const (
	SubmissionIndeterminate SubmissionDisposition = iota
	SubmissionNotSubmitted
	SubmissionApplied
)

// Submission is the engine-owned evidence returned through the load facade.
type Submission struct {
	AttemptID, RequestID string
	Identity             storage.Identity
	RunID                string
	Disposition          SubmissionDisposition
	Index, Term          uint64
	Result               Result
	ResultAvailable      bool
}

// Identity returns the immutable durable identity of this engine.
func (g *Group) Identity() storage.Identity {
	return storage.Identity{NodeName: g.opts.NodeName, ClusterID: g.opts.ClusterID,
		GroupID: g.opts.GroupID, ReplicaID: g.opts.ReplicaID}
}

func (g *Group) submissionBase(p Proposal, attemptID string) Submission {
	return Submission{AttemptID: attemptID, RequestID: p.RequestID, Identity: g.Identity(), RunID: g.RunID()}
}

func validSubmissionID(id string) bool {
	return utf8.ValidString(id) && strings.TrimSpace(id) != "" && !strings.ContainsRune(id, 0)
}

// Submit returns evidence for one load proposal, optionally admitted only in
// expectedTerm. It never deduplicates application requests or rebinds replicas.
//
//revive:disable-next-line:confusing-naming // Receipt entrypoint; submit is the legacy queue helper.
func (g *Group) Submit(parent context.Context, p Proposal, attemptID string,
	expectedTerm uint64) (out Submission, err error) {
	out = g.submissionBase(p, attemptID)
	refused := out
	refused.Disposition = SubmissionNotSubmitted
	if g.opts.GroupID == topology.MetaGroupID || !validSubmissionID(p.RequestID) || !validSubmissionID(attemptID) {
		return refused, fault.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(parent, g.opts.ProposalTimeout)
	defer cancel()
	started := time.Now()
	g.meter.ObserveProposal()
	defer func() { g.observeProposalResult(started, err) }()
	if err := ctx.Err(); err != nil {
		return refused, err
	}
	g.mu.RLock()
	accepting := g.started && !g.stopping && !g.stopped && g.ctx.Err() == nil
	g.mu.RUnlock()
	if !accepting {
		return refused, g.stoppedSubmissionError()
	}
	if g.WALStall() > time.Second {
		return refused, fault.ErrOverloaded
	}
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		return refused, fault.ErrOverloaded
	}
	req := &request{ctx: ctx, correlation: attemptID, expectedTerm: expectedTerm,
		proposal:         Proposal{RequestID: p.RequestID, Data: bytes.Clone(p.Data)},
		reportSubmission: true, reply: make(chan response, 1)}
	enqueue, stopEnqueue := context.WithTimeout(ctx, 5*time.Second)
	defer stopEnqueue()
	// TODO(not planned): guaranteed queued-payload release on retained stopped
	// handles needs a separate lifecycle change; reuse existing enqueue bounds.
	select {
	case g.proposals <- req:
		return g.waitSubmission(ctx, out, req)
	case <-enqueue.Done():
		return refused, enqueue.Err()
	case <-g.done:
		return refused, g.stoppedSubmissionError()
	}
}

func (g *Group) stoppedSubmissionError() error {
	cause := g.exitError()
	if errors.Is(cause, fault.ErrStopped) {
		return cause
	}
	return fault.New(fault.Stopped, "replica engine is not accepting proposals", cause)
}

func (g *Group) waitSubmission(ctx context.Context, base Submission, req *request) (Submission, error) {
	var waitErr error
	select {
	case r := <-req.reply:
		return submissionResponse(base, r)
	case <-ctx.Done():
		waitErr = ctx.Err()
	case <-g.done:
		waitErr = g.stoppedSubmissionError()
	}
	select {
	case r := <-req.reply:
		return submissionResponse(base, r)
	default:
		return base, waitErr
	}
}

func submissionResponse(base Submission, r response) (Submission, error) {
	switch r.disposition {
	case SubmissionApplied:
		base.Disposition, base.Index, base.Term = SubmissionApplied, r.index, r.term
		base.ResultAvailable = r.resultAvailable
		if r.resultAvailable {
			base.Result = r.result
		}
	case SubmissionNotSubmitted:
		base.Disposition = SubmissionNotSubmitted
	default: // Missing evidence is indeterminate, regardless of the error code.
		base.Disposition = SubmissionIndeterminate
	}
	return base, r.err
}

func (g *Group) refuseProposal(req *request, err error) {
	delete(g.props, req.correlation)
	if !req.reportSubmission {
		answer(req, Result{}, err)
		return
	}
	req.reply <- response{err: err, disposition: SubmissionNotSubmitted}
}

func (*Group) answerApplied(req *request, entry raftpb.Entry, result Result) {
	if !req.reportSubmission {
		answer(req, result, nil)
		return
	}
	// TODO(not planned): result-size policy belongs to GQ-06; an adapter that
	// strips a known applied result must preserve its disposition and position.
	owned := Result{Code: result.Code, Data: bytes.Clone(result.Data)}
	req.reply <- response{result: owned, disposition: SubmissionApplied,
		index: entry.Index, term: entry.Term, resultAvailable: true}
}

func (g *Group) observeProposalResult(started time.Time, err error) {
	outcome := internalmetrics.Applied
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		outcome = internalmetrics.TimedOut
	case errors.Is(err, context.Canceled):
		outcome = internalmetrics.Canceled
	case errors.Is(err, fault.ErrOverloaded):
		outcome = internalmetrics.Overloaded
	case err != nil:
		outcome = internalmetrics.Unavailable
	default: // Complete applied result.
	}
	g.meter.ObserveResult(time.Since(started), outcome)
}
