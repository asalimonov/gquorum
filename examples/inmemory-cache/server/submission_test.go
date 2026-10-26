//go:build !functional

package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestProposalErrorReplyPreservesOutcome(t *testing.T) {
	t.Parallel()
	cases := []struct {
		disposition load.SubmissionDisposition
		err         error
		prefix      string
	}{
		{load.SubmissionNotSubmitted, fault.ErrOverloaded, "-TRYAGAIN "},
		{load.SubmissionNotSubmitted, fault.New(fault.Stopped, "halted", fault.ErrUnavailable), "-ERR stopped: halted"},
		{load.SubmissionIndeterminate, context.DeadlineExceeded, "-ERR proposal outcome unknown"},
		{load.SubmissionApplied, load.ErrResultUnavailable, "-ERR proposal applied; result unavailable"},
		{load.SubmissionApplied, context.DeadlineExceeded, "-ERR proposal applied; result unavailable"},
	}
	for _, tc := range cases {
		var output bytes.Buffer
		w := resp.NewWriter(&output)
		replyProposalError(w, load.Submission{Disposition: tc.disposition}, tc.err)
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(output.String(), tc.prefix) {
			t.Fatal(output.String(), tc.prefix)
		}
	}
}

func TestReadErrorReplyKeepsReadRetryPolicy(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	w := resp.NewWriter(&output)
	replyError(w, errors.Join(context.DeadlineExceeded, errors.New("read wait")))
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "-TRYAGAIN ") {
		t.Fatal(output.String())
	}
}
