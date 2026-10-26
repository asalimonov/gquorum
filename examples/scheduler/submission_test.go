//go:build !functional

package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestProposalReconcileYieldsBeforeNewStateDecision(t *testing.T) {
	t.Parallel()
	for _, d := range []load.SubmissionDisposition{
		load.SubmissionNotSubmitted, load.SubmissionIndeterminate, load.SubmissionApplied,
	} {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			err := waitProposalReconcile(t.Context(), load.Submission{Disposition: d}, fault.ErrUnavailable)
			if err != nil || time.Since(started) != schedulerTick {
				t.Fatal("temporary outcome did not yield for fresh reconciliation", d, err, time.Since(started))
			}
		})
	}
}

func TestProposalReconcileStopsTerminalErrors(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		for _, err := range []error{
			fault.New(fault.Stopped, "expired", fault.ErrNotLeader), fault.ErrInvalid, fault.ErrIdentityMismatch,
		} {
			got := waitProposalReconcile(t.Context(), load.Submission{}, err)
			if !errors.Is(got, err) || time.Since(started) != 0 {
				t.Fatal(got, time.Since(started))
			}
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := waitProposalReconcile(ctx, load.Submission{}, fault.ErrUnavailable); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
