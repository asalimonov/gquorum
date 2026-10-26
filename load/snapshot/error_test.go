//go:build !functional

package snapshot

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestTransferErrorClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		reason string
		want   error
	}{
		{ReasonSizeLimit, fault.ErrInvalid}, {ReasonFrameLimit, fault.ErrInvalid},
		{ReasonMalformed, fault.ErrInvalid}, {ReasonTransferConflict, fault.ErrInvalid},
		{ReasonUnsupported, fault.ErrUnsupported}, {ReasonCapacity, fault.ErrOverloaded},
		{ReasonGroupBusy, fault.ErrOverloaded}, {ReasonIdentity, fault.ErrIdentityMismatch},
		{ReasonChecksum, fault.ErrUnreadable}, {ReasonRestartRequired, fault.ErrUnavailable},
		{ReasonObsolete, fault.ErrUnavailable}, {ReasonProgressTimeout, fault.ErrUnavailable},
		{ReasonTotalTimeout, fault.ErrUnavailable}, {ReasonStopped, fault.ErrStopped},
		{ReasonIngressFailed, fault.ErrUnavailable}, {"future_reason", fault.ErrUnavailable},
		{"", fault.ErrUnavailable},
	}
	for _, tt := range cases {
		t.Run(tt.reason, func(t *testing.T) {
			t.Parallel()
			cause := errors.New("original cause")
			err := &TransferError{Reason: tt.reason, Size: 99, Limit: 64, Cause: cause}
			if !errors.Is(err, tt.want) || !errors.Is(err, cause) {
				t.Fatalf("classification or cause lost: %v", err)
			}
			var detail *TransferError
			var classified *fault.Error
			if !errors.As(err, &detail) || detail.Size != 99 || detail.Limit != 64 ||
				!errors.As(err, &classified) {
				t.Fatal("structured error evidence lost")
			}
			if !strings.Contains(err.Error(), "size=99 limit=64") || !strings.Contains(err.Error(), cause.Error()) {
				t.Fatal("diagnostic lacks sizes/cause", err)
			}
		})
	}
}

func TestOversizeIsNotTransientCapacity(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{ReasonSizeLimit, ReasonFrameLimit, ReasonUnsupported} {
		err := &TransferError{Reason: reason}
		if errors.Is(err, fault.ErrOverloaded) || errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("permanent refusal classified as transient pressure", err)
		}
	}
	err := &TransferError{Reason: ReasonTotalTimeout, Cause: context.DeadlineExceeded}
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("timeout cause/classification lost", err)
	}
}

func TestZeroValuesDoNotClaimSuccess(t *testing.T) {
	t.Parallel()
	var ack Ack
	var status Status
	var page Page
	var err TransferError
	if ack.State == DeliveryAccepted || ack.State == DeliveryNotNeeded ||
		status.ReceiverOutcome == ReceiverInstalled || status.ReceiverOutcome == ReceiverSkipped ||
		len(page.Items) != 0 || !errors.Is(&err, fault.ErrUnavailable) {
		t.Fatal("zero-value observations must remain conservative")
	}
}
