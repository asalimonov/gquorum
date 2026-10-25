//go:build !functional

package loadgrpc

import (
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load/snapshot"
)

func snapshotAckState(t *testing.T) *snapshotClientState {
	t.Helper()
	send := snapshotWireSend(t)
	options, err := (snapshot.Options{}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	ack := snapshot.Ack{Session: snapshot.Session{Key: send.Manifest.Key, Attachment: 1}, ChunkBytes: send.Manifest.ChunkBytes,
		State: snapshot.DeliveryReceiving, RemainingTotal: time.Second, ProgressTimeout: time.Second, ReceiverRunID: "receiver"}
	return &snapshotClientState{t: &Transport{options: Options{MaxMessageBytes: defaultMaxMessageBytes}}, send: send, options: options,
		ack: ack, receiverRun: "receiver", chunk: ack.ChunkBytes, admitted: true, highestSent: send.Manifest.TotalBytes,
		attemptSent: send.Manifest.TotalBytes, deadline: time.Now().Add(time.Second), lastProgress: time.Now(), progressTimeout: time.Second}
}
func TestSnapshotClientRejectsInvalidCumulativeAcknowledgements(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"delivery", "receiver", "attachment", "chunk", "unaligned", "unsent", "premature-accept"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			state := snapshotAckState(t)
			ack := state.ack
			switch kind {
			case "delivery":
				ack.Session.Key.DeliveryID++
			case "receiver":
				ack.ReceiverRunID = "successor"
			case "attachment":
				ack.Session.Attachment++
			case "chunk":
				ack.ChunkBytes /= 2
			case "unaligned":
				ack.NextOffset = 1
			case "unsent":
				state.attemptSent = uint64(state.chunk)
				ack.NextOffset = 2 * uint64(state.chunk)
			case "premature-accept":
				ack.State = snapshot.DeliveryAccepted
				ack.NextOffset = state.send.Manifest.TotalBytes
			}
			wire, err := encodeSnapshotAck(ack, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = state.observe(wire, snapshotAckStreaming)
			var detail *snapshot.TransferError
			if !errors.As(err, &detail) {
				t.Fatal("invalid acknowledgement accepted", ack, err)
			}
		})
	}
}
func TestSnapshotClientDuplicateAckDoesNotRefreshProgress(t *testing.T) {
	t.Parallel()
	state := snapshotAckState(t)
	before := time.Now().Add(-time.Millisecond)
	state.lastProgress = before
	calls := 0
	state.send.ReportProgress = func(snapshot.Progress) { calls++ }
	wire, err := encodeSnapshotAck(state.ack, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.observe(wire, snapshotAckStreaming); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || !state.lastProgress.Equal(before) {
		t.Fatal("duplicate acknowledgement renewed progress")
	}
}

func TestSnapshotClientLateAckCannotRenewExpiredClock(t *testing.T) {
	t.Parallel()
	state := snapshotAckState(t)
	state.lastProgress = time.Now().Add(-2 * time.Second)
	ack := state.ack
	ack.NextOffset = uint64(state.chunk)
	wire, err := encodeSnapshotAck(ack, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = state.observe(wire, snapshotAckStreaming)
	var detail *snapshot.TransferError
	if !errors.As(err, &detail) || detail.Reason != snapshot.ReasonProgressTimeout {
		t.Fatal(err)
	}
	if state.ack.NextOffset != 0 {
		t.Fatal("late Ack advanced expired transfer")
	}
}
