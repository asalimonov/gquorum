//go:build !functional

package loadsnapshot

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3/raftpb"
)

type refusalTransport struct{}

func (refusalTransport) SendSnapshot(context.Context, snapshot.Send) (snapshot.Ack, error) {
	panic("refused send reached transport")
}

func refusalPorts() Ports {
	return Ports{
		Transport:        refusalTransport{},
		ResolvePeer:      func(context.Context, Owner, uint64) (load.Peer, error) { panic("refusal resolved destination") },
		ValidateOutgoing: func(context.Context, Owner, load.Peer) error { panic("refusal validated destination") },
		RegisterDelivery: func(Owner, raftengine.SnapshotAttempt, load.Peer, time.Time) (uint64, error) {
			panic("refusal issued delivery")
		},
		ReportDelivery: func(Owner, snapshot.Key, snapshot.Ack, error) { panic("refusal reported issued delivery") },
		ReportSnapshotFailure: func(Owner, raftengine.SnapshotAttempt, error) {
			panic("PrepareSend must leave native report to caller")
		},
		NoteSnapshotProgress: func(Owner, raftengine.SnapshotAttempt, metrics.SnapshotCatchUp) { panic("refusal fabricated progress") },
	}
}

func refusalOffer(ctx context.Context, owner OwnerKey, size int) raftengine.SnapshotOffer {
	return raftengine.SnapshotOffer{Message: raftpb.Message{Type: raftpb.MsgSnap, From: owner.Peer.Replica.ReplicaID, To: 2, Term: 3,
		Snapshot: &raftpb.Snapshot{Data: make([]byte, size), Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}},
		Attempt: raftengine.SnapshotAttempt{RunID: owner.RunID, Term: 3, ReplicaID: 2, Generation: 1}, PublicationGeneration: 1, Epoch: ctx}
}

func TestSnapshotSendRefusalVisibleWithoutAdmissionOrCallbacks(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{snapshot.ReasonSizeLimit, snapshot.ReasonCapacity, snapshot.ReasonUnsupported} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			c := budgetCoordinator(t, snapshot.Options{MaxSnapshotBytes: 64})
			c.ports = refusalPorts()
			owner := budgetOwnerRef("group")
			size := 32
			if reason == snapshot.ReasonSizeLimit {
				size = 65
			}
			if reason == snapshot.ReasonCapacity {
				reservation, err := c.reserve(outgoing, owner, 32)
				if err != nil {
					t.Fatal(err)
				}
				defer c.release(reservation)
			}
			if reason == snapshot.ReasonUnsupported {
				c.ports.Transport = nil
			}
			offer := refusalOffer(t.Context(), owner.key, size)
			for i := 0; i < 50; i++ {
				job, err := c.PrepareSend(owner.handle, offer)
				if job != nil {
					t.Fatal("refusal created a job")
				}
				wantTransferReason(t, err, reason)
			}
			page := c.List(snapshot.Query{})
			if len(page.Items) != 1 {
				t.Fatal("refusal history not coalesced", len(page.Items))
			}
			row := page.Items[0]
			if !row.AdmissionRefused || row.Refusals != 50 || row.ErrorReason != reason || row.TotalBytes != uint64(size) || row.ReservedBytes != 0 || row.VerifiedBytes != 0 || !row.LastProgressAt.IsZero() || row.LastRefusedAt.IsZero() {
				t.Fatal(row)
			}
			if row.Key.DeliveryID != 0 || row.Key.From != owner.key.Peer || row.Key.To.Replica.ReplicaID != 2 || row.ReceiverOutcome != snapshot.ReceiverUnknown {
				t.Fatal(row)
			}
			if reason == snapshot.ReasonSizeLimit && (row.ErrorSize != 65 || row.ErrorLimit != 64) {
				t.Fatal("size dimensions missing", row)
			}
			if reason != snapshot.ReasonCapacity && (page.Outgoing != 0 || page.SendBytes != 0) {
				t.Fatal("refusal pinned bytes", page)
			}
		})
	}
}

func TestSnapshotRefusalHistoryAndMetadataRemainBounded(t *testing.T) {
	t.Parallel()
	c := budgetCoordinator(t, snapshot.Options{MaxSnapshotBytes: 8})
	c.ports = refusalPorts()
	for i := 0; i < 600; i++ {
		owner := budgetOwnerRef(fmt.Sprintf("group-%03d", i))
		_, err := c.PrepareSend(owner.handle, refusalOffer(t.Context(), owner.key, 9))
		wantTransferReason(t, err, snapshot.ReasonSizeLimit)
	}
	if len(c.refusals) != snapshotHistoryLimit || len(c.history) != snapshotHistoryLimit || len(c.historyIDs) != snapshotHistoryLimit {
		t.Fatal("unbounded refusal bookkeeping")
	}
	if c.outgoingCount != 0 || c.sendBytes != 0 || len(c.outgoing) != 0 || len(c.hashWork) != 0 {
		t.Fatal("refusals retained work")
	}
}
