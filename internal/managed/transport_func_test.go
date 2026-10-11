//go:build functional

package managed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"go.etcd.io/raft/v3/raftpb"
)

type captureTransport struct {
	discardTransport
	mu    sync.Mutex
	batch load.RaftBatch
}

func (c *captureTransport) SendRaft(ctx context.Context, batch load.RaftBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batch = batch
	return ctx.Err()
}
func (c *captureTransport) last() load.RaftBatch { c.mu.Lock(); defer c.mu.Unlock(); return c.batch }

func TestDeliveryProofRejectsForeignTuplesAndExpiresSnapshots(t *testing.T) {
	functional(t)
	f := newFixture(t)
	transport := &captureTransport{}
	f.cfg.Transport = transport
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	_, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	h.mu.Lock()
	slot := h.slots[ref.GroupID]
	h.mu.Unlock()
	sender := engineTransport{h, slot}
	// A nil Send result cannot certify a snapshot. The exact issued token and
	// both complete peers are required for the later delivery report.
	must(t, sender.Send(deadline(t), []raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgSnap}}))
	batch := transport.last()
	report := load.DeliveryReport{From: batch.From, To: batch.To, DeliveryID: batch.DeliveryID, Kind: load.DeliverySnapshotSucceeded}
	foreign := report
	foreign.To.Node.InstanceID = "different-process"
	if err := h.ReportDelivery(deadline(t), foreign); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	h.mu.Lock()
	pending := len(slot.deliveries)
	h.mu.Unlock()
	if pending != 1 {
		t.Fatal("nil send or foreign report consumed snapshot proof")
	}
	must(t, h.ReportDelivery(deadline(t), report))
	must(t, h.ReportDelivery(deadline(t), report)) // repeat is an obsolete acknowledgement
	h.mu.Lock()
	pending = len(slot.deliveries)
	h.mu.Unlock()
	if pending != 0 {
		t.Fatal("accepted report retained snapshot token")
	}
	must(t, sender.Send(deadline(t), []raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgSnap}}))
	h.expireDeliveries(time.Now().Add(2 * time.Second))
	h.mu.Lock()
	pending = len(slot.deliveries)
	h.mu.Unlock()
	if pending != 0 {
		t.Fatal("unacknowledged snapshot did not expire")
	}
	// A report from a former handle cannot consume a replacement's token.
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	_, err = h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	h.mu.Lock()
	next := h.slots[ref.GroupID]
	h.mu.Unlock()
	must(t, (engineTransport{h, next}).Send(deadline(t), []raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgSnap}}))
	must(t, h.ReportDelivery(deadline(t), report))
	h.mu.Lock()
	pending = len(next.deliveries)
	h.mu.Unlock()
	if pending != 1 || transport.last().DeliveryID == report.DeliveryID {
		t.Fatal("old lifetime affected replacement delivery")
	}
}

func TestReceiveChecksAllMessageIdentitiesBeforeStepping(t *testing.T) {
	functional(t)
	f := newFixture(t)
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	g, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	from, err := f.cfg.ResolvePeer(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2})
	must(t, err)
	to := load.Peer{Node: f.node, Replica: ref}
	before := g.Core().Status().Term
	batch := load.RaftBatch{From: from, To: to, DeliveryID: 9, Messages: []raftpb.Message{
		{From: 2, To: 1, Type: raftpb.MsgHeartbeat, Term: before + 100},
		{From: 99, To: 1, Type: raftpb.MsgHeartbeat, Term: before + 100},
	}}
	if err := h.ReceiveRaft(deadline(t), batch); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if got := g.Core().Status().Term; got >= before+100 {
		t.Fatal("partially applied a batch with a foreign sender", got)
	}
	query := load.ReplicaQuery{From: from.Node, To: to}
	status, err := h.ReadReplica(deadline(t), query)
	must(t, err)
	if !status.Found || status.Peer != to {
		t.Fatal(status)
	}
	query.To.Node.InstanceID = "old-process"
	if _, err := h.ReadReplica(deadline(t), query); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	query.To = to
	status, err = h.ReadReplica(deadline(t), query)
	must(t, err)
	if status.Found || status.Peer != to {
		t.Fatal("absence did not retain the queried exact identity", status)
	}
	_, err = f.store.Quarantine(deadline(t), "work", 1)
	must(t, err)
	f.setScope("work", 2, false, false)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2}, StartExplicit)
	must(t, err)
	status, err = h.ReadReplica(deadline(t), query)
	must(t, err)
	if status.Found || status.Peer != to {
		t.Fatal("old query aliased the successor", status)
	}
	query.To.Replica.ReplicaID = 2
	status, err = h.ReadReplica(deadline(t), query)
	must(t, err)
	if !status.Found || status.Peer != query.To {
		t.Fatal("successor not independently observable", status)
	}
}

func TestDeliveryBookkeepingIsBoundedWithoutDiscardingSnapshots(t *testing.T) {
	functional(t)
	f := newFixture(t)
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	_, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	h.mu.Lock()
	slot := h.slots[ref.GroupID]
	h.mu.Unlock()
	sender := engineTransport{h, slot}
	must(t, sender.Send(deadline(t), []raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgSnap}}))
	for i := 0; i < deliveryWindow*2; i++ {
		must(t, sender.Send(deadline(t), []raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgHeartbeat}}))
	}
	h.mu.Lock()
	pending, snapshot := len(slot.deliveries), slot.deliveries[1].snapshot
	h.mu.Unlock()
	if pending > deliveryWindow || !snapshot {
		t.Fatal("ordinary acknowledgements displaced an unresolved snapshot", pending, snapshot)
	}
}
