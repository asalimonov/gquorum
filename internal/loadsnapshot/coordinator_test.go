//go:build !functional

package loadsnapshot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type budgetOwner struct {
	key OwnerKey
	ctx context.Context
}

func (o *budgetOwner) Key() OwnerKey                           { return o.key }
func (o *budgetOwner) Context() context.Context                { return o.ctx }
func (*budgetOwner) SnapshotBounds() raftengine.SnapshotBounds { panic("unexpected native call") }
func (*budgetOwner) PreflightSnapshot(context.Context, raftpb.Message) (raftengine.SnapshotPreflight, error) {
	panic("unexpected native call")
}
func (*budgetOwner) PrepareOwnedSnapshot(context.Context, raftpb.Message) (raftstate.Snapshot, error) {
	panic("unexpected native call")
}
func (*budgetOwner) StepOwnedSnapshot(context.Context, raftengine.SnapshotIngress) error {
	panic("unexpected native call")
}
func budgetOwnerRef(group string) snapshotOwner {
	o := &budgetOwner{key: OwnerKey{Peer: load.Peer{Node: topology.Identity{NodeName: "node", ClusterID: "cluster", InstanceID: "process", MemberID: 1}, Replica: load.ReplicaRef{GroupID: group, ReplicaID: 1}}, RunID: "run-" + group}, ctx: context.Background()}
	return snapshotOwner{key: o.key, handle: o}
}
func budgetCoordinator(t *testing.T, o snapshot.Options) *Coordinator {
	t.Helper()
	c := NewCoordinator(o, Ports{})
	if c.configErr != nil {
		t.Fatal(c.configErr)
	}
	c.ctx, c.cancel = context.WithCancel(t.Context())
	t.Cleanup(c.cancel)
	c.started = true
	return c
}
func wantTransferReason(t *testing.T, err error, reason string) {
	t.Helper()
	var e *snapshot.TransferError
	if !errors.As(err, &e) || e.Reason != reason {
		t.Fatalf("want %s, got %v", reason, err)
	}
}
func TestCoordinatorDirectionalBudgetsAndExactGroupSlot(t *testing.T) {
	t.Parallel()
	c := budgetCoordinator(t, snapshot.Options{MaxSnapshotBytes: 64, MaxIncoming: 3, MaxOutgoing: 2, MaxReceiveBytes: 128})
	a, err := c.reserve(incoming, budgetOwnerRef("a"), 64)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.reserve(incoming, budgetOwnerRef("b"), 64)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.reserve(incoming, budgetOwnerRef("c"), 1)
	wantTransferReason(t, err, snapshot.ReasonCapacity)
	_, err = c.reserve(incoming, budgetOwnerRef("a"), 1)
	wantTransferReason(t, err, snapshot.ReasonGroupBusy)
	out, err := c.reserve(outgoing, budgetOwnerRef("a"), 64)
	if err != nil {
		t.Fatal("incoming blocked outgoing", err)
	}
	_, err = c.reserve(outgoing, budgetOwnerRef("b"), 65)
	wantTransferReason(t, err, snapshot.ReasonSizeLimit)
	if c.receiveBytes != 128 || c.sendBytes != 64 || c.incomingCount != 2 || c.outgoingCount != 1 {
		t.Fatal("wrong accounting")
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { c.release(a) })
	}
	wg.Wait()
	next := budgetOwnerRef("a")
	next.key.RunID = "successor"
	next.handle.(*budgetOwner).key = next.key
	r, err := c.reserve(incoming, next, 64)
	if err != nil {
		t.Fatal(err)
	}
	c.release(a)
	if c.groupReservations["a"] != r {
		t.Fatal("old release erased successor slot")
	}
	c.release(b)
	c.release(out)
	c.release(r)
	if len(c.reservations) != 0 || c.receiveBytes != 0 || c.sendBytes != 0 {
		t.Fatal("reservations leaked")
	}
}
func TestCoordinatorFenceRetainsUntilJoined(t *testing.T) {
	t.Parallel()
	c := budgetCoordinator(t, snapshot.Options{MaxSnapshotBytes: 64})
	r, err := c.reserve(incoming, budgetOwnerRef("a"), 64)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	if c.receiveBytes != 64 || c.incomingCount != 1 || c.ctx.Err() == nil {
		t.Fatal("stop freed an owned buffer")
	}
	_, err = c.reserve(outgoing, budgetOwnerRef("b"), 1)
	wantTransferReason(t, err, snapshot.ReasonStopped)
	c.Start(t.Context())
	_, err = c.reserve(incoming, budgetOwnerRef("b"), 1)
	wantTransferReason(t, err, snapshot.ReasonStopped)
	c.release(r)
	if err := c.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestCoordinatorPureConstructionAndArithmetic(t *testing.T) {
	t.Parallel()
	c := NewCoordinator(snapshot.Options{}, Ports{Authorize: func(context.Context, snapshot.Key) (Owner, error) { panic("callback during construction") }})
	if c.started || c.options.MaxSnapshotBytes != 256<<20 {
		t.Fatal("constructor acquired ownership")
	}
	_, err := c.reserve(incoming, budgetOwnerRef("a"), 1)
	if !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	large := uint64(64) << 30
	if uint64(math.MaxInt) < large {
		t.Skip("64-bit allocation-policy test")
	}
	c = budgetCoordinator(t, snapshot.Options{MaxSnapshotBytes: large})
	r, err := c.reserve(incoming, budgetOwnerRef("a"), large)
	if err != nil {
		t.Fatal(err)
	}
	if c.receiveBytes != large {
		t.Fatal("64-bit accounting truncated")
	}
	c.release(r)
	_, err = c.reserve(incoming, budgetOwnerRef("a"), math.MaxUint64)
	wantTransferReason(t, err, snapshot.ReasonSizeLimit)
	c.sequence = math.MaxUint64
	if _, err := c.nextAttachmentLocked(); err == nil {
		t.Fatal("generation wrapped")
	}
	broken := NewCoordinator(snapshot.Options{MaxIncoming: -1}, Ports{})
	broken.Start(t.Context())
	if _, err := broken.reserve(incoming, budgetOwnerRef("a"), 1); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}
func TestCoordinatorCachePressureNeverRefusesAdmission(t *testing.T) {
	t.Parallel()
	c := budgetCoordinator(t, snapshot.Options{MaxSnapshotBytes: 64})
	header := []byte("header")
	for i := 1; i <= snapshotTerminalMinimum+1; i++ {
		key := snapshot.Key{DeliveryID: uint64(i)}
		c.putTerminalLocked(key, snapshotTerminal{manifest: snapshot.Manifest{NativeHeader: header}, ack: snapshot.Ack{State: snapshot.DeliveryAccepted}})
	}
	header[0] = 'X'
	if len(c.terminal) != snapshotTerminalMinimum || c.terminal[snapshot.Key{DeliveryID: 1}] != nil {
		t.Fatal("oldest result not evicted")
	}
	if string(c.terminal[snapshot.Key{DeliveryID: 2}].manifest.NativeHeader) != "header" {
		t.Fatal("terminal header aliased")
	}
	for _, state := range []snapshot.DeliveryState{snapshot.DeliveryFailed, snapshot.DeliveryHandingOff, snapshot.DeliveryReceiving} {
		c.putTerminalLocked(snapshot.Key{}, snapshotTerminal{ack: snapshot.Ack{State: state}})
	}
	if c.terminal[snapshot.Key{}] != nil {
		t.Fatal("unresolved or failed result cached")
	}
	r, err := c.reserve(incoming, budgetOwnerRef("a"), 64)
	if err != nil {
		t.Fatal("terminal cache blocked admission", err)
	}
	c.release(r)
	for i := range snapshotHistoryLimit + 1 {
		c.appendHistoryLocked(snapshot.Status{ErrorReason: fmt.Sprint(i)})
	}
	if len(c.history) != snapshotHistoryLimit || c.history[0].ErrorReason != "1" {
		t.Fatal("history not bounded")
	}
	c.historySequence = math.MaxUint64
	c.appendHistoryLocked(snapshot.Status{})
	if c.historySequence != math.MaxUint64 {
		t.Fatal("history wrapped")
	}
	for i := uint64(1); i <= 3; i++ {
		c.putDigestLocked(SourceSnapshotID{RunID: "a", Generation: i}, [32]byte{byte(i)})
	}
	if len(c.digests) != 2 {
		t.Fatal("digest cache not bounded")
	}
	c.clearDigestsLocked("a")
	if len(c.digests) != 0 || len(c.digestOrder) != 0 {
		t.Fatal("run digests retained")
	}
}

func TestCoordinatorConcurrentAdmissionNeverOversubscribes(t *testing.T) {
	t.Parallel()
	c := budgetCoordinator(t, snapshot.Options{MaxSnapshotBytes: 64, MaxOutgoing: 2})
	admitted := make(chan *snapshotReservation, 64)
	failures := make(chan error, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			r, err := c.reserve(outgoing, budgetOwnerRef(fmt.Sprint("group", i)), 64)
			if err != nil {
				failures <- err
			} else {
				admitted <- r
			}
		})
	}
	wg.Wait()
	close(admitted)
	close(failures)
	if len(admitted) != 2 || c.outgoingCount != 2 || c.sendBytes != 128 {
		t.Fatal("oversubscribed or underused", len(admitted), c.outgoingCount, c.sendBytes)
	}
	for err := range failures {
		wantTransferReason(t, err, snapshot.ReasonCapacity)
	}
	for r := range admitted {
		c.release(r)
	}
	owner := budgetOwnerRef("group")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	owner.handle.(*budgetOwner).ctx = ctx
	_, err := c.reserve(outgoing, owner, 1)
	wantTransferReason(t, err, snapshot.ReasonStopped)
	owner = budgetOwnerRef("group")
	owner.key.RunID = "foreign"
	_, err = c.reserve(outgoing, owner, 1)
	wantTransferReason(t, err, snapshot.ReasonIdentity)
}
