//go:build functional

package managed

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadsnapshot"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

func managedSnapshotOwnerFor(t *testing.T, h *Host) *managedSnapshotOwner {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	owner, err := h.snapshotOwnerLocked(h.slots["work"])
	must(t, err)
	return owner
}
func managedSnapshotOffer(t *testing.T, f *fixture, owner *managedSnapshotOwner) raftengine.SnapshotOffer {
	t.Helper()
	must(t, owner.core.Snapshot(deadline(t)))
	retained, err := f.store.Load(deadline(t), "work")
	must(t, err)
	message := raftpb.Message{Type: raftpb.MsgSnap, From: owner.key.Peer.Replica.ReplicaID, To: 2,
		Term: owner.core.Status().Term, Snapshot: &retained.Snapshot}
	return raftengine.SnapshotOffer{Message: message, Attempt: raftengine.SnapshotAttempt{RunID: owner.key.RunID,
		Term: message.Term, ReplicaID: 2, Generation: 1}, PublicationGeneration: 1, Epoch: owner.Context()}
}
func managedSnapshotRequest(t *testing.T, f *fixture, owner *managedSnapshotOwner) (snapshot.BeginRequest, []byte) {
	t.Helper()
	offer := managedSnapshotOffer(t, f, owner)
	m := offer.Message
	m.From, m.To = 2, 1
	m.Term++
	native := *m.Snapshot
	data := native.Data
	native.Data = nil
	native.Metadata.Index += 10
	m.Snapshot = &native
	header, err := m.Marshal()
	must(t, err)
	from, err := f.cfg.ResolvePeer(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2})
	must(t, err)
	return snapshot.BeginRequest{Manifest: snapshot.Manifest{Key: snapshot.Key{From: from, To: owner.key.Peer, DeliveryID: 17},
		NativeHeader: header, TotalBytes: uint64(len(data)), SHA256: sha256.Sum256(data), ChunkBytes: 64},
		RemainingTotal: time.Second, ProgressTimeout: time.Second}, data
}
func fillManagedSnapshot(t *testing.T, c *loadsnapshot.Coordinator, request snapshot.BeginRequest, data []byte) snapshot.Ack {
	t.Helper()
	ack, err := c.BeginSnapshot(deadline(t), request)
	must(t, err)
	for ack.NextOffset < uint64(len(data)) {
		offset := ack.NextOffset
		n := min(uint64(ack.ChunkBytes), uint64(len(data))-offset)
		chunk := snapshot.Chunk{Session: ack.Session, Offset: offset, Ordinal: offset / uint64(ack.ChunkBytes),
			Data: data[offset : offset+n], SHA256: sha256.Sum256(data[offset : offset+n])}
		ack, err = c.WriteSnapshot(deadline(t), chunk)
		must(t, err)
	}
	return ack
}

type managedSnapshotTransport struct {
	discardTransport
	send func(context.Context, snapshot.Send) (snapshot.Ack, error)
}

func (t *managedSnapshotTransport) SendSnapshot(ctx context.Context, send snapshot.Send) (snapshot.Ack, error) {
	return t.send(ctx, send)
}
func TestManagedSnapshot_SourceStopJoinsBorrowedTransportBeforeClose(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var closed atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{
			StateMachine: &stateMachine{}, Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}
	f.cfg.Transport = &managedSnapshotTransport{send: func(ctx context.Context, _ snapshot.Send) (snapshot.Ack, error) {
		close(entered)
		<-release
		return snapshot.Ack{}, ctx.Err()
	}}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	_, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	owner := managedSnapshotOwnerFor(t, h)
	transport := engineTransport{h, owner.slot}
	if _, selected := any(transport).(raftengine.SnapshotJobTransport); selected {
		t.Fatal("Task 15 activated early")
	}
	job, err := transport.prepareSnapshot(managedSnapshotOffer(t, f, owner))
	must(t, err)
	done := make(chan struct{})
	go func() { defer close(done); job.Run(t.Context()) }()
	waitSignal(t, entered)
	err = cancelAdmittedSnapshotStop(t, h, ref)
	if !errors.Is(err, fault.ErrShutdownTimeout) || closed.Load() != 0 || h.snapshots.List(snapshot.Query{}).Outgoing != 1 {
		t.Fatal("source pin/application released before transport return", err, closed.Load())
	}
	once.Do(func() { close(release) })
	waitSignal(t, done)
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	if closed.Load() != 1 || h.snapshots.List(snapshot.Query{}).Outgoing != 0 {
		t.Fatal("source ownership not joined")
	}
}

type managedSnapshotRestore struct {
	enabled atomic.Bool
	stateMachine
	entered, release chan struct{}
}

func (m *managedSnapshotRestore) Restore(ctx context.Context, _ []byte) error {
	if !m.enabled.Load() {
		return nil
	}
	close(m.entered)
	<-m.release
	return ctx.Err()
}
func TestManagedSnapshot_ReceivedRestoreAndFailedCloseRemainOwned(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	machine := &managedSnapshotRestore{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	defer once.Do(func() { close(machine.release) })
	closeFailure := errors.New("close incomplete")
	var closes atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{
			StateMachine: machine, Close: func(context.Context) error {
				if closes.Add(1) == 1 {
					return closeFailure
				}
				return nil
			}}, nil
	}
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	_, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	owner := managedSnapshotOwnerFor(t, h)
	request, data := managedSnapshotRequest(t, f, owner)
	machine.enabled.Store(true)
	ack := fillManagedSnapshot(t, h.snapshots, request, data)
	ack, err = h.snapshots.FinishSnapshot(deadline(t), ack.Session)
	must(t, err)
	if ack.State != snapshot.DeliveryAccepted {
		t.Fatal(ack)
	}
	waitSignal(t, machine.entered)
	err = cancelAdmittedSnapshotStop(t, h, ref)
	if !errors.Is(err, fault.ErrShutdownTimeout) || closes.Load() != 0 || h.snapshots.List(snapshot.Query{}).Incoming != 1 {
		t.Fatal("native Restore ownership released early", err, closes.Load())
	}
	once.Do(func() { close(machine.release) })
	if err = h.StopReplica(deadline(t), ref, StopExplicit); !errors.Is(err, closeFailure) {
		t.Fatal(err)
	}
	if _, err = h.StartReplica(deadline(t), ref, StartExplicit); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("failed Close permitted successor", err)
	}
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	if closes.Load() != 2 || h.snapshots.List(snapshot.Query{}).Incoming != 0 {
		t.Fatal("receive/application ownership not joined")
	}
}
func TestManagedSnapshot_UnpublishedOwnerAllowsNativeSkip(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, released sync.Once
	defer released.Do(func() { close(release) })
	scope := f.cfg.Scope
	var h *Host
	f.cfg.Scope = func(ctx context.Context, id storage.Identity) (result metaengine.LoadScope, err error) {
		result, err = scope(ctx, id)
		h.mu.Lock()
		slot := h.slots[id.GroupID]
		running := slot != nil && slot.core != nil && slot.core.Running()
		h.mu.Unlock()
		if running {
			enteredOnce.Do(func() { close(entered); <-release })
		}
		return result, err
	}
	h = f.host(t)
	started := make(chan error, 1)
	go func() {
		_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
		started <- err
	}()
	waitSignal(t, entered)
	if _, published := h.Lookup("work"); published {
		t.Fatal("startup published early")
	}
	owner := managedSnapshotOwnerFor(t, h)
	from, err := f.cfg.ResolvePeer(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2})
	must(t, err)
	bounds := owner.SnapshotBounds()
	m := raftpb.Message{Type: raftpb.MsgSnap, From: 2, To: 1, Term: bounds.Term + 1, Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index: bounds.Commit, Term: bounds.Term, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}}
	header, err := m.Marshal()
	must(t, err)
	ack, err := h.snapshots.BeginSnapshot(deadline(t), snapshot.BeginRequest{Manifest: snapshot.Manifest{
		Key: snapshot.Key{From: from, To: owner.key.Peer, DeliveryID: 1}, NativeHeader: header, TotalBytes: 4, ChunkBytes: 4},
		RemainingTotal: time.Second, ProgressTimeout: time.Second})
	must(t, err)
	if ack.State != snapshot.DeliveryNotNeeded || h.snapshots.List(snapshot.Query{}).Incoming != 0 {
		t.Fatal(ack)
	}
	released.Do(func() { close(release) })
	must(t, <-started)
}
func TestManagedSnapshot_StoppedCoordinatorCannotResumeIntoNewRun(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	_, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	owner := managedSnapshotOwnerFor(t, h)
	request, _ := managedSnapshotRequest(t, f, owner)
	_, err = h.snapshots.BeginSnapshot(deadline(t), request)
	must(t, err)
	must(t, h.Stop(deadline(t)))
	if h.snapshots.List(snapshot.Query{}).Incoming != 0 {
		t.Fatal("old coordinator retained reservation")
	}
	next := f.host(t)
	_, err = next.StartReplica(deadline(t), ref, StartRecovery)
	must(t, err)
	request.Resume = true
	_, err = h.snapshots.BeginSnapshot(deadline(t), request)
	if !errors.Is(err, fault.ErrStopped) {
		t.Fatal("old coordinator admitted work", err)
	}
	_, err = next.snapshots.BeginSnapshot(deadline(t), request)
	var detail *snapshot.TransferError
	if !errors.As(err, &detail) || detail.Reason != snapshot.ReasonRestartRequired {
		t.Fatal("new run resumed volatile old bytes", err)
	}
	if next.snapshots.List(snapshot.Query{}).Incoming != 0 {
		t.Fatal("unknown resume allocated staging")
	}
}

func TestManagedSnapshot_DeliveryUsesOriginalDeadlineAndPrivateTerminalCAS(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	h := f.host(t)
	_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
	must(t, err)
	owner := managedSnapshotOwnerFor(t, h)
	to, err := f.cfg.ResolvePeer(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2})
	must(t, err)
	attempt := raftengine.SnapshotAttempt{RunID: owner.key.RunID, Term: 1, ReplicaID: 2, Generation: 1}
	original := time.Now().Add(5 * time.Minute)
	id, err := h.registerSnapshotDelivery(owner, attempt, to, original)
	must(t, err)
	h.expireDeliveries(time.Now().Add(2 * time.Second))
	h.mu.Lock()
	record, found := owner.slot.deliveries[id]
	h.mu.Unlock()
	if !found || !record.deadline.Equal(original) {
		t.Fatal("snapshot inherited ordinary EffectTimeout", record)
	}
	key := snapshot.Key{From: owner.key.Peer, To: to, DeliveryID: id}
	err = h.ReportDelivery(deadline(t), load.DeliveryReport{From: key.From, To: key.To, DeliveryID: id, Kind: load.DeliverySnapshotSucceeded})
	if !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("ordinary report consumed job token", err)
	}
	// A stale destination report still ends only its exact source attempt. It
	// must not perform a fresh route lookup and strand native snapshot state.
	h.reportSnapshotDelivery(owner, key, snapshot.Ack{}, &snapshot.TransferError{Reason: snapshot.ReasonIdentity})
	h.mu.Lock()
	_, found = owner.slot.deliveries[id]
	h.mu.Unlock()
	if found {
		t.Fatal("failed exact report retained token")
	}
	id, err = h.registerSnapshotDelivery(owner, attempt, to, original)
	must(t, err)
	key.DeliveryID = id
	h.expireDeliveries(original.Add(time.Second))
	h.reportSnapshotDelivery(owner, key, snapshot.Ack{Session: snapshot.Session{Key: key}, State: snapshot.DeliveryAccepted}, nil)
	h.mu.Lock()
	remaining := len(owner.slot.deliveries)
	h.mu.Unlock()
	if remaining != 0 {
		t.Fatal("late acknowledgement revived expired delivery")
	}
}

func cancelAdmittedSnapshotStop(t *testing.T, h *Host, ref load.ReplicaRef) error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- h.StopReplica(ctx, ref, StopExplicit) }()
	await(deadline(t), t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.slots[ref.GroupID].state == "stopping" })
	cancel()
	select {
	case err := <-stopped:
		return err
	case <-deadline(t).Done():
		t.Fatal("stop waiter did not cancel")
		return nil
	}
}
