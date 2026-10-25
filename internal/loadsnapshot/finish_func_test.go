//go:build functional

package loadsnapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/memstore"
	"go.etcd.io/raft/v3/raftpb"
)

func finishFixture(t *testing.T) *receiveFixture {
	t.Helper()
	f := newReceiveFixture(t)
	data, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: "cluster", GroupID: "group", Application: []byte("restored application")})
	if err != nil {
		t.Fatal(err)
	}
	f.data = data
	f.begin.Manifest.TotalBytes = uint64(len(data))
	f.begin.Manifest.SHA256 = sha256.Sum256(data)
	f.owner.prepare = func(_ context.Context, m raftpb.Message) (raftstate.Snapshot, error) {
		return raftstate.DecodeRaftSnapshot(*m.Snapshot, "cluster", "group")
	}
	return f
}
func fillReceive(t *testing.T, f *receiveFixture) snapshot.Ack {
	t.Helper()
	ack := beginReceive(t, f)
	for ack.NextOffset < uint64(len(f.data)) {
		next, err := f.c.WriteSnapshot(t.Context(), receiveChunk(f, ack, ack.NextOffset, min(uint64(ack.ChunkBytes), uint64(len(f.data))-ack.NextOffset)))
		if err != nil {
			t.Fatal(err)
		}
		ack = next
	}
	return ack
}
func waitFinishRow(t *testing.T, c *Coordinator, outcome snapshot.ReceiverOutcome) snapshot.Status {
	t.Helper()
	ctx := receiveContext(t)
	for {
		page := c.List(snapshot.Query{})
		for _, row := range page.Items {
			if row.ReceiverOutcome == outcome && row.ReservedBytes == 0 {
				return row
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("completion observation missing", page)
			return snapshot.Status{}
		case <-time.After(time.Millisecond):
		}
	}
}
func TestSnapshotFinish_AcceptanceRetainsUntilNativeDoneAndReplaysAck(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	entered := make(chan raftengine.SnapshotIngress, 1)
	var steps atomic.Int32
	f.owner.step = func(_ context.Context, in raftengine.SnapshotIngress) error { steps.Add(1); entered <- in; return nil }
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	accepted, err := f.c.FinishSnapshot(t.Context(), ack.Session)
	if err != nil || accepted.State != snapshot.DeliveryAccepted {
		t.Fatal(accepted, err)
	}
	in := <-entered
	if &in.Message.Snapshot.Data[0] != &tr.staging.(*memorySnapshotStaging).data[0] || len(tr.header.Snapshot.Data) != 0 {
		t.Fatal("payload copied or retained in header")
	}
	page := f.c.List(snapshot.Query{})
	if page.Incoming != 1 || len(page.Items) != 1 || page.Items[0].ReceiverOutcome != snapshot.ReceiverUnknown {
		t.Fatal(page)
	}
	duplicate, err := f.c.FinishSnapshot(t.Context(), ack.Session)
	if err != nil || duplicate.State != snapshot.DeliveryAccepted || steps.Load() != 1 {
		t.Fatal(duplicate, err, steps.Load())
	}
	in.Done(raftengine.SnapshotInstalled, nil)
	in.Done(raftengine.SnapshotInstallFailed, fault.ErrUnreadable) // Duplicate completion cannot overwrite installation.
	receiveBarrier(t, tr.cleaned)
	row := waitFinishRow(t, f.c, snapshot.ReceiverInstalled)
	if row.Delivery != snapshot.DeliveryAccepted {
		t.Fatal(row)
	}
	resumed := f.begin
	resumed.Resume = true
	replay, err := f.c.BeginSnapshot(t.Context(), resumed)
	if err != nil || replay.State != snapshot.DeliveryAccepted || steps.Load() != 1 {
		t.Fatal(replay, err)
	}
	if n, b := receiveCounts(f.c); n != 0 || b != 0 {
		t.Fatal(n, b)
	}
}
func TestSnapshotFinish_CanceledWaitDoesNotUndoQueuedIngress(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	entered := make(chan raftengine.SnapshotIngress, 1)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	f.owner.step = func(_ context.Context, in raftengine.SnapshotIngress) error {
		entered <- in
		<-release
		return context.Canceled
	}
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	returned := make(chan error, 1)
	go func() { _, err := f.c.FinishSnapshot(ctx, ack.Session); returned <- err }()
	var in raftengine.SnapshotIngress
	select {
	case in = <-entered:
	case <-receiveContext(t).Done():
		t.Fatal("handoff not reached")
	}
	cancel()
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	receiveBarrier(t, tr.completion.finalizerDone)
	if n, _ := receiveCounts(f.c); n != 1 {
		t.Fatal("queued native ownership released")
	}
	select {
	case <-tr.cleaned:
		t.Fatal("unresolved native completion discarded")
	default:
	}
	in.Done(raftengine.SnapshotInstalled, nil)
	receiveBarrier(t, tr.cleaned)
	resumed := f.begin
	resumed.Resume = true
	result, err := f.c.BeginSnapshot(t.Context(), resumed)
	if err != nil || result.State != snapshot.DeliveryAccepted {
		t.Fatal(result, err)
	}
}
func TestSnapshotFinish_FailedCallbackCannotRaceSuccessfulStepReply(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	called := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	f.owner.step = func(_ context.Context, in raftengine.SnapshotIngress) error {
		in.Done(raftengine.SnapshotInstallFailed, fault.ErrUnreadable)
		close(called)
		<-release
		return nil
	}
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	returned := make(chan snapshot.Ack, 1)
	errs := make(chan error, 1)
	go func() { result, err := f.c.FinishSnapshot(t.Context(), ack.Session); returned <- result; errs <- err }()
	receiveBarrier(t, called)
	select {
	case result := <-returned:
		t.Fatal("failure published before Step returned", result)
	default:
	}
	once.Do(func() { close(release) })
	select {
	case result := <-returned:
		if err := <-errs; err != nil || result.State != snapshot.DeliveryAccepted {
			t.Fatal(result, err)
		}
	case <-receiveContext(t).Done():
		t.Fatal("Step acceptance missing")
	}
	receiveBarrier(t, tr.cleaned)
	row := waitFinishRow(t, f.c, snapshot.ReceiverFailed)
	if row.Delivery != snapshot.DeliveryAccepted || row.ErrorReason == "" {
		t.Fatal(row)
	}
}
func TestSnapshotFinish_AbortWaitsForOffLoopDecodeAndNeverSteps(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	original := f.owner.prepare
	var steps atomic.Int32
	f.owner.prepare = func(ctx context.Context, m raftpb.Message) (raftstate.Snapshot, error) {
		close(entered)
		<-release
		return original(ctx, m)
	}
	f.owner.step = func(context.Context, raftengine.SnapshotIngress) error { steps.Add(1); return nil }
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	finished := make(chan error, 1)
	go func() { _, err := f.c.FinishSnapshot(t.Context(), ack.Session); finished <- err }()
	receiveBarrier(t, entered)
	// Diagnostic reads remain available while full decoding is blocked.
	if p := f.c.List(snapshot.Query{}); len(p.Items) != 1 || p.Items[0].Delivery != snapshot.DeliveryVerifying {
		t.Fatal(p)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := f.c.AbortSnapshot(ctx, ack.Session); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if n, _ := receiveCounts(f.c); n != 1 {
		t.Fatal("decode ownership released")
	}
	once.Do(func() { close(release) })
	if err := <-finished; err == nil {
		t.Fatal("aborted decode accepted")
	}
	receiveBarrier(t, tr.cleaned)
	if steps.Load() != 0 {
		t.Fatal("canceled decode entered native Step")
	}
}

type closeFailureStaging struct {
	snapshotStaging
	fail  atomic.Bool
	calls atomic.Int32
}

func (s *closeFailureStaging) Close() error {
	s.calls.Add(1)
	if s.fail.Load() {
		return errors.New("release failed")
	}
	return s.snapshotStaging.Close()
}
func TestSnapshotFinish_CloseFailureAndStopRetry(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	gate := &closeFailureStaging{snapshotStaging: tr.staging}
	gate.fail.Store(true)
	tr.staging = gate
	if err := f.c.Stop(receiveContext(t)); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	if n, _ := receiveCounts(f.c); n != 1 || gate.calls.Load() == 0 {
		t.Fatal("failed Close released ownership")
	}
	gate.fail.Store(false)
	if err := f.c.Stop(receiveContext(t)); err != nil {
		t.Fatal(err)
	}
	if n, _ := receiveCounts(f.c); n != 0 {
		t.Fatal("Stop retry did not release")
	}
	receiveBarrier(t, tr.cleaned)
}
func TestSnapshotFinish_StopRetainsAcceptedNativeOwner(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	entered := make(chan raftengine.SnapshotIngress, 1)
	f.owner.step = func(_ context.Context, in raftengine.SnapshotIngress) error { entered <- in; return nil }
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	if _, err := f.c.FinishSnapshot(t.Context(), ack.Session); err != nil {
		t.Fatal(err)
	}
	in := <-entered
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	f.owner.cancel()
	if err := f.c.StopOwner(ctx, f.owner.key); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	if n, _ := receiveCounts(f.c); n != 1 {
		t.Fatal("StopOwner freed pending native data")
	}
	in.Done(raftengine.SnapshotInstallFailed, context.Canceled)
	if err := f.c.StopOwner(receiveContext(t), f.owner.key); err != nil {
		t.Fatal(err)
	}
	receiveBarrier(t, tr.cleaned)
	row := waitFinishRow(t, f.c, snapshot.ReceiverFailed)
	if row.Delivery != snapshot.DeliveryAccepted {
		t.Fatal(row)
	}
}
func TestSnapshotFinish_WholeDigestFailureNeverCallsNative(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	f.begin.Manifest.SHA256[0] ^= 1
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	_, err := f.c.FinishSnapshot(t.Context(), ack.Session)
	requireReceiveReason(t, err, snapshot.ReasonChecksum)
	receiveBarrier(t, tr.cleaned)
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	if len(f.c.terminal) != 0 {
		t.Fatal("failed ingress cached as acceptance")
	}
}

type nativeFinishOwner struct {
	key   OwnerKey
	group *raftengine.Group
}

func (o *nativeFinishOwner) Key() OwnerKey            { return o.key }
func (o *nativeFinishOwner) Context() context.Context { return o.group.RunContext() }
func (o *nativeFinishOwner) SnapshotBounds() raftengine.SnapshotBounds {
	return o.group.SnapshotBounds()
}
func (o *nativeFinishOwner) PreflightSnapshot(ctx context.Context, m raftpb.Message) (raftengine.SnapshotPreflight, error) {
	return o.group.PreflightSnapshot(ctx, m)
}
func (o *nativeFinishOwner) PrepareOwnedSnapshot(ctx context.Context, m raftpb.Message) (raftstate.Snapshot, error) {
	return o.group.PrepareOwnedSnapshot(ctx, m)
}
func (o *nativeFinishOwner) StepOwnedSnapshot(ctx context.Context, in raftengine.SnapshotIngress) error {
	return o.group.StepOwnedSnapshot(ctx, in)
}

type finishTransport struct{}

func (finishTransport) Send(context.Context, []raftpb.Message) error { return nil }

type finishMachine struct {
	entered  chan struct{}
	release  chan struct{}
	fail     error
	restores atomic.Int32
	data     []byte
}

func (*finishMachine) Apply(context.Context, raftengine.Command) (raftengine.Result, error) {
	return raftengine.Result{}, nil
}
func (*finishMachine) Snapshot(context.Context) ([]byte, error) { return []byte("empty"), nil }
func (m *finishMachine) Restore(_ context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	m.restores.Add(1)
	if m.entered != nil {
		close(m.entered)
		<-m.release
	}
	if m.fail != nil {
		return m.fail
	}
	m.data = append([]byte(nil), data...)
	return nil
}

type finishStore struct {
	storage.Store
	fail bool
}

func (s *finishStore) SaveSnapshot(ctx context.Context, group string, ss raftpb.Snapshot) error {
	if s.fail {
		return fault.ErrUnreadable
	}
	return s.Store.SaveSnapshot(ctx, group, ss)
}
func TestSnapshotFinish_RealEnginePersistenceRestoreAndFailure(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"installed", "restore failure", "write failure", "native skip", "partial"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := finishFixture(t)
			base, err := memstore.New(memstore.Options{NodeName: "receiver", ClusterID: "cluster"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := base.Close(); err != nil {
					t.Error(err)
				}
			})
			store := &finishStore{Store: base, fail: kind == "write failure"}
			machine := &finishMachine{}
			if kind != "write failure" && kind != "native skip" && kind != "partial" {
				machine.entered = make(chan struct{})
				machine.release = make(chan struct{})
			}
			if kind == "restore failure" {
				machine.fail = errors.New("Restore failed")
			}
			group, err := raftengine.New(raftengine.Options{ClusterID: "cluster", GroupID: "group", NodeName: "receiver", ReplicaID: 2, Mode: raftengine.Joining,
				Store: store, Machine: machine, Transport: finishTransport{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal(err)
			}
			if err := group.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				group.RequestStop()
				if err := group.Stop(receiveContext(t)); err != nil {
					t.Error(err)
				}
				if err := f.c.Stop(receiveContext(t)); err != nil {
					t.Error(err)
				}
			})
			if err := group.WaitReady(receiveContext(t)); err != nil {
				t.Fatal(err)
			}
			owner := &nativeFinishOwner{key: OwnerKey{Peer: f.owner.key.Peer, RunID: group.RunID()}, group: group}
			f.c.ports.Authorize = func(ctx context.Context, key snapshot.Key) (Owner, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if key != f.begin.Manifest.Key {
					return nil, fault.ErrIdentityMismatch
				}
				return owner, nil
			}
			var once sync.Once
			if machine.release != nil {
				t.Cleanup(func() { once.Do(func() { close(machine.release) }) })
			}
			if kind == "partial" {
				ack := beginReceive(t, f)
				tr, _ := f.c.findTransfer(ack.Session.Key)
				if _, err := f.c.WriteSnapshot(t.Context(), receiveChunk(f, ack, 0, 4)); err != nil {
					t.Fatal(err)
				}
				_, err := f.c.FinishSnapshot(t.Context(), ack.Session)
				requireReceiveReason(t, err, snapshot.ReasonMalformed)
				receiveBarrier(t, tr.cleaned)
				stored, err := base.Load(t.Context(), "group")
				if err != nil || stored.Snapshot.Metadata.Index != 0 || machine.restores.Load() != 0 {
					t.Fatal("partial snapshot entered native state", stored, err)
				}
				return
			}
			ack := fillReceive(t, f)
			tr, _ := f.c.findTransfer(ack.Session.Key)
			if kind == "native skip" {
				// Admission happened at term 0. Advancing the actual owner term before the
				// full handoff makes native Raft skip this previously needed snapshot.
				if err := group.Step(t.Context(), raftpb.Message{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 4}); err != nil {
					t.Fatal(err)
				}
			}
			result, err := f.c.FinishSnapshot(receiveContext(t), ack.Session)
			if err != nil || result.State != snapshot.DeliveryAccepted {
				t.Fatal(result, err)
			}
			if machine.entered != nil {
				receiveBarrier(t, machine.entered)
				if n, _ := receiveCounts(f.c); n != 1 {
					t.Fatal("accepted Restore released reservation")
				}
				retained, err := base.Load(t.Context(), "group")
				if err != nil || retained.Snapshot.Metadata.Index != 9 {
					t.Fatal("Restore before persistence", retained, err)
				}
				once.Do(func() { close(machine.release) })
			}
			receiveBarrier(t, tr.cleaned)
			want := snapshot.ReceiverInstalled
			if kind == "write failure" || kind == "restore failure" {
				want = snapshot.ReceiverFailed
			}
			if kind == "native skip" {
				want = snapshot.ReceiverSkipped
			}
			row := waitFinishRow(t, f.c, want)
			if row.Delivery != snapshot.DeliveryAccepted {
				t.Fatal(row)
			}
			if kind == "installed" && string(machine.data) != "restored application" {
				t.Fatal(string(machine.data))
			}
			if (kind == "write failure" || kind == "native skip") && machine.restores.Load() != 0 {
				t.Fatal("failed/skipped snapshot restored")
			}
			if kind == "write failure" || kind == "native skip" {
				retained, err := base.Load(t.Context(), "group")
				if err != nil || retained.Snapshot.Metadata.Index != 0 {
					t.Fatal("partial/skipped snapshot persisted", retained, err)
				}
			}
		})
	}
}

func TestSnapshotFinish_ExpiryCleansDetachedTransfer(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	ack := beginReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	if err := f.c.DetachSnapshot(t.Context(), ack.Session); err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	tr.progress = time.Now().Add(-time.Minute)
	tr.mu.Unlock()
	f.c.expire(time.Now())
	receiveBarrier(t, tr.cleaned)
	row := waitFinishRow(t, f.c, snapshot.ReceiverFailed)
	if row.Delivery != snapshot.DeliveryFailed || row.ErrorReason != snapshot.ReasonProgressTimeout {
		t.Fatal(row)
	}
	if n, b := receiveCounts(f.c); n != 0 || b != 0 {
		t.Fatal(n, b)
	}
}

type deferredIngressError struct {
	entered, release chan struct{}
	once             sync.Once
}

func (*deferredIngressError) Error() string { return "deferred cause" }
func (e *deferredIngressError) Unwrap() error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return fault.ErrUnreadable
}
func TestSnapshotFinish_NativeCallbackOnlyRecordsAndSignals(t *testing.T) {
	t.Parallel()
	f := finishFixture(t)
	cause := &deferredIngressError{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(cause.release) }) })
	callbackReturned := make(chan struct{})
	f.owner.step = func(_ context.Context, in raftengine.SnapshotIngress) error {
		in.Done(raftengine.SnapshotInstallFailed, cause)
		close(callbackReturned)
		return nil
	}
	ack := fillReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	done := make(chan error, 1)
	go func() { _, err := f.c.FinishSnapshot(t.Context(), ack.Session); done <- err }()
	receiveBarrier(t, callbackReturned)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	receiveBarrier(t, cause.entered)
	if p := f.c.List(snapshot.Query{}); p.Incoming != 1 || len(p.Items) != 1 {
		t.Fatal("error inspection blocked status or freed ownership", p)
	}
	release.Do(func() { close(cause.release) })
	receiveBarrier(t, tr.cleaned)
}
