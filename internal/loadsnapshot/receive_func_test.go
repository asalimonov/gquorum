//go:build functional

package loadsnapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type receiveOwner struct {
	prepare      func(context.Context, raftpb.Message) (raftstate.Snapshot, error)
	step         func(context.Context, raftengine.SnapshotIngress) error
	key          OwnerKey
	ctx          context.Context
	cancel       context.CancelFunc
	term, commit atomic.Uint64
	preflights   atomic.Int32
}

func (o *receiveOwner) Key() OwnerKey            { return o.key }
func (o *receiveOwner) Context() context.Context { return o.ctx }
func (o *receiveOwner) SnapshotBounds() raftengine.SnapshotBounds {
	return raftengine.SnapshotBounds{RunID: o.key.RunID, Term: o.term.Load(), Commit: o.commit.Load()}
}
func (o *receiveOwner) PreflightSnapshot(ctx context.Context, m raftpb.Message) (raftengine.SnapshotPreflight, error) {
	o.preflights.Add(1)
	if err := ctx.Err(); err != nil {
		return raftengine.SnapshotPreflight{}, err
	}
	skip := m.Term < o.term.Load() || m.Snapshot.Metadata.Index <= o.commit.Load()
	return raftengine.SnapshotPreflight{Term: o.term.Load(), Commit: o.commit.Load(), NotNeeded: skip, HeaderStepped: skip}, nil
}
func (o *receiveOwner) PrepareOwnedSnapshot(ctx context.Context, m raftpb.Message) (raftstate.Snapshot, error) {
	if o.prepare == nil {
		panic("unexpected native preparation")
	}
	return o.prepare(ctx, m)
}
func (o *receiveOwner) StepOwnedSnapshot(ctx context.Context, in raftengine.SnapshotIngress) error {
	if o.step == nil {
		panic("unexpected native ingress")
	}
	return o.step(ctx, in)
}

type receiveFixture struct {
	c      *Coordinator
	mu     sync.Mutex
	owner  *receiveOwner
	source load.Peer
	auths  atomic.Int32
	begin  snapshot.BeginRequest
	data   []byte
}

func newReceiveFixture(t *testing.T) *receiveFixture {
	t.Helper()
	peer := func(name string, id uint64) load.Peer {
		return load.Peer{Node: topology.Identity{NodeName: name, ClusterID: "cluster", InstanceID: "process-" + name, MemberID: id}, Replica: load.ReplicaRef{GroupID: "group", ReplicaID: id}}
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	o := &receiveOwner{key: OwnerKey{Peer: peer("receiver", 2), RunID: "receiver-run"}, ctx: ownerCtx, cancel: cancel}
	o.term.Store(2)
	f := &receiveFixture{owner: o, source: peer("sender", 1), data: []byte("abcdefghij")}
	header := raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}}
	encoded, err := header.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	f.begin = snapshot.BeginRequest{Manifest: snapshot.Manifest{Key: snapshot.Key{From: f.source, To: o.key.Peer, DeliveryID: 1}, NativeHeader: encoded, TotalBytes: uint64(len(f.data)), SHA256: sha256.Sum256(f.data), ChunkBytes: 8}, RemainingTotal: time.Minute, ProgressTimeout: 45 * time.Second, MaxChunkBytes: 6}
	f.c = NewCoordinator(snapshot.Options{MaxSnapshotBytes: 4 << 10, ChunkBytes: 4, TotalTimeout: time.Minute, ProgressTimeout: 30 * time.Second}, Ports{Authorize: func(ctx context.Context, key snapshot.Key) (Owner, error) {
		f.auths.Add(1)
		f.mu.Lock()
		defer f.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if key.From != f.source || key.To != f.owner.key.Peer {
			return nil, fault.ErrIdentityMismatch
		}
		return f.owner, nil
	}})
	f.c.Start(t.Context())
	t.Cleanup(func() {
		f.mu.Lock()
		f.owner.cancel()
		f.mu.Unlock()
		f.c.mu.Lock()
		var owned []*snapshotReceive
		for _, tr := range f.c.incoming {
			owned = append(owned, tr)
		}
		f.c.mu.Unlock()
		for _, tr := range owned {
			f.c.failReceive(tr, context.Canceled)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.c.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return f
}
func beginReceive(t *testing.T, f *receiveFixture) snapshot.Ack {
	t.Helper()
	ack, err := f.c.BeginSnapshot(t.Context(), f.begin)
	if err != nil {
		t.Fatal(err)
	}
	return ack
}
func receiveChunk(f *receiveFixture, ack snapshot.Ack, off, n uint64) snapshot.Chunk {
	data := bytes.Clone(f.data[off : off+n])
	return snapshot.Chunk{Session: ack.Session, Offset: off, Ordinal: off / uint64(ack.ChunkBytes), Data: data, SHA256: sha256.Sum256(data)}
}
func requireReceiveReason(t *testing.T, err error, reason string) {
	t.Helper()
	var detail *snapshot.TransferError
	if !errors.As(err, &detail) || detail.Reason != reason {
		t.Fatalf("want %s, got %v", reason, err)
	}
}
func receiveContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func receiveBarrier(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-receiveContext(t).Done():
		t.Fatal("barrier timed out")
	}
}
func receiveCounts(c *Coordinator) (int, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.incomingCount, c.receiveBytes
}

func TestSnapshotReceive_ResumePreservesOffsetManifestAndDeadlines(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	ack := beginReceive(t, f)
	if ack.ChunkBytes != 4 || ack.ProgressTimeout != 30*time.Second || ack.ReceiverRunID != f.owner.key.RunID {
		t.Fatal(ack)
	}
	tr, _ := f.c.findTransfer(ack.Session.Key)
	deadline := tr.deadline
	chunk := receiveChunk(f, ack, 0, 4)
	next, err := f.c.WriteSnapshot(t.Context(), chunk)
	if err != nil || next.NextOffset != 4 {
		t.Fatal(next, err)
	}
	tr.mu.Lock()
	progress := tr.progress
	tr.mu.Unlock()
	next, err = f.c.WriteSnapshot(t.Context(), chunk)
	if err != nil || next.NextOffset != 4 {
		t.Fatal(next, err)
	}
	tr.mu.Lock()
	if tr.progress != progress {
		t.Fatal("duplicate refreshed progress")
	}
	tr.mu.Unlock()
	if err := f.c.DetachSnapshot(t.Context(), ack.Session); err != nil {
		t.Fatal(err)
	}
	receiveBarrier(t, f.c.SessionDone(ack.Session))
	resumed := f.begin
	resumed.Resume = true
	resumed.RemainingTotal = time.Hour
	resumed.ProgressTimeout = time.Hour
	capped := resumed
	capped.MaxChunkBytes = 2
	_, err = f.c.BeginSnapshot(t.Context(), capped)
	requireReceiveReason(t, err, snapshot.ReasonFrameLimit)
	changed := resumed
	changed.Manifest.ChunkBytes++
	_, err = f.c.BeginSnapshot(t.Context(), changed)
	requireReceiveReason(t, err, snapshot.ReasonTransferConflict)
	current, err := f.c.BeginSnapshot(t.Context(), resumed)
	if err != nil {
		t.Fatal(err)
	}
	if current.Session.Attachment == ack.Session.Attachment || current.NextOffset != 4 || current.ChunkBytes != 4 || tr.deadline != deadline || current.ProgressTimeout != 30*time.Second {
		t.Fatal("resume changed mapping or clocks", current)
	}
	_, err = f.c.WriteSnapshot(t.Context(), chunk)
	requireReceiveReason(t, err, snapshot.ReasonRestartRequired)
	err = f.c.AbortSnapshot(t.Context(), ack.Session)
	requireReceiveReason(t, err, snapshot.ReasonRestartRequired)
	for _, part := range [][2]uint64{{4, 4}, {8, 2}} {
		current, err = f.c.WriteSnapshot(t.Context(), receiveChunk(f, current, part[0], part[1]))
		if err != nil {
			t.Fatal(err)
		}
	}
	if current.NextOffset != uint64(len(f.data)) || f.owner.preflights.Load() != 1 {
		t.Fatal(current, "chunk called native preflight")
	}
	if err := f.c.AbortSnapshot(t.Context(), current.Session); err != nil {
		t.Fatal(err)
	}
	if n, b := receiveCounts(f.c); n != 0 || b != 0 {
		t.Fatal(n, b)
	}
}
func TestSnapshotReceive_RejectsBadChunksAndReleasesAfterJoin(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"checksum", "hole", "ordinal", "short", "oversized", "conflicting duplicate"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newReceiveFixture(t)
			ack := beginReceive(t, f)
			tr, _ := f.c.findTransfer(ack.Session.Key)
			chunk := receiveChunk(f, ack, 0, 4)
			reason := snapshot.ReasonMalformed
			switch kind {
			case "checksum":
				chunk.SHA256[0] ^= 1
				reason = snapshot.ReasonChecksum
			case "hole":
				chunk = receiveChunk(f, ack, 4, 4)
			case "ordinal":
				chunk.Ordinal = 9
			case "short":
				chunk = receiveChunk(f, ack, 0, 3)
			case "oversized":
				chunk = receiveChunk(f, ack, 0, 5)
			case "conflicting duplicate":
				if _, err := f.c.WriteSnapshot(t.Context(), chunk); err != nil {
					t.Fatal(err)
				}
				chunk.Data[0] = 'X'
				chunk.SHA256 = sha256.Sum256(chunk.Data)
				reason = snapshot.ReasonTransferConflict
			}
			_, err := f.c.WriteSnapshot(t.Context(), chunk)
			requireReceiveReason(t, err, reason)
			receiveBarrier(t, tr.cleaned)
			if n, b := receiveCounts(f.c); n != 0 || b != 0 {
				t.Fatal(n, b)
			}
			resumed := f.begin
			resumed.Resume = true
			_, err = f.c.BeginSnapshot(t.Context(), resumed)
			requireReceiveReason(t, err, snapshot.ReasonRestartRequired)
		})
	}
}
func TestSnapshotReceive_NotNeededAndTerminalMiss(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	f.owner.commit.Store(9)
	ack := beginReceive(t, f)
	if ack.State != snapshot.DeliveryNotNeeded || ack.Session.Attachment != 0 || ack.NextOffset != 0 {
		t.Fatal(ack)
	}
	if n, b := receiveCounts(f.c); n != 0 || b != 0 {
		t.Fatal("skip allocated", n, b)
	}
	resumed := f.begin
	resumed.Resume = true
	cached, err := f.c.BeginSnapshot(t.Context(), resumed)
	if err != nil || cached.State != snapshot.DeliveryNotNeeded || f.owner.preflights.Load() != 1 {
		t.Fatal(cached, err)
	}
	f.c.mu.Lock()
	f.c.terminal[ack.Session.Key].expires = time.Now().Add(-time.Second)
	f.c.mu.Unlock()
	_, err = f.c.BeginSnapshot(t.Context(), resumed)
	requireReceiveReason(t, err, snapshot.ReasonRestartRequired)
	if n, b := receiveCounts(f.c); n != 0 || b != 0 {
		t.Fatal(n, b)
	}
}
func TestSnapshotReceive_InvalidHeadersBeforeAuthorization(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"zero term", "zero index", "future snapshot term", "payload", "entries", "zero total", "deadline", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newReceiveFixture(t)
			b := f.begin
			var h raftpb.Message
			if err := h.Unmarshal(b.Manifest.NativeHeader); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "zero term":
				h.Term = 0
			case "zero index":
				h.Snapshot.Metadata.Index = 0
			case "future snapshot term":
				h.Snapshot.Metadata.Term = 4
			case "payload":
				h.Snapshot.Data = []byte("x")
			case "entries":
				h.Entries = []raftpb.Entry{{Index: 1}}
			case "zero total":
				b.Manifest.TotalBytes = 0
			case "deadline":
				b.RemainingTotal = 0
			case "oversized":
				b.Manifest.TotalBytes = f.c.options.MaxSnapshotBytes + 1
			}
			var err error
			b.Manifest.NativeHeader, err = h.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.c.BeginSnapshot(t.Context(), b); err == nil {
				t.Fatal("malformed Begin accepted")
			}
			if f.auths.Load() != 0 {
				t.Fatal("invalid header reached authorization")
			}
			if n, bytes := receiveCounts(f.c); n != 0 || bytes != 0 {
				t.Fatal(n, bytes)
			}
		})
	}
}
func TestSnapshotReceive_ObsoletePreemptionAndRunReplacement(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	ack := beginReceive(t, f)
	old, _ := f.c.findTransfer(ack.Session.Key)
	f.owner.term.Store(4)
	replacement := f.begin
	replacement.Manifest.Key.DeliveryID++
	var h raftpb.Message
	if err := h.Unmarshal(replacement.Manifest.NativeHeader); err != nil {
		t.Fatal(err)
	}
	h.Term = 5
	var err error
	replacement.Manifest.NativeHeader, err = h.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	next, err := f.c.BeginSnapshot(t.Context(), replacement)
	if err != nil {
		t.Fatal(err)
	}
	receiveBarrier(t, old.cleaned)
	if next.Session.Attachment == ack.Session.Attachment {
		t.Fatal("preemption reused generation")
	}
	// A receiver-only engine restart cannot resume retained partial bytes.
	f.mu.Lock()
	f.owner.cancel()
	ownerCtx, cancel := context.WithCancel(t.Context())
	f.owner = &receiveOwner{key: OwnerKey{Peer: f.owner.key.Peer, RunID: "replacement-run"}, ctx: ownerCtx, cancel: cancel}
	f.owner.term.Store(4)
	f.mu.Unlock()
	resumed := replacement
	resumed.Resume = true
	_, err = f.c.BeginSnapshot(t.Context(), resumed)
	requireReceiveReason(t, err, snapshot.ReasonRestartRequired)
	replacement.Manifest.Key.DeliveryID++
	next, err = f.c.BeginSnapshot(t.Context(), replacement)
	if err != nil || next.NextOffset != 0 || next.ReceiverRunID != "replacement-run" {
		t.Fatal(next, err)
	}
	// Process identity, not a reused node name, governs source authentication.
	f.mu.Lock()
	f.source.Node.InstanceID = "new-sender-process"
	f.mu.Unlock()
	resumed = replacement
	resumed.Resume = true
	if _, err = f.c.BeginSnapshot(t.Context(), resumed); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
}

type gatedSnapshotStaging struct {
	snapshotStaging
	entered, release  chan struct{}
	once              sync.Once
	active            atomic.Bool
	closedWhileActive atomic.Bool
}

func (s *gatedSnapshotStaging) Append(_ context.Context, off uint64, data []byte) error {
	s.active.Store(true)
	s.once.Do(func() { close(s.entered) })
	<-s.release
	err := s.snapshotStaging.Append(context.Background(), off, data)
	s.active.Store(false)
	return err
}
func (s *gatedSnapshotStaging) Close() error {
	if s.active.Load() {
		s.closedWhileActive.Store(true)
	}
	return s.snapshotStaging.Close()
}
func TestSnapshotReceive_ReconnectJoinsOldMutation(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	ack := beginReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	gate := &gatedSnapshotStaging{snapshotStaging: tr.staging, entered: make(chan struct{}), release: make(chan struct{})}
	tr.staging = gate
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate.release) }) })
	writeDone := make(chan error, 1)
	go func() { _, err := f.c.WriteSnapshot(t.Context(), receiveChunk(f, ack, 0, 4)); writeDone <- err }()
	receiveBarrier(t, gate.entered)
	resumed := f.begin
	resumed.Resume = true
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	resumeDone := make(chan error, 1)
	go func() { _, err := f.c.BeginSnapshot(ctx, resumed); resumeDone <- err }()
	receiveBarrier(t, f.c.SessionDone(ack.Session))
	cancel()
	if err := <-resumeDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n, b := receiveCounts(f.c); n != 1 || b != uint64(len(f.data)) {
		t.Fatal("old mutation freed reservation", n, b)
	}
	release.Do(func() { close(gate.release) })
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	next, err := f.c.BeginSnapshot(t.Context(), resumed)
	if err != nil || next.NextOffset != 4 {
		t.Fatal(next, err)
	}
	if next.Session.Attachment == ack.Session.Attachment || gate.closedWhileActive.Load() {
		t.Fatal("attachment overlapped old mutation")
	}
}
func TestSnapshotReceive_AbortTimeoutRetainsBlockedMutation(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	ack := beginReceive(t, f)
	tr, _ := f.c.findTransfer(ack.Session.Key)
	gate := &gatedSnapshotStaging{snapshotStaging: tr.staging, entered: make(chan struct{}), release: make(chan struct{})}
	tr.staging = gate
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate.release) }) })
	returned := make(chan error, 1)
	go func() { _, err := f.c.WriteSnapshot(t.Context(), receiveChunk(f, ack, 0, 4)); returned <- err }()
	receiveBarrier(t, gate.entered)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := f.c.AbortSnapshot(ctx, ack.Session); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if n, _ := receiveCounts(f.c); n != 1 || gate.closedWhileActive.Load() {
		t.Fatal("aborted mutation released early")
	}
	release.Do(func() { close(gate.release) })
	if err := <-returned; err == nil {
		t.Fatal("aborted write reported success")
	}
	receiveBarrier(t, tr.cleaned)
	if n, _ := receiveCounts(f.c); n != 0 || gate.closedWhileActive.Load() {
		t.Fatal("cleanup not joined")
	}
}

func TestSnapshotReceive_TimeoutsAndCachedObsolescence(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"progress", "total", "obsolete"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newReceiveFixture(t)
			ack := beginReceive(t, f)
			tr, _ := f.c.findTransfer(ack.Session.Key)
			reason := snapshot.ReasonProgressTimeout
			tr.mu.Lock()
			switch kind {
			case "progress":
				tr.progress = time.Now().Add(-time.Minute)
			case "total":
				tr.deadline = time.Now().Add(-time.Second)
				reason = snapshot.ReasonTotalTimeout
			case "obsolete":
				f.owner.term.Store(4)
				reason = snapshot.ReasonObsolete
			}
			tr.mu.Unlock()
			_, err := f.c.WriteSnapshot(t.Context(), receiveChunk(f, ack, 0, 4))
			requireReceiveReason(t, err, reason)
			receiveBarrier(t, tr.cleaned)
			if f.owner.preflights.Load() != 1 {
				t.Fatal("chunk performed a native preflight")
			}
		})
	}
}
func TestSnapshotReceive_TransportCapDoesNotRewriteManifest(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	b := f.begin
	b.MaxChunkBytes = 2
	ack, err := f.c.BeginSnapshot(t.Context(), b)
	if err != nil || ack.ChunkBytes != 2 {
		t.Fatal(ack, err)
	}
	tr, _ := f.c.findTransfer(ack.Session.Key)
	if tr.manifest.ChunkBytes != 8 {
		t.Fatal("requested size rewritten")
	}
	b.Resume = true
	b.MaxChunkBytes = 8
	resumed, err := f.c.BeginSnapshot(t.Context(), b)
	if err != nil || resumed.ChunkBytes != 2 {
		t.Fatal("resume renegotiated", resumed, err)
	}
}
func TestSnapshotReceive_OwnerChangeAfterAllocationReleasesUnpublished(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	original := f.c.ports.Authorize
	var calls atomic.Int32
	f.c.ports.Authorize = func(ctx context.Context, key snapshot.Key) (Owner, error) {
		if calls.Add(1) == 3 {
			f.mu.Lock()
			f.owner.cancel()
			next, cancel := context.WithCancel(t.Context())
			f.owner = &receiveOwner{key: OwnerKey{Peer: f.owner.key.Peer, RunID: "replacement"}, ctx: next, cancel: cancel}
			f.mu.Unlock()
		}
		return original(ctx, key)
	}
	if _, err := f.c.BeginSnapshot(t.Context(), f.begin); err == nil {
		t.Fatal("published across owner replacement")
	}
	if n, b := receiveCounts(f.c); n != 0 || b != 0 {
		t.Fatal("unpublished reservation retained", n, b)
	}
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	if len(f.c.incoming) != 0 || len(f.c.groupReservations) != 0 {
		t.Fatal("unpublished slot retained")
	}
}

func TestSnapshotReceive_AuthorizationIsBoundedAndJoined(t *testing.T) {
	t.Parallel()
	f := newReceiveFixture(t)
	original := f.c.ports.Authorize
	entered := make(chan struct{}, minimumSnapshotOperations)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	f.c.ports.Authorize = func(ctx context.Context, key snapshot.Key) (Owner, error) {
		entered <- struct{}{}
		<-release
		return original(ctx, key)
	}
	returned := make(chan error, minimumSnapshotOperations)
	for range minimumSnapshotOperations {
		go func() { _, err := f.c.BeginSnapshot(t.Context(), f.begin); returned <- err }()
	}
	for range minimumSnapshotOperations {
		select {
		case <-entered:
		case <-receiveContext(t).Done():
			t.Fatal("authorization not reached")
		}
	}
	_, err := f.c.BeginSnapshot(t.Context(), f.begin)
	requireReceiveReason(t, err, snapshot.ReasonCapacity)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := f.c.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("stop did not retain authorization owner", err)
	}
	once.Do(func() { close(release) })
	for range minimumSnapshotOperations {
		if err := <-returned; err == nil {
			t.Fatal("authorization published after stop")
		}
	}
	if err := f.c.Stop(receiveContext(t)); err != nil {
		t.Fatal(err)
	}
	if n, b := receiveCounts(f.c); n != 0 || b != 0 {
		t.Fatal("stopped authorization allocated", n, b)
	}
}
