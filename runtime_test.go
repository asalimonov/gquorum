package gquorum

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type testMachine struct {
	mu    sync.Mutex
	value []byte
}

func (m *testMachine) Apply(ctx context.Context, c load.Command) (load.Result, error) {
	if err := ctx.Err(); err != nil {
		return load.Result{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = append([]byte(nil), c.Data...)
	return load.Result{Code: "ok"}, nil
}
func (m *testMachine) Snapshot(context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.value) == 0 {
		return []byte("empty"), nil
	}
	return append([]byte(nil), m.value...), nil
}
func (m *testMachine) Restore(ctx context.Context, b []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = append([]byte(nil), b...)
	return nil
}

type testTransport struct{}

func (testTransport) Send(ctx context.Context, _ []raftpb.Message) error { return ctx.Err() }
func testDeadline(t *testing.T) <-chan time.Time                         { t.Helper(); return time.After(2 * time.Second) }
func testStore(t *testing.T) *walstore.Store {
	s, _ := testStoreAt(t)
	return s
}
func testStoreAt(t *testing.T) (*walstore.Store, string) {
	t.Helper()
	prewrite := false
	root := t.TempDir()
	s, err := walstore.Open(walstore.Options{RootDir: root, NodeName: "a", ClusterID: "cluster", SegmentBytes: 32 << 10, Prewrite: &prewrite})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, root
}
func testLoadOptions(s storage.Store, id string, replica uint64) load.Options {
	return load.Options{GroupID: id, NodeName: "a", ClusterID: "cluster", ReplicaID: replica, Mode: load.Founding, Peers: []raft.Peer{{ID: replica}}, Store: s, Machine: &testMachine{}, Transport: testTransport{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Timing: load.Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 20}}
}
func testRuntime(t *testing.T) *Runtime {
	t.Helper()
	r, err := New(Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return r
}
func testMetaOptions(t *testing.T) metaservice.Options {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return metaservice.Options{Identity: topology.Identity{NodeName: "a", ClusterID: "cluster", InstanceID: "process", MemberID: 1}, DBPath: filepath.Join(t.TempDir(), "meta.bolt"), Founders: []metaservice.FoundingMember{{Node: topology.Node{Name: "a", Type: "worker"}, MemberID: 1, Address: listener.Addr().String()}}, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1}, Timing: metaservice.Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 20}, Network: metaservice.NetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("test-credential")}, Reconciliation: metaservice.ReconciliationOptions{Disabled: true}}
}

func TestRuntimeConstructorsRegistryAndSharedOwnership(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	r := testRuntime(t)
	g, err := r.OpenLoad(testLoadOptions(store, "z", 7))
	if err != nil {
		t.Fatal(err)
	}
	if g.Running() {
		t.Fatal("constructor started workers")
	}
	if _, err = r.OpenLoad(testLoadOptions(store, "z", 7)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("duplicate replaced live ownership", err)
	}
	if _, err = r.OpenLoad(testLoadOptions(store, "b", 3)); err != nil {
		t.Fatal(err)
	}
	ids := r.LoadIDs()
	if len(ids) != 2 || ids[0] != "b" || ids[1] != "z" {
		t.Fatal(ids)
	}
	ids[0] = "changed"
	if r.LoadIDs()[0] != "b" {
		t.Fatal("registry alias")
	}
	if err = r.StopLoad(context.Background(), "z", 8); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if _, exists := r.Load("z"); !exists {
		t.Fatal("stale stop unregistered handle")
	}
	if err = r.StopLoad(context.Background(), "z", 7); err != nil {
		t.Fatal(err)
	}
	if _, err = r.OpenLoad(testLoadOptions(store, "z", 7)); err != nil {
		t.Fatal("retained handle failed reopen", err)
	}
	if err = r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.LoadIDs()) != 0 {
		t.Fatal("teardown retained handles")
	}
	probe, err := store.Probe(context.Background(), "z")
	if err != nil || !probe.HasLog {
		t.Fatal("runtime closed/wiped injected store", probe, err)
	}
	if _, err = r.OpenLoad(testLoadOptions(store, "other", 2)); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

func TestRuntimeOwnerRejectedBeforeStoreMutation(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	r := testRuntime(t)
	if _, err := r.OpenLoad(testLoadOptions(store, "first", 7)); err != nil {
		t.Fatal(err)
	}
	wrong := testLoadOptions(store, "wrong", 3)
	wrong.NodeName = "foreign"
	if _, err := r.OpenLoad(wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	probe, err := store.Probe(context.Background(), "wrong")
	if err != nil || probe.Exists {
		t.Fatal(probe, err)
	}
	if _, err := r.OpenMeta(testMetaOptions(t)); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("attached meta after unapproved loads", err)
	}
}

func TestRuntimeMetaConstructorAndStoppedAdmission(t *testing.T) {
	t.Parallel()
	r := testRuntime(t)
	opts := testMetaOptions(t)
	m, err := r.OpenMeta(opts)
	if err != nil {
		t.Fatal(err)
	}
	if got, found := r.Meta(); !found || got != m || got.Identity() != opts.Identity {
		t.Fatal(got, found)
	}
	if _, err = r.OpenMeta(opts); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	store := testStore(t)
	if _, err = r.OpenLoad(testLoadOptions(store, "work", 7)); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("constructed meta authorized unopened load", err)
	}
	probe, err := store.Probe(context.Background(), "work")
	if err != nil || probe.Exists {
		t.Fatal("refusal mutated store", probe, err)
	}
}

type heldProbe struct {
	storage.Store
	entered, release chan struct{}
	once             sync.Once
}

func (s *heldProbe) Probe(ctx context.Context, id string) (storage.Probe, error) {
	s.once.Do(func() {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	})
	return s.Store.Probe(ctx, id)
}
func TestRuntimeCloseDuringConstructorRetainsOwnershipAndJoins(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	r := testRuntime(t)
	held := &heldProbe{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-held.release:
		default:
			close(held.release)
		}
	}()
	opened := make(chan error, 1)
	go func() { _, err := r.OpenLoad(testLoadOptions(held, "work", 7)); opened <- err }()
	select {
	case <-held.entered:
	case <-testDeadline(t):
		t.Fatal("constructor did not enter probe")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	if len(r.LoadIDs()) != 0 {
		t.Fatal("unconstructed handle exposed")
	}
	if _, err := r.OpenLoad(testLoadOptions(store, "other", 8)); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	close(held.release)
	select {
	case err := <-opened:
		if !errors.Is(err, fault.ErrStopped) {
			t.Fatal(err)
		}
	case <-testDeadline(t):
		t.Fatal("constructor did not join")
	}
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.LoadIDs()) != 0 {
		t.Fatal("constructor published during close")
	}
	if _, err := store.Probe(context.Background(), "work"); err != nil {
		t.Fatal("caller store was closed", err)
	}
}

func TestRuntimeConcurrentCloseAndCanceledStopGate(t *testing.T) {
	t.Parallel()
	r := testRuntime(t)
	if err := r.takeGate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.StopLoad(ctx, "work", 7); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r.releaseGate()
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if err := r.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestRuntimeForeignWALRetainedUntilExactCallerQuarantine(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	r := testRuntime(t)
	foreign, err := load.New(testLoadOptions(store, "work", 99))
	if err != nil {
		t.Fatal(err)
	}
	if err = foreign.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = r.OpenLoad(testLoadOptions(store, "work", 7)); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	if park := r.ParkedLoads()["work"]; park.Reason != ParkIdentityMismatch {
		t.Fatal(park)
	}
	probe, err := store.Probe(context.Background(), "work")
	if err != nil || !probe.Exists || probe.Identity.ReplicaID != 99 {
		t.Fatal("foreign WAL was moved or overwritten", probe, err)
	}
	if _, err = store.Quarantine(context.Background(), "work", 7); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("wrong quarantine identity accepted", err)
	}
	if _, err = store.Quarantine(context.Background(), "work", 99); err != nil {
		t.Fatal(err)
	}
	if _, err = r.OpenLoad(testLoadOptions(store, "work", 7)); err != nil {
		t.Fatal(err)
	}
	if len(r.ParkedLoads()) != 0 {
		t.Fatal("new approved storage did not clear parked status")
	}
}

type failingMachine struct{ testMachine }

func (*failingMachine) Restore(context.Context, []byte) error {
	return errors.New("invalid application snapshot")
}
func TestRuntimeFailedRestoreParksAndCanRetryWithoutLosingStore(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	r := testRuntime(t)
	opts := testLoadOptions(store, "work", 7)
	opts.Machine = &failingMachine{}
	if _, err := r.OpenLoad(opts); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	if park := r.ParkedLoads()["work"]; park.Reason != ParkStoreUnreadable {
		t.Fatal(park)
	}
	if len(r.LoadIDs()) != 0 {
		t.Fatal("failed constructor published handle")
	}
	probe, err := store.Probe(context.Background(), "work")
	if err != nil || !probe.Exists || probe.HasLog {
		t.Fatal(probe, err)
	}
	opts.Machine = &testMachine{}
	if _, err = r.OpenLoad(opts); err != nil {
		t.Fatal("retry lost store ownership", err)
	}
	if len(r.ParkedLoads()) != 0 {
		t.Fatal("successful retry remained parked")
	}
}
