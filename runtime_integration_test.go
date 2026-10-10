package gquorum

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
)

// This caller adapter verifies every effect on receipt. It defines its machine
// and transport independently of the library, even for a one-node test group.
type runtimeController struct {
	r                          *Runtime
	m                          *meta.Group
	store                      storage.Store
	entered, canceled, release chan struct{}
	dbPath                     string
}

func (c *runtimeController) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.GroupStatus{}, err
	}
	if g, exists := c.r.Load(id); exists {
		return g.Status(), nil
	}
	return topology.GroupStatus{GroupID: id}, nil
}
func (c *runtimeController) CreateGroup(ctx context.Context, a topology.AuthorizedStep) error {
	if err := c.m.Authorize(ctx, a); err != nil {
		return err
	}
	doc, err := c.m.Target(ctx)
	if err != nil {
		return err
	}
	want := doc.Group(a.GroupID)
	if want == nil || a.Kind != topology.StepCreateFoundingGroup {
		return fault.ErrIdentityMismatch
	}
	var id uint64
	var peers []raft.Peer
	for _, replica := range want.Replicas {
		if replica.Role == topology.ReplicaVoter {
			peers = append(peers, raft.Peer{ID: replica.ReplicaID})
		}
		if replica.NodeName == c.m.Identity().NodeName {
			id = replica.ReplicaID
		}
	}
	if id == 0 {
		return fault.ErrIdentityMismatch
	}
	g, exists := c.r.Load(a.GroupID)
	if exists {
		if g.ReplicaID() != id {
			return fault.ErrIdentityMismatch
		}
	} else {
		opts := testLoadOptions(c.store, a.GroupID, id)
		opts.Peers = peers
		g, err = c.r.OpenLoad(opts)
		if err != nil {
			return err
		}
	}
	if !g.Running() {
		if err = g.Start(context.Background()); err != nil {
			_ = c.r.StopLoad(context.Background(), a.GroupID, id)
			return err
		}
	}
	if err := g.WaitReady(ctx); err != nil {
		return err
	}
	if c.entered != nil {
		close(c.entered)
		<-ctx.Done()
		// Root shutdown must retain loads and the meta ledger until this
		// admitted callback returns, including its cancellation cleanup.
		if !g.Running() {
			return errors.New("load stopped before effect joined")
		}
		if record, found, err := c.m.LookupLoadReplica(context.Background(), a.GroupID); err != nil || !found || record.ReplicaID != id {
			return errors.New("ledger closed before effect joined")
		}
		close(c.canceled)
		<-c.release
		return ctx.Err()
	}
	return nil
}
func (c *runtimeController) PrepareReplica(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *runtimeController) ApplyMembership(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *runtimeController) TransferLeader(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *runtimeController) StopReplica(ctx context.Context, a topology.AuthorizedStep) error {
	if err := c.m.Authorize(ctx, a); err != nil {
		return err
	}
	if a.NodeName != c.m.Identity().NodeName {
		return fault.ErrIdentityMismatch
	}
	return c.r.StopLoad(ctx, a.GroupID, a.ReplicaID)
}
func (c *runtimeController) CleanupReplicaState(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *runtimeController) DeleteGroupState(ctx context.Context, a topology.AuthorizedStep) error {
	if err := c.m.Authorize(ctx, a); err != nil {
		return err
	}
	record, found, err := c.m.LookupLoadReplica(ctx, a.GroupID)
	if err != nil {
		return err
	}
	probe, err := c.store.Probe(ctx, a.GroupID)
	if err != nil {
		return err
	}
	if !found {
		if probe.Exists {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	if probe.Exists && probe.Identity != (storage.Identity{NodeName: record.NodeName, ClusterID: record.ClusterID, GroupID: a.GroupID, ReplicaID: record.ReplicaID}) {
		return fault.ErrIdentityMismatch
	}
	if _, err = c.store.Quarantine(ctx, a.GroupID, record.ReplicaID); err != nil {
		return err
	}
	return c.m.ForgetLoadReplica(ctx, a, record.ReplicaID)
}

func startRuntimeMeta(t *testing.T, r *Runtime, controller *runtimeController) *meta.Group {
	t.Helper()
	opts := testMetaOptions(t)
	if controller.dbPath != "" {
		opts.DBPath = controller.dbPath
		opts.Identity.InstanceID = "restarted-process"
	}
	controller.dbPath = opts.DBPath
	opts.LoadGroups = controller
	m, err := r.OpenMeta(opts)
	if err != nil {
		t.Fatal(err)
	}
	controller.m = m
	if err = m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = m.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRuntimeIdentityMismatchParkSurvivesMetaRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	store := testStore(t)
	first := testRuntime(t)
	c := &runtimeController{r: first, store: store}
	m := startRuntimeMeta(t, first, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	created, err := m.CreateLoadGroup(ctx, meta.GroupSpec{ID: "work"}, meta.MutationOptions{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := m.WaitExecution(ctx, created.ExecutionID)
	if err != nil || status.State != topology.ExecutionSucceeded {
		t.Fatal(status, err)
	}
	record, found, err := m.LookupLoadReplica(ctx, "work")
	if err != nil || !found {
		t.Fatal(record, found, err)
	}
	if err = first.StopLoad(ctx, "work", 1); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Quarantine(ctx, "work", 1); err != nil {
		t.Fatal(err)
	}
	foreign, err := load.New(testLoadOptions(store, "work", 99))
	if err != nil {
		t.Fatal(err)
	}
	if err = foreign.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = first.OpenLoad(testLoadOptions(store, "work", 1)); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	if err = first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	second := testRuntime(t)
	next := &runtimeController{r: second, store: store, dbPath: c.dbPath}
	m = startRuntimeMeta(t, second, next)
	if got, found, err := m.LookupLoadReplica(ctx, "work"); err != nil || !found || got != record {
		t.Fatal("restart lost local pin ledger", got, found, err)
	}
	if _, err = second.OpenLoad(testLoadOptions(store, "work", 1)); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("restart accepted foreign WAL", err)
	}
	if parked := second.ParkedLoads()["work"]; parked.Reason != ParkIdentityMismatch || parked.Identity.ReplicaID != 1 {
		t.Fatal(parked)
	}
	if probe, err := store.Probe(ctx, "work"); err != nil || probe.Identity.ReplicaID != 99 {
		t.Fatal("restart altered foreign state", probe, err)
	}
}

func TestRuntimeApprovedLoadParksLostWALRecoversAndRetires(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	store, root := testStoreAt(t)
	r := testRuntime(t)
	c := &runtimeController{r: r, store: store}
	m := startRuntimeMeta(t, r, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	staged, err := m.CreateLoadGroup(ctx, meta.GroupSpec{ID: "work"}, meta.MutationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	opts := testLoadOptions(store, "work", 1)
	if _, err = r.OpenLoad(opts); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("Latest started unapproved group", err)
	}
	if probe, err := store.Probe(ctx, "work"); err != nil || probe.Exists {
		t.Fatal("staged refusal mutated storage", probe, err)
	}
	id, err := m.Apply(ctx, staged.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	status, err := m.WaitExecution(ctx, id)
	if err != nil || status.State != topology.ExecutionSucceeded {
		t.Fatal(status, err)
	}
	g, exists := r.Load("work")
	if !exists || !g.Running() {
		t.Fatal("approved load did not start")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !g.IsLeader() {
		select {
		case <-ctx.Done():
			t.Fatal("load did not elect leader")
		case <-ticker.C:
		}
	}
	if _, err = g.Propose(ctx, load.Proposal{RequestID: "first", Data: []byte("committed")}); err != nil {
		t.Fatal(err)
	}
	before, found, err := m.LookupLoadReplica(ctx, "work")
	if err != nil || !found || before.ReplicaID != 1 {
		t.Fatal(before, found, err)
	}
	if err = r.StopLoad(ctx, "work", 1); err != nil {
		t.Fatal(err)
	}
	backup, err := store.Quarantine(ctx, "work", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.OpenLoad(opts); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("missing WAL silently refounded", err)
	}
	if parked := r.ParkedLoads()["work"]; parked.Reason != ParkStoreMissing || parked.Identity.ReplicaID != 1 {
		t.Fatal(parked)
	}
	if record, found, err := m.LookupLoadReplica(ctx, "work"); err != nil || !found || record != before {
		t.Fatal("park erased boot ledger", record, found, err)
	}
	if err = os.Rename(backup, filepath.Join(root, "work")); err != nil {
		t.Fatal(err)
	}
	opts.Machine = &testMachine{}
	g, err = r.OpenLoad(opts)
	if err != nil {
		t.Fatal("restored WAL still parked", err)
	}
	if len(r.ParkedLoads()) != 0 {
		t.Fatal("successful restore retained parked observation")
	}
	if err = g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	data, _ := opts.Machine.Snapshot(ctx)
	if string(data) != "committed" {
		t.Fatal("committed tail not replayed", string(data))
	}
	deleted, err := m.DeleteLoadGroup(ctx, "work", meta.MutationOptions{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err = m.WaitExecution(ctx, deleted.ExecutionID)
	if err != nil || status.State != topology.ExecutionSucceeded {
		t.Fatal(status, err)
	}
	if _, exists := r.Load("work"); exists {
		t.Fatal("deleted handle still registered")
	}
	if _, found, err := m.LookupLoadReplica(ctx, "work"); err != nil || found {
		t.Fatal("authorized cleanup kept ledger", found, err)
	}
	if _, err = r.OpenLoad(opts); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal("retired ID reopened", err)
	}
}

func TestRuntimeCloseJoinsEffectsBeforeLoadAndDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	store := testStore(t)
	r := testRuntime(t)
	c := &runtimeController{r: r, store: store, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-c.release:
		default:
			close(c.release)
		}
	}()
	m := startRuntimeMeta(t, r, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := m.CreateLoadGroup(ctx, meta.GroupSpec{ID: "work"}, meta.MutationOptions{Apply: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.entered:
	case <-ctx.Done():
		t.Fatal("effect did not create load")
	}
	r.RequestClose()
	select {
	case <-c.canceled:
	case <-ctx.Done():
		t.Fatal("effect resources closed before cancellation cleanup")
	}
	short, stop := context.WithCancel(context.Background())
	stop()
	if err := r.Close(short); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal(err)
	}
	if g, found := r.Load("work"); !found || !g.Running() {
		t.Fatal("timeout lost live ownership")
	}
	close(c.release)
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.LoadIDs()) != 0 {
		t.Fatal("teardown did not join loads")
	}
	if _, _, err := m.LookupLoadReplica(ctx, "work"); err == nil {
		t.Fatal("meta database left open")
	}
	if probe, err := store.Probe(ctx, "work"); err != nil || !probe.HasLog {
		t.Fatal("caller store not retained", probe, err)
	}
}
