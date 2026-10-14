//go:build functional

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

// The application owns this store so tests can model loss/replacement through
// its public contract. Node still owns all consensus and replica orchestration.
func borrowedAPICluster(t *testing.T, configure func(*quorum.NodeOptions)) (*apiCluster, *walstore.Store, string) {
	t.Helper()
	var store *walstore.Store
	var root string
	c := newSingleNodeFixture(t, true, func(_ int, o *quorum.NodeOptions) {
		wal := o.Loads.Storage.WAL
		root = wal.RootDir
		var err error
		store, err = walstore.Open(walstore.Options{RootDir: root, NodeName: o.Identity.NodeName, ClusterID: o.Identity.ClusterID, SegmentBytes: wal.SegmentBytes, Prewrite: wal.Prewrite, Logger: o.Logger})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		o.Loads.Storage = quorum.LoadStorageOptions{Store: store, Retirement: store}
		if configure != nil {
			configure(o)
		}
	})
	return c, store, root
}

func waitAPIOperator(ctx context.Context, t *testing.T, n quorum.Node, id topology.OperatorID) {
	t.Helper()
	status, err := n.Cluster().WaitOperator(ctx, id)
	if err != nil || status.State != topology.OperatorSucceeded {
		t.Fatal("operator", status, err)
	}
}

func restartAPINode(t *testing.T, c *apiCluster, i int) quorum.Node {
	t.Helper()
	opts := c.options[i]
	l, err := net.Listen("tcp", opts.Meta.Network.AdvertiseAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	opts.Meta.Network.Listener = l
	opts.Identity.InstanceID += "-restart"
	n, err := quorum.NewNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	c.nodes[i], c.options[i] = n, opts
	c.network.install(n)
	if err := n.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPublicNodeIdentityMismatchParkSurvivesMetaRestart(t *testing.T) {
	t.Parallel()
	c, store, _ := borrowedAPICluster(t, nil)
	c.startAll(t)
	n := c.nodes[0]
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()
	result, err := n.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "work"}, quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	waitAPIOperator(ctx, t, n, result.OperatorID)
	g, ok := n.LoadGroup("work")
	if !ok {
		t.Fatal("approved replica missing")
	}
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: g.ReplicaID()}
	if err := n.StopLoadGroup(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Quarantine(ctx, "work", ref.ReplicaID); err != nil {
		t.Fatal(err)
	}
	foreign := storage.Identity{NodeName: n.Identity().NodeName, ClusterID: n.Identity().ClusterID, GroupID: "work", ReplicaID: 99}
	if err := store.BindReplica(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	if err := store.Ensure(ctx, "work"); err != nil {
		t.Fatal(err)
	}
	oldIdentity := n.Identity()
	for attempt := range 2 {
		if attempt != 0 {
			if err := n.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			n = restartAPINode(t, c, 0)
			if n.Identity().MemberID != oldIdentity.MemberID || n.Identity().InstanceID == oldIdentity.InstanceID {
				t.Fatal("restart changed member or retained process", n.Identity(), oldIdentity)
			}
		}
		_, err := n.StartLoadGroup(ctx, ref)
		var parked *quorum.LoadParkedError
		if !errors.As(err, &parked) || !errors.Is(err, fault.ErrReplicaNotReady) || parked.Reason != quorum.ParkIdentityMismatch || parked.Identity.ReplicaID != ref.ReplicaID {
			t.Fatal("foreign store accepted or diagnostic lost", err)
		}
		view := n.ParkedLoads()
		original := view["work"]
		delete(view, "work")
		if n.ParkedLoads()["work"] != original {
			t.Fatal("parking map aliases node state")
		}
		if probe, err := store.Probe(ctx, "work"); err != nil || probe.Identity != foreign {
			t.Fatal("restart altered foreign state", probe, err)
		}
	}
}

func TestPublicNodeApprovedLoadParksLostWALRecoversAndRetires(t *testing.T) {
	t.Parallel()
	c, store, root := borrowedAPICluster(t, nil)
	c.startAll(t)
	n := c.nodes[0]
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()
	staged, err := n.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "work"}, quorum.MutationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: staged.Document.Group("work").Replicas[0].ReplicaID}
	if _, err := n.StartLoadGroup(ctx, ref); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("staged group did not wait for committed authority", err)
	}
	if probe, err := store.Probe(ctx, "work"); err != nil || probe.Exists {
		t.Fatal("staged refusal mutated store", probe, err)
	}
	id, err := n.Cluster().Approve(ctx, staged.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	waitAPIOperator(ctx, t, n, id)
	g, found := n.LoadGroup("work")
	if !found || !g.Running() {
		t.Fatal("approved replica did not start")
	}
	apiEventually(t, g.IsLeader)
	if _, err := g.Propose(ctx, load.Proposal{RequestID: "first", Data: []byte{7}}); err != nil {
		t.Fatal(err)
	}
	if err := n.StopLoadGroup(ctx, ref); err != nil {
		t.Fatal(err)
	}
	backup, err := store.Quarantine(ctx, "work", ref.ReplicaID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.StartLoadGroup(ctx, ref); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("missing WAL silently refounded", err)
	}
	if parked := n.ParkedLoads()["work"]; parked.Reason != quorum.ParkStoreMissing || parked.Identity.ReplicaID != ref.ReplicaID {
		t.Fatal(parked)
	}
	if err := os.Rename(backup, filepath.Join(root, "work")); err != nil {
		t.Fatal(err)
	}
	reopened, err := n.StartLoadGroup(ctx, ref)
	if err != nil || !reopened.Running() {
		t.Fatal("restored WAL remained parked", err)
	}
	if len(n.ParkedLoads()) != 0 || c.value(n.Identity().NodeName, "work") != 7 {
		t.Fatal("successful restore lost committed tail or remained parked", n.ParkedLoads())
	}
	apiEventually(t, reopened.IsLeader)
	deleted, err := n.Cluster().DeleteGroup(ctx, "work", quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	waitAPIOperator(ctx, t, n, deleted.OperatorID)
	if _, exists := n.LoadGroup("work"); exists {
		t.Fatal("deleted handle stayed registered")
	}
	if probe, err := store.Probe(ctx, "work"); err != nil || probe.Exists {
		t.Fatal("deleted store retained at live path", probe, err)
	}
	if _, err := n.StartLoadGroup(ctx, ref); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal("retired ID reopened", err)
	}
}

type joiningActionTransport struct {
	load.Transport
	entered, canceled, release chan struct{}
	once                       sync.Once
}

func (s *joiningActionTransport) ApplyAction(ctx context.Context, req load.ActionRequest) (load.ActionResult, error) {
	result, err := s.Transport.ApplyAction(ctx, req)
	if err != nil || req.Authorization.Kind != topology.StepCreateFoundingGroup {
		return result, err
	}
	s.once.Do(func() {
		close(s.entered)
		<-ctx.Done()
		close(s.canceled)
		<-s.release
	})
	return result, ctx.Err()
}

func TestPublicNodeStopJoinsEffectsBeforeDatabase(t *testing.T) {
	t.Parallel()
	blocked := &joiningActionTransport{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	c, store, _ := borrowedAPICluster(t, func(o *quorum.NodeOptions) {
		blocked.Transport = o.Loads.Transport
		o.Loads.Transport = blocked
	})
	var once sync.Once
	release := func() { once.Do(func() { close(blocked.release) }) }
	defer release()
	c.startAll(t)
	n := c.nodes[0]
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	if _, err := n.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "work"}, quorum.MutationOptions{Approve: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal("effect did not create replica")
	}
	n.RequestStop()
	select {
	case <-blocked.canceled:
	case <-ctx.Done():
		t.Fatal("effect did not receive cancellation")
	}
	brief, end := context.WithTimeout(ctx, 30*time.Millisecond)
	defer end()
	if err := n.Stop(brief); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("shutdown lost in-flight effect", err)
	}
	if len(n.Status().Loads) != 1 {
		t.Fatal("shutdown forgot replica before effect joined", n.Status())
	}
	path := filepath.Join(c.options[0].StorageDir, "_meta.bolt")
	if db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 30 * time.Millisecond}); !errors.Is(err, bolt.ErrTimeout) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatal("meta ledger closed before effect joined", err)
	}
	release()
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if len(n.LoadGroupIDs()) != 0 || len(n.Status().Loads) != 0 {
		t.Fatal("shutdown retained replica ownership", n.Status())
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal("completed shutdown retained meta DB", err)
	}
	_ = db.Close()
	if probe, err := store.Probe(ctx, "work"); err != nil || !probe.HasLog {
		t.Fatal("borrowed WAL store not retained", probe, err)
	}
}
