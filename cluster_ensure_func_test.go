//go:build functional

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
	"github.com/asalimonov/gquorum/topology"
)

// ensureMachine is a minimal state machine for single-replica groups.
type ensureMachine struct{}

func (ensureMachine) Apply(context.Context, load.Command) (load.Result, error) {
	return load.Result{Code: "ok"}, nil
}
func (ensureMachine) Snapshot(context.Context) ([]byte, error) { return []byte("{}"), nil }
func (ensureMachine) Restore(context.Context, []byte) error    { return nil }

// loopbackTransport delivers a single node's own load effects back to it.
type loopbackTransport struct{ node Node }

func (l *loopbackTransport) SendRaft(ctx context.Context, b load.RaftBatch) error {
	return l.node.ReceiveLoadRaft(ctx, b)
}
func (l *loopbackTransport) ReadReplica(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	return l.node.ReadLoadReplica(ctx, q)
}
func (l *loopbackTransport) ApplyAction(ctx context.Context, a load.ActionRequest) (load.ActionResult, error) {
	return l.node.ApplyLoadAction(ctx, a)
}
func (l *loopbackTransport) InstallFence(ctx context.Context, f load.FenceRequest) (load.FenceResult, error) {
	return l.node.InstallLoadFence(ctx, f)
}

func singleReplicaSpec(id string, labels map[string]string) GroupSpec {
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.TargetVoters, voterConfig.MaxSurgeVoters = 1, 0
	return GroupSpec{ID: id, Labels: labels, VoterConfig: &voterConfig, Nodes: []string{"n1"}, Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1}}
}

func TestEnsureGroupCreatesApprovesAndRefusesConflicts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "ensure")
	loopback := &loopbackTransport{}
	o.Loads = LoadOptions{Transport: loopback, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			return load.Application{StateMachine: ensureMachine{}, Close: func(context.Context) error { return nil }}, nil
		}}
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	loopback.node = n
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	c := n.Cluster()
	app := EnsureOptions{Actor: "app"}

	spec := singleReplicaSpec("work", map[string]string{"app": "demo"})
	created, err := c.EnsureGroup(ctx, spec, app)
	if err != nil || created.ID != "work" || len(created.Replicas) != 1 || created.Replicas[0].NodeName != "n1" {
		t.Fatal(created, err)
	}
	again, err := c.EnsureGroup(ctx, spec, app)
	if err != nil || again.Replicas[0].ReplicaID != created.Replicas[0].ReplicaID {
		t.Fatal("repeat was not idempotent", again, err)
	}
	// Local application holds do not erase the group's replicated convergence.
	ref := load.ReplicaRef{GroupID: created.ID, ReplicaID: created.Replicas[0].ReplicaID}
	if err := n.StopLoadGroup(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if held, err := c.EnsureGroup(ctx, spec, app); err != nil || held.Replicas[0].ReplicaID != ref.ReplicaID {
		t.Fatal("EnsureGroup waited on or replaced a held local replica", held, err)
	}
	if _, running := n.LoadGroup(created.ID); running {
		t.Fatal("EnsureGroup cleared a local hold")
	}
	if _, err := n.StartLoadGroup(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureGroup(ctx, singleReplicaSpec("work", map[string]string{"app": "other"}), app); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("different spec accepted", err)
	}
	if _, err := c.EnsureGroup(ctx, spec, EnsureOptions{}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("missing actor accepted", err)
	}

	// A retry after staging without approval approves its own creation.
	own := singleReplicaSpec("second", nil)
	latest, err := c.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateGroup(ctx, own, MutationOptions{ExpectedVersion: latest.Version, Actor: "app", Comment: "staged only"}); err != nil {
		t.Fatal(err)
	}
	if g, err := c.EnsureGroup(ctx, own, EnsureOptions{Actor: "app", Comment: "staged only"}); err != nil || g.ID != "second" {
		t.Fatal("own staged creation not approved", g, err)
	}

	// Foreign staged intent is never approved on someone else's behalf.
	latest, err = c.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := c.CreateGroup(ctx, singleReplicaSpec("third", nil), MutationOptions{ExpectedVersion: latest.Version, Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureGroup(ctx, singleReplicaSpec("fourth", nil), app); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("foreign staged intent did not conflict", err)
	}
	target, err := c.Target(ctx)
	if err != nil || target.Version == staged.Document.Version || target.Group("third") != nil {
		t.Fatal("foreign intent was approved", target.Version, err)
	}
	if existing, err := c.EnsureGroup(ctx, spec, app); err != nil || existing.ID != spec.ID {
		t.Fatal("unrelated staged intent blocked a proven group", existing, err)
	}
	if _, err := c.DeleteGroup(ctx, spec.ID, MutationOptions{ExpectedVersion: staged.Document.Version, Actor: "operator"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureGroup(ctx, spec, app); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("old convergence hid a staged deletion", err)
	}
}

func TestEnsureGroupCreatesAnInMemoryGroup(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "memory")
	loopback := &loopbackTransport{}
	wal := filepath.Join(root, "loads")
	o.Loads = LoadOptions{Transport: loopback, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: wal}},
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			return load.Application{StateMachine: ensureMachine{}, Close: func(context.Context) error { return nil }}, nil
		}}
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	loopback.node = n
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	spec := singleReplicaSpec("cache", nil)
	spec.Storage = topology.StorageMemory
	created, err := n.Cluster().EnsureGroup(ctx, spec, EnsureOptions{Actor: "app"})
	if err != nil || created.Storage != topology.StorageMemory {
		t.Fatal("memory group not recorded", created, err)
	}
	persistent := singleReplicaSpec("cache", nil)
	if _, err := n.Cluster().EnsureGroup(ctx, persistent, EnsureOptions{Actor: "app"}); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("default storage spec matched a memory group", err)
	}
	for {
		g, found := n.LoadGroup("cache")
		if found {
			_, err = g.Propose(ctx, load.Proposal{RequestID: "first", Data: []byte("value")})
		}
		if found && err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("memory replica did not serve", found, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(wal, "cache")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("memory group wrote to the WAL store", err)
	}
}

func TestEnsureGroupReturnsFailedCreationWithoutRetryingOperation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	options := authorityNodeOptions(t, root, "failed-ensure")
	loopback := &loopbackTransport{}
	options.Loads = LoadOptions{Transport: loopback, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			return load.Application{}, fault.ErrInvalid
		}}
	n, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	loopback.node = n
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	c := n.Cluster()
	spec := singleReplicaSpec("failed", nil)
	for range 2 {
		if _, err := c.EnsureGroup(ctx, spec, EnsureOptions{Actor: "app"}); !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("failure was hidden or retried", err)
		}
		view, err := c.ReadTopology(ctx)
		if err != nil || view.Latest.Version != 2 || view.Target.Version != 2 {
			t.Fatal(view, err)
		}
		status, err := c.Status(ctx)
		if err != nil || status.Operator == nil || status.Operator.State != topology.OperatorFailed || status.Operator.Attempt != 1 {
			t.Fatal(status.Operator, err)
		}
	}
}
