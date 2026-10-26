//go:build functional

package localdb

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage/walstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type singleReplicaTransport struct{}

func (singleReplicaTransport) Send(context.Context, []raftpb.Message) error {
	return errors.New("unexpected remote recipient in single-replica persistence fixture")
}

// This is one local persistence fixture. Multi-node quorum and failover remain
// exclusively in the Docker suite, rather than being simulated in this process.
func TestSQLiteRecoversThroughPublicGroupAndWAL(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	root := t.TempDir()
	id := testStorageIdentity()
	prewrite := false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := walstore.Open(walstore.Options{RootDir: filepath.Join(root, "raft"), NodeName: id.NodeName, ClusterID: id.ClusterID, SegmentBytes: 32 << 10, Prewrite: &prewrite, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	start := func(mode load.StartMode) (*load.Standalone, *Machine) {
		t.Helper()
		machine, err := Open(ctx, filepath.Join(root, "application"), id)
		if err != nil {
			t.Fatal(err)
		}
		opts := load.StandaloneOptions{Identity: id, InstanceID: "process-" + string(mode), Mode: mode, Store: store, StateMachine: machine, Transport: singleReplicaTransport{}, Logger: logger, Engine: load.EngineOptions{Timing: load.Timing{TickInterval: 10 * time.Millisecond}, SnapshotThreshold: 10000}}
		if mode == load.Founding {
			opts.Peers = []raft.Peer{{ID: id.ReplicaID}}
		}
		owner, err := load.NewStandalone(opts)
		if err != nil {
			_ = machine.Close(context.Background())
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owner.Stop(context.Background()); err != nil {
				t.Error(err)
			}
			if err := machine.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		watch, err := owner.Group().Watch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.Start(ctx); err != nil {
			t.Fatal(err)
		}
		for {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case event, open := <-watch:
				if !open {
					t.Fatal("replica stopped before leadership", owner.Group().Error())
				}
				if event.Ready && event.IsLeader {
					return owner, machine
				}
			}
		}
	}
	owner, machine := start(load.Founding)
	group := owner.Group()
	if state, err := task.ReadState(ctx, group); err != nil || state.Meta.LastTaskID != 0 {
		t.Fatal("fresh state", state, err)
	}
	if _, err := task.ReadTask(ctx, group, 1); err == nil {
		t.Fatal("missing task query succeeded")
	}
	create := func(expected int64) {
		t.Helper()
		p, err := task.Encode("create", task.Create{ExpectedLastTaskID: expected, Tasks: []task.NewTask{{Seed: testSeed, DurationMS: 2000, Executor: executor(2)}}})
		if err != nil {
			t.Fatal(err)
		}
		out, err := load.ProposeRetry(ctx, group.Propose, p, load.ProposalRetryOptions{})
		if err != nil || out.Result.Code != "ok" {
			t.Fatal(out, err)
		}
	}
	create(0)
	if err := group.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	// This second task exists only in the committed tail after the snapshot.
	create(1)
	if err := owner.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := machine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = walstore.Open(walstore.Options{RootDir: filepath.Join(root, "raft"), NodeName: id.NodeName, ClusterID: id.ClusterID, SegmentBytes: 32 << 10, Prewrite: &prewrite, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	next, _ := start(load.Joining)
	state, err := task.ReadState(ctx, next.Group())
	if err != nil || state.Meta.LastTaskID != 2 || len(state.Outstanding) != 2 {
		t.Fatalf("incomplete recovery: %+v %v", state, err)
	}
	currentTask, err := task.ReadTask(ctx, next.Group(), 2)
	if err != nil || currentTask.State != "assigned" || currentTask.Attempt != 1 {
		t.Fatal(currentTask, err)
	}
	page, err := task.ReadTasks(ctx, next.Group(), 0, 1)
	if err != nil || len(page.Tasks) != 1 || page.Next != 1 {
		t.Fatal(page, err)
	}
	page, err = task.ReadTasks(ctx, next.Group(), page.Next, 1)
	if err != nil || len(page.Tasks) != 1 || page.Tasks[0].TaskID != 2 || page.Next != 0 {
		t.Fatal(page, err)
	}
	// Stop before closing the shared borrowed WAL store during cleanup.
	if err := next.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
