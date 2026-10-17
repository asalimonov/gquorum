//go:build functional

package gquorum

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// One local participant exercises the real service/controller/managed ownership
// boundary. Multi-node quorum isolation belongs to the Docker suite.
func TestIndependentGroupConvergesWhileFactoryRemainsOwned(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "resume original operation"
		if shutdown {
			name = "shutdown joins late factory"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			o := authorityNodeOptions(t, root, "isolation")
			o.CallbackTimeout = 30 * time.Second
			loopback := &loopbackTransport{}
			entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var starts atomic.Int32
			var releaseOnce, enteredOnce, closeOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			o.Loads = LoadOptions{Transport: loopback,
				Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
				Factory: func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
					if spec.Identity.GroupID == "blocked" {
						starts.Add(1)
						enteredOnce.Do(func() { close(entered) })
						<-release // Deliberately ignores cancellation; Node must retain ownership.
					}
					return load.Application{StateMachine: ensureMachine{}, Close: func(context.Context) error {
						if spec.Identity.GroupID == "blocked" {
							closeOnce.Do(func() { close(closed) })
						}
						return nil
					}}, nil
				}}
			n, err := NewNode(o)
			if err != nil {
				t.Fatal(err)
			}
			loopback.node = n
			t.Cleanup(func() { unblock(); stopAuthorityNode(t, n) })
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			if err := n.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := n.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			c := n.Cluster()
			first, err := c.CreateGroup(ctx, singleReplicaSpec("blocked", nil), MutationOptions{
				ExpectedVersion: 1, Actor: "app", Approve: true})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("factory was not admitted", ctx.Err())
			}
			second, err := c.CreateGroup(ctx, singleReplicaSpec("healthy", nil), MutationOptions{
				ExpectedVersion: first.Document.Version, Actor: "app", Approve: true})
			if err != nil {
				t.Fatal("unrelated approval waited on the first factory", err)
			}
			status, err := c.WaitOperation(ctx, second.OperationID)
			if err != nil || status.State != topology.OperationSucceeded {
				t.Fatal(status, err)
			}
			local, err := c.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range local.Groups {
				if row.GroupID != topology.MetaGroupID && row.GroupID != "healthy" {
					continue
				}
				observed := row.Observation
				if observed == nil || !observed.Found || observed.Reporter != n.Identity() ||
					observed.SourceVersion != row.ConvergedVersion || observed.ReporterReplicaID == 0 ||
					observed.ReceivedAt.IsZero() || observed.LeaderID != observed.ReporterReplicaID {
					t.Fatal("real status producer lost exact observation provenance", row)
				}
			}
			page, err := c.ReadProgress(ctx, topology.ProgressQuery{})
			if err != nil || page.TargetVersion != second.Document.Version || page.FullyConverged.Version != 1 {
				t.Fatal("global completion hid pending work", page, err)
			}
			for _, row := range page.Groups {
				switch row.GroupID {
				case "blocked":
					if row.Converged != nil || row.OperationID != first.OperationID {
						t.Fatal("new approval replaced unrelated owner", row)
					}
				case "healthy":
					if row.ConvergedVersion != second.Document.Version || row.Converged == nil {
						t.Fatal(row)
					}
				}
			}
			if _, found := n.LoadGroup("blocked"); found || starts.Load() != 1 {
				t.Fatal("unready factory published or acquired twice", found, starts.Load())
			}
			wait, stopWait := context.WithTimeout(ctx, 30*time.Millisecond)
			_, err = c.WaitOperation(wait, first.OperationID)
			stopWait()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("waiter did not cancel independently", err)
			}
			if shutdown {
				n.RequestStop()
				join, stopJoin := context.WithTimeout(ctx, 30*time.Millisecond)
				err = n.Stop(join)
				stopJoin()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("shutdown released an owned factory", err)
				}
				select {
				case <-closed:
					t.Fatal("application closed before factory returned")
				default:
				}
				unblock()
				if err := n.Stop(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case <-closed:
				default:
					t.Fatal("late factory application was not released by joined Stop")
				}
			} else {
				unblock()
				status, err := c.WaitOperation(ctx, first.OperationID)
				if err != nil || status.State != topology.OperationSucceeded {
					t.Fatal(status, err)
				}
				page, err := c.ReadProgress(ctx, topology.ProgressQuery{GroupID: "blocked"})
				if err != nil || len(page.Groups) != 1 || page.Groups[0].ConvergedVersion != first.Document.Version ||
					page.FullyConverged.Version != second.Document.Version {
					t.Fatal("resumed scope used a newer document as its source", page, err)
				}
			}
			if starts.Load() != 1 {
				t.Fatal("retry reacquired the application", starts.Load())
			}
		})
	}
}
