//go:build !functional

package gquorum

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestNodeConstructionAndStopBeforeStartAcquireNothing(t *testing.T) {
	t.Parallel()
	o := nodeOptionsForTest(t)
	o.Loads = loadOptionsForTest(t)
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	if n.Status().State != NodeNew || n.Cluster() != n.Cluster() {
		t.Fatal("invalid new node or unstable cluster facade")
	}
	if _, err := os.Stat(o.Meta.DBPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("constructor opened storage", err)
	}
	if _, err := n.StartLoadGroup(context.Background(), load.ReplicaRef{GroupID: "x", ReplicaID: 1}); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	// The listener and dependencies panic on use. An unstarted Node owns none.
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n.Status().State != NodeStopped || n.Status().Ready {
		t.Fatal(n.Status())
	}
	if err := n.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped lifetime restarted", err)
	}
	if err := n.WaitReady(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
