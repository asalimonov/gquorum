//go:build functional

package raftengine

import (
	"context"
	"errors"
	"testing"

	"go.etcd.io/raft/v3"
)

type cancelRestore struct {
	machine
	entered chan struct{}
}

func (m *cancelRestore) Restore(ctx context.Context, _ []byte) error {
	close(m.entered)
	<-ctx.Done()
	return ctx.Err()
}

func TestStartupRecoveryUsesOwnerContext(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "a")
	m := &cancelRestore{entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := NewContext(ctx, Options{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 1,
			Mode: Founding, Peers: []raft.Peer{{ID: 1}}, Store: s, Machine: m, Transport: endpoint{network: &network{}}})
		done <- err
	}()
	select {
	case <-m.entered:
	case <-ctx.Done():
		t.Fatal("restore did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-testContext(t).Done():
		t.Fatal("constructor ignored owner cancellation")
	}
	if _, err := s.Probe(testContext(t), "work"); err != nil {
		t.Fatal("constructor closed borrowed store:", err)
	}
}
