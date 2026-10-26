//go:build functional

package raftengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

type cancelRestore struct {
	machine
	entered chan struct{}
	context context.Context
}

func (m *cancelRestore) Restore(ctx context.Context, _ []byte) error {
	m.context = ctx
	close(m.entered)
	<-ctx.Done()
	return ctx.Err()
}

func TestStartupRecoveryUsesOwnerContext(t *testing.T) {
	t.Parallel()
	for _, managed := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "managed"}[managed], func(t *testing.T) {
			t.Parallel()
			s := openStore(t, t.TempDir(), "a")
			m := &cancelRestore{entered: make(chan struct{})}
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			options := Options{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 1,
				Mode: Founding, Peers: []raft.Peer{{ID: 1}}, Store: s, Machine: m, Transport: endpoint{network: &network{}}}
			if managed {
				options.SnapshotPolicy = SnapshotPolicy{TotalTimeout: 30 * time.Minute, ProgressTimeout: time.Minute}
			}
			done := make(chan error, 1)
			go func() {
				_, err := NewContext(ctx, options)
				done <- err
			}()
			select {
			case <-m.entered:
			case <-ctx.Done():
				t.Fatal("restore did not start")
			}
			restoreDeadline, bounded := m.context.Deadline()
			if managed {
				ownerDeadline, ownerBounded := ctx.Deadline()
				if bounded != ownerBounded || bounded && !restoreDeadline.Equal(ownerDeadline) {
					t.Fatal("managed replay replaced its owner's deadline", restoreDeadline, ownerDeadline)
				}
			} else if !bounded || time.Until(restoreDeadline) > 5*time.Second {
				t.Fatal("legacy startup lost its bounded deadline", restoreDeadline)
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
		})
	}
}
