//go:build functional

package gquorum

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/load"
)

func twoGroupRecoverySource(t *testing.T) NodeOptions {
	t.Helper()
	retained := recoveryFixtureOptions(t, true)
	options := authorityNodeOptions(t, retained.StorageDir, "second-source-group")
	options.Loads = retained.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	participant, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	transport.node = participant
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx := recoveryBarrierContext(t)
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	views, err := participant.Cluster().ReadTopology(ctx)
	if err != nil {
		t.Fatal(err)
	}
	created, err := participant.Cluster().CreateGroup(ctx, singleReplicaSpec("jobs", nil), MutationOptions{
		ExpectedVersion: views.Latest.Version, Actor: "fixture", Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := participant.Cluster().WaitOperation(ctx, created.OperationID); err != nil {
		t.Fatal(err)
	}
	stopAuthorityNode(t, participant)
	return options
}

func TestRecoverySerialReplayKeepsAllGroupsPrivateWhenOneCheckpointFails(t *testing.T) {
	t.Parallel()
	source := twoGroupRecoverySource(t)
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := facade.Run(t.Context(), operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	options := authorityNodeOptions(t, source.StorageDir, "multi-recovery")
	options.Node = source.Bootstrap.Founders[0].Node
	options.Loads = source.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var first atomic.Bool
	var factories, active, maximum, runs, closed atomic.Int32
	failure := errors.New("one application's portable checkpoint failed")
	options.Loads.Factory = func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		machine := recoveryBarrierMachine{restore: func(ctx context.Context, data []byte) error {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			if first.CompareAndSwap(false, true) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return ensureMachine{}.Restore(ctx, data)
		}, snapshot: func(ctx context.Context) ([]byte, error) {
			if spec.Identity.GroupID == "jobs" {
				return nil, failure
			}
			return ensureMachine{}.Snapshot(ctx)
		}}
		return load.Application{StateMachine: machine, Run: func(context.Context, *load.Group) error { runs.Add(1); return nil },
			Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}
	options, err = RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	transport.node = participant
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	t.Cleanup(unblock)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitRecoverySignal(t, entered)
	ctx := recoveryBarrierContext(t)
	observations, err := participant.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for len(participant.Status().Loads) < 2 {
		select {
		case <-observations:
		case <-ctx.Done():
			t.Fatal("second reservation did not progress independently", participant.Status())
		}
	}
	if factories.Load() != 1 || active.Load() != 1 {
		t.Fatal("recovery callbacks overlapped", factories.Load(), active.Load())
	}
	unblock()
	n := participant.(*node)
	for {
		n.mu.RLock()
		control := n.recoveryCoordinator
		n.mu.RUnlock()
		if control != nil && control.Error() != nil {
			if !errors.Is(control.Error(), failure) {
				t.Fatal(control.Error())
			}
			break
		}
		select {
		case <-observations:
		case <-ctx.Done():
			t.Fatal("checkpoint failure not recorded", participant.Status())
		}
	}
	if factories.Load() != 2 || maximum.Load() != 1 || participant.Status().Ready || len(participant.LoadGroupIDs()) != 0 || runs.Load() != 0 {
		t.Fatal("failed import published or skipped a group", participant.Status(), factories.Load(), maximum.Load(), runs.Load())
	}
	stopAuthorityNode(t, participant)
	if closed.Load() != 2 {
		t.Fatal("failed import lost an application owner", closed.Load())
	}
}
