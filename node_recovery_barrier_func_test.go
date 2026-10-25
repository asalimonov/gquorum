//go:build functional

package gquorum

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"go.etcd.io/raft/v3/raftpb"
)

func newRecoveryLoadNode(t *testing.T, factory GroupFactory, hooks lifecycle.Hooks) (Node, *Recovery, recovery.Operation) {
	t.Helper()
	source := recoveryFixtureOptions(t, true)
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := facade.Run(t.Context(), operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	options := authorityNodeOptions(t, source.StorageDir, "recovery-barrier")
	options.Node = source.Bootstrap.Founders[0].Node
	options.Loads = source.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	options.Loads.Factory = factory
	options.Lifecycle.Hooks = hooks
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
	return participant, facade, operation
}

func recoveryBarrierContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func awaitRecoverySignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-recoveryBarrierContext(t).Done():
		t.Fatal("recovery barrier not reached")
	}
}

type recoveryBarrierMachine struct {
	ensureMachine
	snapshot func(context.Context) ([]byte, error)
	restore  func(context.Context, []byte) error
}

func (m recoveryBarrierMachine) Snapshot(ctx context.Context) ([]byte, error) {
	if m.snapshot != nil {
		return m.snapshot(ctx)
	}
	return m.ensureMachine.Snapshot(ctx)
}
func (m recoveryBarrierMachine) Restore(ctx context.Context, data []byte) error {
	if m.restore != nil {
		return m.restore(ctx, data)
	}
	return m.ensureMachine.Restore(ctx, data)
}

func TestRecoveryPrivateCheckpointRejectsForwardedBusinessAndJoinsStop(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var factories, runs, closed atomic.Int32
	participant, facade, operation := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: recoveryBarrierMachine{snapshot: func(ctx context.Context) ([]byte, error) {
			close(entered)
			<-release
			return ensureMachine{}.Snapshot(ctx)
		}}, Run: func(context.Context, *load.Group) error { runs.Add(1); return nil },
			Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}, nil)
	t.Cleanup(unblock)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitRecoverySignal(t, entered)
	if participant.Status().Ready || len(participant.LoadGroupIDs()) != 0 || factories.Load() != 1 || runs.Load() != 0 {
		t.Fatal("private readiness was publicly released", participant.Status(), factories.Load(), runs.Load())
	}
	state := participant.Status()
	if len(state.Loads) != 1 || state.Loads[0].State != "recovery_ready" {
		t.Fatal(state.Loads)
	}
	ref := load.ReplicaRef{GroupID: state.Loads[0].Identity.GroupID, ReplicaID: state.Loads[0].Identity.ReplicaID}
	peer := load.Peer{Node: participant.Identity(), Replica: ref}
	err := participant.ReceiveLoadRaft(t.Context(), load.RaftBatch{From: peer, To: peer, DeliveryID: 1,
		Messages: []raftpb.Message{{Type: raftpb.MsgProp, From: ref.ReplicaID, To: ref.ReplicaID}}})
	if !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("forwarded proposal bypassed private gate", err)
	}
	if _, err := participant.StartLoadGroup(t.Context(), ref); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	holdCtx, stopHold := context.WithTimeout(t.Context(), 25*time.Millisecond)
	holdErr := participant.StopLoadGroup(holdCtx, ref)
	stopHold()
	if !errors.Is(holdErr, fault.ErrShutdownTimeout) {
		t.Fatal("held recovery did not retain its checkpoint owner", holdErr)
	}
	held := participant.(*node).host.Held()
	if len(held) != 1 || held[0].GroupID != ref.GroupID || held[0].ReplicaID != ref.ReplicaID {
		t.Fatal("exact local hold lost", held)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	err = participant.Stop(ctx)
	cancel()
	if !errors.Is(err, fault.ErrShutdownTimeout) || closed.Load() != 0 {
		t.Fatal("checkpoint ownership was released early", err, closed.Load())
	}
	unblock()
	stopAuthorityNode(t, participant)
	if closed.Load() != 1 || runs.Load() != 0 {
		t.Fatal("late checkpoint launched Run or lost Close", closed.Load(), runs.Load())
	}
	current, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || current.Complete {
		t.Fatal("stopped recovery became Complete", current, err)
	}
}

func TestRecoveryReadinessSeesAllHandlesButRunWaitsForDurableComplete(t *testing.T) {
	t.Parallel()
	entered, release, run := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var calls, runs atomic.Int32
	hooks := recoveryReadinessHooks{readiness: func(ctx context.Context, in lifecycle.ReadinessInput) (lifecycle.ReadinessDecision, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
			return lifecycle.ReadinessDecision{State: lifecycle.Ready}, nil
		case <-ctx.Done():
			return lifecycle.ReadinessDecision{}, ctx.Err()
		}
	}}
	participant, facade, operation := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: ensureMachine{}, Run: func(context.Context, *load.Group) error { runs.Add(1); close(run); return nil }}, nil
	}, hooks)
	t.Cleanup(unblock)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitRecoverySignal(t, entered)
	current, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || current.Phase != recovery.Validating || current.Complete || participant.Status().Ready ||
		len(participant.LoadGroupIDs()) != 1 || runs.Load() != 0 {
		t.Fatal("validation barrier violated", current, participant.Status(), err, runs.Load())
	}
	handle, found := participant.LoadGroup(participant.LoadGroupIDs()[0])
	if !found {
		t.Fatal("validation handle missing")
	}
	unblock()
	if err := participant.WaitReady(recoveryBarrierContext(t)); err != nil {
		t.Fatal(err, participant.Status())
	}
	awaitRecoverySignal(t, run)
	current, err = facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || !current.Complete || runs.Load() != 1 {
		t.Fatal("Run was released before durable completion", current, err, runs.Load())
	}
	stopAuthorityNode(t, participant)
	if handle.Running() {
		t.Fatal("stopped handle redirected or remained running")
	}
}

func TestRecoveryLateFactorySuccessRemainsOwnedThroughStop(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var factories, closed, runs atomic.Int32
	participant, _, _ := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		close(entered)
		<-release
		return load.Application{StateMachine: ensureMachine{}, Close: func(context.Context) error { closed.Add(1); return nil },
			Run: func(context.Context, *load.Group) error { runs.Add(1); return nil }}, nil
	}, nil)
	t.Cleanup(unblock)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitRecoverySignal(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	err := participant.Stop(ctx)
	cancel()
	if !errors.Is(err, fault.ErrShutdownTimeout) || closed.Load() != 0 || factories.Load() != 1 {
		t.Fatal("late factory ownership lost", err, closed.Load(), factories.Load())
	}
	unblock()
	stopAuthorityNode(t, participant)
	if closed.Load() != 1 || runs.Load() != 0 || factories.Load() != 1 {
		t.Fatal("late success escaped owner", closed.Load(), runs.Load(), factories.Load())
	}
}

type recoveryReadinessHooks struct {
	lifecycle.Hooks
	readiness func(context.Context, lifecycle.ReadinessInput) (lifecycle.ReadinessDecision, error)
}

func (h recoveryReadinessHooks) Readiness(ctx context.Context, input lifecycle.ReadinessInput) (lifecycle.ReadinessDecision, error) {
	return h.readiness(ctx, input)
}

func TestRecoveryLongRestoreIgnoresReadinessWaiterDeadline(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var factories atomic.Int32
	participant, _, _ := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: recoveryBarrierMachine{restore: func(ctx context.Context, data []byte) error {
			close(entered)
			select {
			case <-release:
				return ensureMachine{}.Restore(ctx, data)
			case <-ctx.Done():
				return ctx.Err()
			}
		}}}, nil
	}, nil)
	t.Cleanup(unblock)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitRecoverySignal(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	err := participant.WaitReady(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiter deadline was not independent", err)
	}
	timer := time.NewTimer(5100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if factories.Load() != 1 || participant.Status().Ready || len(participant.LoadGroupIDs()) != 0 {
		t.Fatal("long restore duplicated or published", factories.Load(), participant.Status())
	}
	unblock()
	if err := participant.WaitReady(recoveryBarrierContext(t)); err != nil {
		t.Fatal("owned restore used the ordinary timeout", err, participant.Status())
	}
	if factories.Load() != 1 {
		t.Fatal("restore waiter created another factory", factories.Load())
	}
}

func TestRecoveryFailedValidationNeverCompletesOrReleasesRun(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	var once sync.Once
	var runs, closed atomic.Int32
	defaults, err := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hooks := recoveryReadinessHooks{Hooks: defaults, readiness: func(context.Context, lifecycle.ReadinessInput) (lifecycle.ReadinessDecision, error) {
		once.Do(func() { close(entered) })
		return lifecycle.ReadinessDecision{State: lifecycle.Failed, Reason: "application rejected recovered state"}, nil
	}}
	participant, facade, operation := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: ensureMachine{}, Run: func(context.Context, *load.Group) error { runs.Add(1); return nil },
			Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}, hooks)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitRecoverySignal(t, entered)
	stopAuthorityNode(t, participant)
	current, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || current.Phase != recovery.Validating || current.Complete || runs.Load() != 0 || closed.Load() != 1 {
		t.Fatal("failed validator released recovery", current, err, runs.Load(), closed.Load())
	}
}
