//go:build functional

package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRecoveryNodeReplaysAndPublishesLoadBeforeDeferredRun(t *testing.T) {
	t.Parallel()
	source := recoveryFixtureOptions(t, true)
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := facade.Run(t.Context(), operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	options := authorityNodeOptions(t, source.StorageDir, "recovered-load")
	options.Node = source.Bootstrap.Founders[0].Node
	options.Loads = source.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	var factories, runs, closed atomic.Int32
	runFailure := make(chan error, 1)
	runDone := make(chan struct{})
	options.Loads.Factory = func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		if spec.Recovery == nil || spec.Recovery.RecoveryID != operation.RecoveryID ||
			spec.Recovery.Source.ClusterID == spec.Identity.ClusterID {
			return load.Application{}, fmt.Errorf("missing exact recovery factory input")
		}
		return load.Application{StateMachine: ensureMachine{}, Run: func(ctx context.Context, _ *load.Group) error {
			current, err := facade.Status(ctx, operation.RecoveryID)
			if err != nil || current.Phase != recovery.Complete {
				runFailure <- fmt.Errorf("Run preceded durable completion: %v %v", current.Phase, err)
			}
			runs.Add(1)
			close(runDone)
			return nil
		}, Close: func(context.Context) error { closed.Add(1); return nil }}, nil
	}
	options, err = quorum.RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := newNode(options)
	if err != nil {
		t.Fatal(err)
	}
	transport.node = participant
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal("recovery did not complete", err, participant.Status())
	}
	if factories.Load() != 1 || len(participant.LoadGroupIDs()) != 1 {
		t.Fatal("recovered data missing", factories.Load(), participant.LoadGroupIDs())
	}
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatal("Run was not released", ctx.Err())
	}
	if runs.Load() != 1 {
		t.Fatal("Run launched more than once", runs.Load())
	}
	select {
	case err := <-runFailure:
		t.Fatal(err)
	default:
	}
	stopAuthorityNode(t, participant)
	if closed.Load() != 1 {
		t.Fatal("application ownership was lost", closed.Load())
	}
}

func newRecoveryLoadNode(t *testing.T, factory quorum.GroupFactory, hooks lifecycle.Hooks) (quorum.Node, *recoveryFacade, recovery.Operation) {
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
	options, err = quorum.RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := newNode(options)
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
	if _, err := participant.StartLoadGroup(t.Context(), ref); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("held private replica became public", err)
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

func TestRecoveryTransferCannotPoisonCreationBeforeConsumption(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	participant, facade, operation := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: recoveryBarrierMachine{snapshot: func(context.Context) ([]byte, error) {
			close(entered)
			<-release
			return ensureMachine{}.Snapshot(context.Background())
		}}}, nil
	}, nil)
	t.Cleanup(unblock)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitRecoverySignal(t, entered)
	// The creation operation has succeeded; this is the former floor-poisoning
	// interval between actual creation and metadata consumption.
	identity := participant.Identity()
	if err := participant.Cluster().TransferLeader(t.Context(), "work", identity.NodeName); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("public transfer crossed pending import gate", err)
	}
	before, err := participant.Cluster().ReadTopology(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	refuse := func(err error) {
		t.Helper()
		if !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("pending import allowed mutation", err)
		}
	}
	_, err = participant.Cluster().Stage(t.Context(), before.Latest, before.Latest.Version)
	refuse(err)
	_, err = participant.Cluster().Approve(t.Context(), before.Latest.Version)
	refuse(err)
	_, err = participant.Cluster().AddNode(t.Context(), topology.Node{Name: "n2", Type: "worker"}, quorum.MutationOptions{Preview: true})
	refuse(err)
	refuse(participant.SetSchedulingEnabled(t.Context(), true))
	if participant.Status().Ready {
		t.Fatal("Node ready while consumption held")
	}
	after, err := participant.Cluster().ReadTopology(t.Context())
	if err != nil || after.Latest.Version != before.Latest.Version || after.Target.Version != before.Target.Version {
		t.Fatal("gate allowed intervening intent", after, err)
	}
	unblock()
	if err := participant.WaitReady(recoveryBarrierContext(t)); err != nil {
		t.Fatal(err, participant.Status())
	}
	status, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || !status.Complete {
		t.Fatal("refused transfer stranded consumption", status, err)
	}
}

func twoGroupRecoverySource(t *testing.T) quorum.NodeOptions {
	t.Helper()
	retained := recoveryFixtureOptions(t, true)
	options := authorityNodeOptions(t, retained.StorageDir, "second-source-group")
	options.Loads = retained.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	participant, err := newNode(options)
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
	created, err := participant.Cluster().CreateGroup(ctx, singleReplicaSpec("jobs", nil), quorum.MutationOptions{
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
	options, err = quorum.RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := newNode(options)
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
	for {
		current, readErr := facade.Status(ctx, operation.RecoveryID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if current.Issue != nil {
			if !strings.Contains(current.Issue.Message, failure.Error()) {
				t.Fatal(current.Issue)
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
