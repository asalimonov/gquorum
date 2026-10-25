//go:build functional

package gquorum

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
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
	options, err = RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	var traceMu sync.Mutex
	trace := map[metaengine.ProposalKind]error{}
	participant.(*node).recoveryCommandObserver = func(kind metaengine.ProposalKind, err error) { traceMu.Lock(); trace[kind] = err; traceMu.Unlock() }
	transport.node = participant
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	if err := participant.WaitReady(ctx); err != nil {
		state := participant.Status()
		n := participant.(*node)
		var control error
		if n.recoveryCoordinator != nil {
			control = n.recoveryCoordinator.Error()
		}
		t.Fatal("recovery did not complete", err, state, control)
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
	traceMu.Lock()
	defer traceMu.Unlock()
	for _, kind := range []metaengine.ProposalKind{metaengine.KindBeginEffect, metaengine.KindRecordFence,
		metaengine.KindRecordEffect, metaengine.KindTransitionOperation, metaengine.KindRecoveryGroupConsumed,
		metaengine.KindCompleteRecoveryImport} {
		err, found := trace[kind]
		if !found || err != nil {
			t.Fatal("actual committed recovery command was missing or refused", kind, found, err, trace)
		}
	}
	if closed.Load() != 1 {
		t.Fatal("application ownership was lost", closed.Load())
	}
}
