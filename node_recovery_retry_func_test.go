//go:build functional

package gquorum

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoveryruntime"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

type lostRecoveryConsumption struct {
	recoveryruntime.MetaPort
	calls atomic.Int32
}

func (m *lostRecoveryConsumption) RecordRecoveredGroup(ctx context.Context, p model.GroupConsumedProposal) error {
	err := m.MetaPort.RecordRecoveredGroup(ctx, p)
	if err == nil && m.calls.Add(1) == 1 {
		return errors.New("lost committed consumption reply")
	}
	return err
}

func TestRecoveryConfirmsLostConsumptionReplyWithoutAnotherFactory(t *testing.T) {
	t.Parallel()
	var factories atomic.Int32
	participant, facade, operation := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: ensureMachine{}}, nil
	}, nil)
	var port *lostRecoveryConsumption
	participant.(*node).recoveryMetaPort = func(meta recoveryruntime.MetaPort) recoveryruntime.MetaPort {
		port = &lostRecoveryConsumption{MetaPort: meta}
		return port
	}
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := participant.WaitReady(recoveryBarrierContext(t)); err != nil {
		t.Fatal(err, participant.Status())
	}
	if factories.Load() != 1 || port.calls.Load() != 1 {
		t.Fatal("lost reply repeated acquisition or mutation", factories.Load(), port.calls.Load())
	}
	stopAuthorityNode(t, participant)
	current, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || !current.Complete {
		t.Fatal(current, err)
	}
}

func retainedRecoveryNode(t *testing.T, previous Node, factory GroupFactory) Node {
	t.Helper()
	retained := previous.(*node).opts
	options := authorityNodeOptions(t, retained.StorageDir, "retained-B-restart")
	options.Identity.ClusterID = previous.Identity().ClusterID
	options.Identity.MemberID = previous.Identity().MemberID
	options.Bootstrap.Founders[0].MemberID = options.Identity.MemberID
	options.Node = retained.Node
	options.Loads = retained.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	options.Loads.Factory = factory
	next, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	transport.node = next
	t.Cleanup(func() { stopAuthorityNode(t, next) })
	return next
}

func TestRecoveryCompletedRestartUsesPortableBAndNeverRefillsMissingWAL(t *testing.T) {
	t.Parallel()
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "portable restart", true: "missing consumed WAL"}[missing], func(t *testing.T) {
			t.Parallel()
			participant, _, _ := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
				return load.Application{StateMachine: ensureMachine{}}, nil
			}, nil)
			if err := participant.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := participant.WaitReady(recoveryBarrierContext(t)); err != nil {
				t.Fatal(err, participant.Status())
			}
			stopAuthorityNode(t, participant)
			root := participant.(*node).opts.Loads.Storage.WAL.RootDir
			if missing {
				if err := os.RemoveAll(filepath.Join(root, "work")); err != nil {
					t.Fatal(err)
				}
			}
			var factories atomic.Int32
			restarted := retainedRecoveryNode(t, participant, func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
				factories.Add(1)
				if spec.Recovery != nil {
					return load.Application{}, errors.New("completed restart reused A input")
				}
				return load.Application{StateMachine: ensureMachine{}}, nil
			})
			if err := restarted.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !missing {
				if err := restarted.WaitReady(recoveryBarrierContext(t)); err != nil {
					t.Fatal(err, restarted.Status())
				}
				if len(restarted.LoadGroupIDs()) == 0 {
					ctx := recoveryBarrierContext(t)
					observations, err := restarted.Watch(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for len(restarted.LoadGroupIDs()) == 0 {
						select {
						case <-observations:
						case <-ctx.Done():
							t.Fatal("ordinary load restart did not publish", restarted.Status())
						}
					}
				}
				if factories.Load() != 1 {
					t.Fatal(factories.Load())
				}
				return
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			observations, err := restarted.Watch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for len(restarted.ParkedLoads()) == 0 {
				select {
				case <-observations:
				case <-ctx.Done():
					t.Fatal("missing B was not parked", restarted.Status())
				}
			}
			if factories.Load() != 0 || restarted.Status().Ready {
				t.Fatal("missing consumed B was refilled", factories.Load(), restarted.Status())
			}
			if _, err := os.Stat(filepath.Join(root, "work")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("missing B path recreated", err)
			}
		})
	}
}

func TestRecoveryFactoryFailureResumesOneOrdinaryRetryWithOriginalSeed(t *testing.T) {
	t.Parallel()
	var factories atomic.Int32
	participant, facade, operation := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{}, errors.New("factory failure after protected seed installation")
	}, nil)
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	observations, err := participant.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := participant.(*node)
	for {
		n.mu.RLock()
		coordinator := n.recoveryCoordinator
		n.mu.RUnlock()
		if coordinator != nil && coordinator.Error() != nil {
			break
		}
		select {
		case <-observations:
		case <-ctx.Done():
			t.Fatal("failed factory did not produce retained failure", participant.Status())
		}
	}
	stopAuthorityNode(t, participant)
	if factories.Load() != 1 {
		t.Fatal("failed factory automatically looped", factories.Load())
	}
	current, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Complete {
		t.Fatal("failed factory completed recovery")
	}
	restarted := retainedRecoveryNode(t, participant, func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		if spec.Recovery == nil {
			return load.Application{}, errors.New("unconsumed retry lost recovery input")
		}
		return load.Application{StateMachine: ensureMachine{}}, nil
	})
	if err := restarted.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.WaitReady(recoveryBarrierContext(t)); err != nil {
		r := restarted.(*node)
		r.mu.RLock()
		control := r.recoveryCoordinator
		r.mu.RUnlock()
		var failure error
		if control != nil {
			failure = control.Error()
		}
		inspect, done := context.WithTimeout(context.Background(), 2*time.Second)
		operation, readErr := restarted.Cluster().Operation(inspect, topology.MakeOperationID(2, 2))
		done()
		t.Logf("retry details: %+v; read=%v; factories=%d", operation, readErr, factories.Load())
		t.Fatal("ordinary retry did not resume original seed", err, restarted.Status(), failure)
	}
	if factories.Load() != 2 {
		t.Fatal("retry created overlapping acquisitions", factories.Load())
	}
	stopAuthorityNode(t, restarted)
	current, err = facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || !current.Complete {
		t.Fatal(current, err)
	}
}
