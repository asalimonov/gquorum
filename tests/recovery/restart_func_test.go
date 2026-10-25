//go:build functional

package recovery_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func retainedRecoveryNode(t *testing.T, previous quorum.Node, factory quorum.GroupFactory) quorum.Node {
	t.Helper()
	retained := previous.(*fixtureNode).options
	options := authorityNodeOptions(t, retained.StorageDir, "retained-B-restart")
	options.Identity.ClusterID = previous.Identity().ClusterID
	options.Identity.MemberID = previous.Identity().MemberID
	options.Bootstrap.Founders[0].MemberID = options.Identity.MemberID
	options.Node = retained.Node
	options.Loads = retained.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	options.Loads.Factory = factory
	next, err := newNode(options)
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
			root := participant.(*fixtureNode).options.Loads.Storage.WAL.RootDir
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
			parked := restarted.ParkedLoads()
			if len(parked) != 1 || parked["work"].Identity.GroupID != "work" || parked["work"].Identity.ClusterID != restarted.Identity().ClusterID || parked["work"].Reason != quorum.ParkStoreMissing {
				t.Fatal("missing exact parked B evidence", parked)
			}
			if factories.Load() != 0 || len(restarted.LoadGroupIDs()) != 0 {
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
	for {
		current, readErr := facade.Status(ctx, operation.RecoveryID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if current.Issue != nil {
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
		inspect, done := context.WithTimeout(context.Background(), 2*time.Second)
		operation, readErr := restarted.Cluster().Operation(inspect, topology.MakeOperationID(2, 2))
		done()
		t.Logf("retry details: %+v; read=%v; factories=%d", operation, readErr, factories.Load())
		t.Fatal("ordinary retry did not resume original seed", err, restarted.Status())
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

func TestRecoveryCompletedLineageCanRecoverAgainWithoutArchivingHistory(t *testing.T) {
	t.Parallel()
	first, oldFacade, oldOperation := newRecoveryLoadNode(t, func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: ensureMachine{}}, nil
	}, nil)
	if err := first.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.WaitReady(recoveryBarrierContext(t)); err != nil {
		t.Fatal(err, first.Status())
	}
	firstIdentity := first.Identity()
	stopAuthorityNode(t, first)
	source := first.(*fixtureNode).options
	source.Identity = firstIdentity
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := facade.Run(t.Context(), operation.RecoveryID)
	if err != nil {
		t.Fatal("completed history blocked a later recovery", err)
	}
	opts := authorityNodeOptions(t, source.StorageDir, "second-recovery")
	opts.Node, opts.Loads = source.Node, source.Loads
	transport := &loopbackTransport{}
	opts.Loads.Transport = transport
	opts, err = quorum.RecoveryStartOptions(status, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	transport.node = second
	t.Cleanup(func() { stopAuthorityNode(t, second) })
	if err := second.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := second.WaitReady(recoveryBarrierContext(t)); err != nil {
		t.Fatal(err, second.Status())
	}
	identity := second.Identity()
	if identity.ClusterID == firstIdentity.ClusterID || identity.MemberID <= firstIdentity.MemberID || len(second.LoadGroupIDs()) != 1 {
		t.Fatal("second recovery reused lineage or lost load", identity, firstIdentity, second.Status())
	}
	history, err := oldFacade.Status(t.Context(), oldOperation.RecoveryID)
	if err != nil || history.Phase != recovery.Complete || history.Identity.ClusterID != firstIdentity.ClusterID {
		t.Fatal("second recovery altered first operation", history, err)
	}
	rows, err := facade.List(t.Context())
	if err != nil || len(rows) != 2 || rows[0].RecoveryID != operation.RecoveryID {
		t.Fatal("second recovery lost history", rows, err)
	}
}

func TestRecoveryExactSourceGroupBoundAllowsDestinationMetadata(t *testing.T) {
	t.Parallel()
	source := recoveryFixtureOptions(t, true)
	handle, err := newRecovery(source)
	if err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "exact input bound"}
	report, err := handle.Inspect(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	var limit uint64
	for _, group := range report.Groups {
		if group.GroupID == "work" {
			limit = group.Bytes
		}
	}
	if limit == 0 {
		t.Fatal("missing measured source group")
	}
	handle, err = newRecovery(source, recovery.Options{Resources: recovery.Resources{MaxGroupBytes: limit}})
	if err != nil {
		t.Fatal(err)
	}
	report, err = handle.Inspect(t.Context(), report.Request)
	if err != nil {
		t.Fatal("exact source was refused", err)
	}
	operation, err := handle.Approve(t.Context(), report.Request, recovery.Acceptance{PlanDigest: report.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := handle.Run(t.Context(), operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	opts := authorityNodeOptions(t, source.StorageDir, "exact-bound-B")
	opts.Node = source.Bootstrap.Founders[0].Node
	opts.Loads = source.Loads
	opts, err = quorum.RecoveryStartOptions(status, opts)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := newNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.Loads.Transport.(*loopbackTransport).node = participant
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := participant.WaitReady(recoveryBarrierContext(t)); err != nil {
		t.Fatal("source bound constrained fresh destination metadata", err, participant.Status())
	}
}

func TestRecoveryNodeCannotRecreateMissingCompletedBMetadata(t *testing.T) {
	t.Parallel()
	source := recoveryFixtureOptions(t, true)
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	options := authorityNodeOptions(t, source.StorageDir, "pending-load")
	options.Node = source.Bootstrap.Founders[0].Node
	options.Loads = source.Loads
	transport := &loopbackTransport{}
	options.Loads.Transport = transport
	var factories atomic.Int32
	options.Loads.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: ensureMachine{}}, nil
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
		t.Fatal("recovered load did not become ready", err, participant.Status())
	}
	if factories.Load() != 1 || len(participant.LoadGroupIDs()) != 1 {
		t.Fatal("load was not reconstructed", factories.Load())
	}
	stopAuthorityNode(t, participant)
	current, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || current.Phase != recovery.Complete {
		t.Fatal(current, err)
	}
	for _, name := range []string{metaDBFilename, authorityDBFilename} {
		if err := os.Remove(filepath.Join(source.StorageDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	retry := authorityNodeOptions(t, source.StorageDir, "missing-B")
	retry.Node, retry.Loads = options.Node, options.Loads
	retry.Identity.ClusterID = current.Identity.ClusterID
	retry.Identity.MemberID = current.Identity.MemberID
	retry.Bootstrap.Founders[0].MemberID = current.Identity.MemberID
	restarted, err := newNode(retry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, restarted) })
	if err := restarted.Start(t.Context()); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("established B was silently refounded", err)
	}
	stopAuthorityNode(t, restarted)
	if factories.Load() != 1 {
		t.Fatal("missing B identity replayed source application")
	}
	for _, name := range []string{metaDBFilename, authorityDBFilename} {
		if _, err := os.Stat(filepath.Join(source.StorageDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
}
