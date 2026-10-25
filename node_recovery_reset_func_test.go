//go:build functional

package gquorum

import (
	"context"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
)

func TestRecoveryResetStartupUsesOwnedArchiveHistoryBeforeFreshJoin(t *testing.T) {
	t.Parallel()
	source := recoveryFixtureOptions(t, true)
	facade, err := NewRecovery(source)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.Inspect(t.Context(), recovery.Request{Mode: recovery.ResetForJoin, ClusterID: "named-recovered",
		Actor: "operator", Comment: "explicit reset for admission"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := facade.Approve(t.Context(), plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if err != nil || status.Phase != recovery.JoinReady {
		t.Fatal(status, err)
	}
	backup := filepath.Join(source.StorageDir, recoverydisk.Namespace, operation.BackupName, "backup")
	original := sourceFileHashes(t, backup)
	var factories atomic.Int32
	for _, instance := range []string{"first-join", "interrupted-join-retry"} {
		options := authorityNodeOptions(t, source.StorageDir, instance)
		options.Node = source.Bootstrap.Founders[0].Node
		options.Loads = source.Loads
		options.Loads.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
			factories.Add(1)
			return load.Application{StateMachine: ensureMachine{}}, nil
		}
		options.Bootstrap.JoinSeeds = []string{"127.0.0.1:1"}
		options, err = RecoveryStartOptions(status, options)
		if err != nil {
			t.Fatal(err)
		}
		participant, err := NewNode(options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stopAuthorityNode(t, participant) })
		// No second participant: unreachable seed keeps this in the ordinary join path.
		if err := participant.Start(t.Context()); err != nil {
			t.Fatal("own reset archive counted as live WAL", err)
		}
		if participant.Status().Ready || participant.Identity().ClusterID != "named-recovered" {
			t.Fatal(participant.Status())
		}
		stopAuthorityNode(t, participant)
		after, err := facade.Status(t.Context(), operation.RecoveryID)
		if err != nil || after.Phase != recovery.JoinReady {
			t.Fatal("interrupted admission was re-archived", after, err)
		}
		if !reflect.DeepEqual(original, sourceFileHashes(t, backup)) {
			t.Fatal("interrupted reset altered A backup")
		}
	}
	if factories.Load() != 0 {
		t.Fatal("reset replayed an A application", factories.Load())
	}
}
