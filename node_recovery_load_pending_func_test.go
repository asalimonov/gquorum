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

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
)

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
	restarted, err := NewNode(retry)
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
