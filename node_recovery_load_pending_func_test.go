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

// Task12 replaces the deliberate intermediate refusal with exact seeded replay.
func TestRecoveryNodePendingLoadCannotFoundEmptyOrRecreateMissingBMetadata(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	observations, err := participant.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case observation := <-observations:
			if observation.Status.ErrorCode != fault.Unsupported {
				continue
			}
		case <-ctx.Done():
			t.Fatal("recovery did not report its pending capability", participant.Status(), ctx.Err())
		}
		break
	}
	if participant.Status().Ready || factories.Load() != 0 || len(participant.LoadGroupIDs()) != 0 {
		t.Fatal("partial implementation published an empty recovered application")
	}
	stopAuthorityNode(t, participant)
	current, err := facade.Status(t.Context(), operation.RecoveryID)
	if err != nil || current.Phase != recovery.Importing {
		t.Fatal(current, err)
	}
	for _, name := range []string{metaDBFilename, authorityDBFilename} {
		if err := os.Remove(filepath.Join(source.StorageDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	retry := authorityNodeOptions(t, source.StorageDir, "missing-B")
	retry.Node, retry.Loads = options.Node, options.Loads
	retry, err = RecoveryStartOptions(current, retry)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewNode(retry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, restarted) })
	if err := restarted.Start(t.Context()); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("established B was silently refounded", err)
	}
	stopAuthorityNode(t, restarted)
	if factories.Load() != 0 {
		t.Fatal("missing B identity replayed source application")
	}
	for _, name := range []string{metaDBFilename, authorityDBFilename} {
		if _, err := os.Stat(filepath.Join(source.StorageDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
}
