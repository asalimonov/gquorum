//go:build functional

package gquorum

import (
	"context"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoveryruntime"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryNodeMetadataOnlyFoundingImportAndCompletion(t *testing.T) {
	t.Parallel()
	for _, gate := range []lifecycle.StartupGate{lifecycle.LocalAdmission, lifecycle.AllFounders} {
		t.Run(string(gate), func(t *testing.T) { t.Parallel(); recoveryMetadataOnly(t, gate) })
	}
}

func recoveryMetadataOnly(t *testing.T, gate lifecycle.StartupGate) {
	t.Helper()
	source := recoveryFixtureOptions(t, false)
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	options := authorityNodeOptions(t, source.StorageDir, "recovered-process")
	options.Node = source.Bootstrap.Founders[0].Node
	options.Lifecycle.Default.Startup = gate
	options, err = RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal(err, participant.Status())
	}
	completed, err := facade.Status(ctx, operation.RecoveryID)
	if err != nil || completed.Phase != recovery.Complete {
		t.Fatal(completed, err)
	}
	owner := participant.(*node)
	owner.mu.RLock()
	detached := owner.recoveryManifest == nil && owner.recoveryCoordinator == nil
	owner.mu.RUnlock()
	if !detached {
		t.Fatal("completed recovery retained an identity gate over later readmission")
	}
	actual := participant.Identity()
	if actual.ClusterID != status.Identity.ClusterID || actual.MemberID != status.Identity.MemberID || actual.InstanceID != "recovered-process" {
		t.Fatal("recovered founder retained old protocol identity", actual)
	}
	observed, err := participant.Cluster().ReadTopology(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Target.Version != 2 || observed.Target.Origin != topology.OriginOperator {
		t.Fatal(observed)
	}
	stopAuthorityNode(t, participant)
	// Completed history is evidence, never a second recovery/founding permission.
	restart := authorityNodeOptions(t, source.StorageDir, "recovered-restart")
	restart.Identity.ClusterID = status.Identity.ClusterID
	restart.Identity.MemberID = actual.MemberID
	restart.Bootstrap.Founders[0].MemberID = actual.MemberID
	restarted, err := NewNode(restart)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, restarted) })
	if err := restarted.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if restarted.Identity().MemberID != actual.MemberID {
		t.Fatal("retained B was reseeded")
	}
}

type heldRecoveryMeta struct {
	recoveryruntime.MetaPort
	entered     chan struct{}
	release     <-chan struct{}
	imports     atomic.Int32
	enteredOnce sync.Once
}

func (m *heldRecoveryMeta) WaitRecoveryReplay(ctx context.Context) error {
	if err := m.MetaPort.WaitRecoveryReplay(ctx); err != nil {
		return err
	}
	m.enteredOnce.Do(func() { close(m.entered) })
	select {
	case <-m.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *heldRecoveryMeta) ImportRecovery(ctx context.Context, proposal model.ImportProposal) (topology.OperationID, error) {
	id, err := m.MetaPort.ImportRecovery(ctx, proposal)
	m.imports.Add(1)
	if err == nil {
		return "", errors.New("lost committed import reply")
	}
	return id, err
}

func TestRecoveryNodeClosesPublicGateBeforeImportAndResolvesLostReply(t *testing.T) {
	t.Parallel()
	source := recoveryFixtureOptions(t, false)
	facade, operation := acceptedRecovery(t, source, nil)
	status, err := executeRecovery(t.Context(), facade, operation.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	options := authorityNodeOptions(t, source.StorageDir, "held-recovery")
	options.Node = source.Bootstrap.Founders[0].Node
	options, err = RecoveryStartOptions(status, options)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, participant) })
	release, entered := make(chan struct{}), make(chan struct{})
	var port *heldRecoveryMeta
	participant.(*node).recoveryMetaPort = func(actual recoveryruntime.MetaPort) recoveryruntime.MetaPort {
		port = &heldRecoveryMeta{MetaPort: actual, entered: entered, release: release}
		return port
	}
	if err := participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	before, err := participant.Cluster().ReadTopology(ctx)
	if err != nil || before.Target.Version != 1 {
		t.Fatal(before, err)
	}
	refusal := func(err error) {
		t.Helper()
		if !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal(err)
		}
	}
	_, err = participant.Cluster().Stage(ctx, before.Latest, before.Latest.Version)
	refusal(err)
	_, err = participant.Cluster().Approve(ctx, 1)
	refusal(err)
	_, err = participant.Cluster().AddNode(ctx, topology.Node{Name: "n2", Type: "worker"}, MutationOptions{Preview: true})
	refusal(err)
	refusal(participant.SetSchedulingEnabled(ctx, true))
	_, err = participant.StartLoadGroup(ctx, load.ReplicaRef{GroupID: "unapproved", ReplicaID: 1})
	refusal(err)
	if participant.Status().Ready {
		t.Fatal("Node became ready while import was held")
	}
	observed, err := facade.Status(ctx, operation.RecoveryID)
	if err != nil || observed.Phase != recovery.AwaitingStart {
		t.Fatal(observed, err)
	}
	after, err := participant.Cluster().ReadTopology(ctx)
	if err != nil || after.Target.Version != 1 || after.Latest.Version != 1 {
		t.Fatal("gate allowed intervening intent", after, err)
	}
	close(release)
	if err := participant.WaitReady(ctx); err != nil {
		t.Fatal(err, participant.Status())
	}
	if port.imports.Load() != 1 {
		t.Fatal("uncertain import created another proposal", port.imports.Load())
	}
	observed, err = facade.Status(ctx, operation.RecoveryID)
	if err != nil || observed.Phase != recovery.Complete {
		t.Fatal(observed, err)
	}
}
