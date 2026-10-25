//go:build functional

package gquorum

import (
	"context"
	"testing"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
)

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
	source := first.(*node).opts
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
	opts, err = RecoveryStartOptions(status, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewNode(opts)
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
