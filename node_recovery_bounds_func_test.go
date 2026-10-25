//go:build functional

package gquorum

import (
	"testing"

	"github.com/asalimonov/gquorum/recovery"
)

func TestRecoveryExactSourceGroupBoundAllowsDestinationMetadata(t *testing.T) {
	t.Parallel()
	source := recoveryFixtureOptions(t, true)
	handle, err := NewRecovery(source)
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
	handle, err = NewRecovery(source, recovery.Options{Resources: recovery.Resources{MaxGroupBytes: limit}})
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
	opts, err = RecoveryStartOptions(status, opts)
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewNode(opts)
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
