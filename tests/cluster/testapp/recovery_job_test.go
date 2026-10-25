//go:build !functional

package main

import (
	"testing"

	"github.com/asalimonov/gquorum/recovery"
)

func TestRecoveryJobRejectsInconsistentSavedIntent(t *testing.T) {
	t.Parallel()
	base := recoveryJob{ID: "job", SourceCluster: "A", Maintenance: true, Phase: "preparing",
		Request: recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "permanent loss"}}
	if err := validateRecoveryJob(base); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*recoveryJob){
		func(j *recoveryJob) { j.Maintenance = false }, func(j *recoveryJob) { j.Phase = "unknown" },
		func(j *recoveryJob) { j.Acceptance = &recovery.Acceptance{PlanDigest: "missing"} },
		func(j *recoveryJob) { j.Operation = &recovery.Operation{RecoveryID: "wrong"} },
	} {
		copy := base
		change(&copy)
		if err := validateRecoveryJob(copy); err == nil {
			t.Fatal("accepted inconsistent job", copy)
		}
	}
}
func TestRecoveryPrepareRetryKeepsEffectiveIdentity(t *testing.T) {
	t.Parallel()
	request := recovery.Request{Mode: recovery.Rebuild, Actor: "operator", Comment: "loss"}
	effective := request
	effective.RecoveryID = "generated"
	effective.ClusterID = "B"
	if !sameRecoveryRequest(effective, request) {
		t.Fatal("lost prepare reply cannot retry")
	}
	changed := request
	changed.ClusterID = "C"
	if sameRecoveryRequest(effective, changed) {
		t.Fatal("accepted different lineage")
	}
	original := &recoveryJob{Request: effective, Plan: &recovery.Plan{Digest: "digest"}}
	copy := cloneRecoveryJob(original)
	copy.Plan.Digest = "changed"
	if original.Plan.Digest != "digest" || original.Request.RecoveryID != "generated" {
		t.Fatal("mutated request")
	}
}
