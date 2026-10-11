package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestProjectionRetirementVerifiesDurableIdentityAndRetries(t *testing.T) {
	t.Parallel()
	a := &application{cfg: config{NodeName: "node1", ClusterID: "cluster", DataDir: t.TempDir()}, clusterHooks: newClusterHooks()}
	key := walKey{"work", 7}
	id := a.projectionIdentity(key)
	authorization := topology.AuthorizedStep{OperatorID: "exec-v3-a1", Version: 3, Attempt: 1, StepSeq: 1, Kind: topology.StepCleanupReplicaState, GroupID: "work", NodeName: "node1", ReplicaID: 7}
	request := load.RetirementRequest{Identity: id, Reason: load.RetirementTopology, Authorization: &authorization}
	foreign := id
	foreign.ReplicaID++
	raw, err := json.Marshal(projectionFile{Identity: foreign, State: newMachine().state})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeProjection(t.Context(), a.projectionPath(key), raw); err != nil {
		t.Fatal(err)
	}
	if result, err := a.retireReplica(t.Context(), request); !errors.Is(err, fault.ErrIdentityMismatch) || result.Verified {
		t.Fatal("foreign projection retired", result, err)
	}
	if _, err := os.Stat(a.projectionPath(key)); err != nil {
		t.Fatal("foreign data removed", err)
	}
	raw, err = json.Marshal(projectionFile{Identity: id, State: newMachine().state})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeProjection(t.Context(), a.projectionPath(key), raw); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.retireReplica(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for range 2 {
		result, err := a.retireReplica(t.Context(), request)
		if err != nil || !result.Verified || result.Identity != request.Identity || result.Reason != request.Reason || result.Authorization == nil || *result.Authorization != authorization || result.AdmissionID != "" {
			t.Fatal(result, err)
		}
	}
	if _, err := os.Stat(a.projectionPath(key)); !os.IsNotExist(err) {
		t.Fatal("projection retained after verified retirement", err)
	}
}
