package storagetest

import (
	"errors"
	"io/fs"
	"testing"

	"github.com/asalimonov/gquorum/storage"
)

const retirementReplicaID = 7

// RunRetirement checks the optional exact collection and proof-release contract.
// Persistent adapters additionally need restart and partial-deletion tests.
func RunRetirement(t *testing.T, open Opener) {
	t.Helper()
	s := open(t, "a")
	retirement, ok := s.(storage.RetirementStore)
	if !ok {
		t.Fatal("adapter does not implement RetirementStore")
	}
	id := storage.Identity{ClusterID: "test-cluster", NodeName: "a", GroupID: "group", ReplicaID: retirementReplicaID}
	must(t, s.BindReplica(t.Context(), id))
	must(t, s.Ensure(t.Context(), id.GroupID))
	_, err := s.Quarantine(t.Context(), id.GroupID, id.ReplicaID)
	must(t, err)
	rows, err := retirement.Retired(t.Context())
	must(t, err)
	if len(rows) != 1 || rows[0].Identity != id {
		t.Fatal("missing exact retirement", rows)
	}
	record := rows[0]
	if err := retirement.ForgetCollection(t.Context(), record); err == nil {
		t.Fatal("proof released while retired data remained")
	}
	successor := id
	successor.ReplicaID++
	must(t, s.BindReplica(t.Context(), successor))
	must(t, s.Ensure(t.Context(), id.GroupID))
	for range 2 {
		wrong := record
		wrong.Identity.ReplicaID++
		if err := retirement.Collect(t.Context(), wrong); err == nil {
			t.Fatal("foreign identity collected")
		}
		must(t, retirement.Collect(t.Context(), record))
	}
	rows, err = retirement.Retired(t.Context())
	must(t, err)
	if len(rows) != 0 {
		t.Fatal("collected proof reappeared as retired data", rows)
	}
	verifyCollectionRelease(t, retirement, record)
	probe, err := s.Probe(t.Context(), id.GroupID)
	if err != nil || !probe.Exists || probe.Identity != successor {
		t.Fatal("collection/release changed successor", probe, err)
	}
}

func verifyCollectionRelease(t *testing.T, retirement storage.RetirementStore, record storage.RetiredReplica) {
	t.Helper()
	wrong := record
	wrong.Identity.ReplicaID++
	if err := retirement.ForgetCollection(t.Context(), wrong); err == nil {
		t.Fatal("foreign identity released collection proof")
	}
	for range 2 {
		must(t, retirement.ForgetCollection(t.Context(), record))
	}
	if err := retirement.Collect(t.Context(), record); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("absence without retained proof claimed collection", err)
	}
}
