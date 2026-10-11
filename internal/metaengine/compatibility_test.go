package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func baselineBytes(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "api-baseline", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestBaselineProposalAndResultBytes(t *testing.T) {
	t.Parallel()
	var proposals []json.RawMessage
	if err := json.Unmarshal(baselineBytes(t, "proposals.json"), &proposals); err != nil {
		t.Fatal(err)
	}
	if len(proposals) != 12 {
		t.Fatalf("incomplete baseline: %d commands", len(proposals))
	}
	for _, raw := range proposals {
		p, err := DecodeProposal(raw)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Encode(reflect.ValueOf(p).Elem().Interface())
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("proposal encoding changed:\n%s\n%s\n%v", raw, got, err)
		}
	}
	raw := baselineBytes(t, "apply-result.json")
	var result ApplyResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || result.ExecutionID != "exec-v1-a1" {
		t.Fatalf("lost approval identity: %+v", result)
	}
	got, err := json.Marshal(result)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("apply reply encoding changed:\n%s\n%s\n%v", raw, got, err)
	}
}

func TestBaselineSnapshotRestoresCompleteState(t *testing.T) {
	t.Parallel()
	raw := baselineBytes(t, "snapshot.json")
	m := NewMachine(topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()}, nil)
	if err := m.Restore(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	latest, target, active := m.Views()
	if latest.Version != 1 || target.Version != 1 || active.Version != 1 || len(active.Nodes) != 3 || active.ClusterID != "compat-cluster" {
		t.Fatal("lost document state", latest, target, active)
	}
	ex, ok := m.CurrentExecution()
	if !ok || ex.Status.ID != "exec-v1-a1" || ex.Status.State != topology.ExecutionSucceeded {
		t.Fatalf("lost operator: %+v", ex)
	}
	a, ok := m.Admission("n1", "process-1")
	if !ok || a.MemberID != 1 || a.State != AdmissionCompleted {
		t.Fatalf("lost admission: %+v", a)
	}
	s, ok := m.LockSession("node/n1")
	if !ok || s.SessionID != "session-1" || s.InstanceID != "process-1" || s.TTLSeconds != 30 {
		t.Fatalf("lost session/lock: %+v", s)
	}
	got, err := m.Snapshot(context.Background())
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("snapshot encoding changed:\n%s\n%s\n%v", raw, got, err)
	}
}
