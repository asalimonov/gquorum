//go:build !functional

package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestBaselineProposalsRejectedAndResultBytesPreserved(t *testing.T) {
	t.Parallel()
	var proposals []json.RawMessage
	if err := json.Unmarshal(baselineBytes(t, "proposals.json"), &proposals); err != nil {
		t.Fatal(err)
	}
	if len(proposals) != 12 {
		t.Fatalf("incomplete baseline: %d commands", len(proposals))
	}
	for _, raw := range proposals {
		if _, err := DecodeProposal(raw); !errors.Is(err, fault.ErrUnsupported) {
			t.Fatal("legacy proposal accepted", err)
		}
	}
	raw := baselineBytes(t, "apply-result.json")
	var result ApplyResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || result.OperatorID != "exec-v1-a1" {
		t.Fatalf("lost approval identity: %+v", result)
	}
	got, err := json.Marshal(result)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("apply reply encoding changed:\n%s\n%s\n%v", raw, got, err)
	}
}

func TestBaselineSnapshotIsRejectedWithoutMutation(t *testing.T) {
	t.Parallel()
	m := NewMachine(topology.Rules{SchemaVersion: 1, Limits: topology.Limits{}.WithDefaults()}, nil)
	if err := m.Restore(context.Background(), baselineBytes(t, "snapshot.json")); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	latest, target, active := m.Views()
	if latest.Version != 0 || target.Version != 0 || active.Version != 0 {
		t.Fatal("rejected old snapshot mutated state")
	}
}
