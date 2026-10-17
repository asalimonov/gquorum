//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestFrozenScopedSnapshotRetainsCompleteEvidence(t *testing.T) {
	t.Parallel()
	raw := currentFormatBytes(t, "snapshot.json")
	m := mustMachine(t, testRules(), nil)
	if err := m.Restore(t.Context(), raw); err != nil {
		t.Fatal(err)
	}
	encoded, err := m.Snapshot(t.Context())
	if err != nil || !bytes.Equal(encoded, bytes.TrimSpace(raw)) {
		t.Fatal("schema-4 restore lost retained proof", err)
	}
	if index, term := m.AppliedState(); index != 30 || term != 4 {
		t.Fatal("lost applied boundary", index, term)
	}
	page, err := m.Progress(topology.ProgressQuery{})
	if err != nil || page.LatestVersion != 6 || page.TargetVersion != 5 || page.FullyConverged.Version != 1 || len(page.Cleanup) == 0 {
		t.Fatal("lost independent progress or cleanup", page, err)
	}
}

func TestFrozenCollectionCommandAndAcceptanceKeepExactProof(t *testing.T) {
	t.Parallel()
	raw := currentFormatBytes(t, "collection-command.json")
	command, err := DecodeProposal(raw)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := command.(*RecordCollectionProposal)
	if !ok {
		t.Fatalf("unexpected command %T", command)
	}
	encoded, err := Encode(*p)
	if err != nil || !bytes.Equal(encoded, bytes.TrimSpace(raw)) {
		t.Fatal("collection command changed", err)
	}
	raw = currentFormatBytes(t, "collection-result.json")
	var result ApplyResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || result.Collection == nil || result.Collection.Settled || result.Collection.AppliedIndex != 31 || !result.Collection.Receipt.Equal(p.Receipt) {
		t.Fatal("lost collection acceptance echo", result)
	}
	encoded, err = json.Marshal(result)
	if err != nil || !bytes.Equal(encoded, bytes.TrimSpace(raw)) {
		t.Fatal("collection acceptance changed", err)
	}
}
