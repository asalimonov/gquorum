//go:build functional

package authoritystore

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFrozenV2JournalRetainsFloorsRetirementAndCollectionAcrossRestart(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join("testdata", "scoped-v2", "authority.bolt"))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "0a638c5e52984a8e8eab09faa6df8be91c54a4564bf1d3785ed5705361b88ce0" {
		t.Fatal("frozen schema-2 fixture changed; inspect provenance rather than regenerating")
	}
	path := filepath.Join(t.TempDir(), "authority.bolt")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	id := localIdentity()
	id.InstanceID = "retained-restart"
	store, err := Open(path, id, RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	current, err := store.Current(t.Context(), "work")
	if err != nil || !current.Found || current.Record.Grant.Key.Revision != 11 || !current.Record.Complete {
		t.Fatal("lost host fence floor", current, err)
	}
	page, err := store.Collections(t.Context(), "", 10)
	if err != nil || len(page.Records) != 2 || page.Next != "" {
		t.Fatal("lost collection rows", page, err)
	}
	for i, row := range page.Records {
		if row.Retired.Identity.ReplicaID != uint64(7+i) || row.Sequence != uint64(i+1) || row.Grant.OperationID != "exec-v2-a1" {
			t.Fatal("lost exact identity or sequence", row)
		}
		proof, err := store.CompletedRetirement(t.Context(), row.Retired.Identity)
		if err != nil || !proof.Found || !proof.Record.Grant.Equal(row.Grant) || !reflect.DeepEqual(proof.Record.Receipt, &row.Retirement) {
			t.Fatal("lost independent retirement proof", proof, err)
		}
	}
	if page.Records[0].State != collectionPrepared || page.Records[0].CollectedAt != nil || page.Records[1].State != collectionCollected || page.Records[1].CollectedAt == nil || !page.Records[1].CollectedAt.Equal(time.Unix(20, 0)) {
		t.Fatal("restart changed collection milestones", page.Records)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(raw, after) {
		t.Fatal("retained open or reads rewrote evidence", err)
	}
	store, err = Open(path, id, RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	completed := page.Records[1]
	duplicate, err := store.CompleteCollection(t.Context(), completed.Retired.Identity, completed.Sequence, time.Unix(999, 0))
	if err != nil || !duplicate.Equal(collectionReceipt(completed)) {
		t.Fatal("retry replaced the first durable completion", duplicate, err)
	}
}
