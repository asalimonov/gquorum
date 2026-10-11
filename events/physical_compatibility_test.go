package events

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestBaselinePhysicalJournalPreservesRecordsAndSequence(t *testing.T) {
	t.Parallel()
	fixture := filepath.Join("testdata", "legacy-events.bolt")
	old, err := bolt.Open(fixture, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	retained := map[uint64][]byte{}
	err = old.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketEvents)
		if b.Sequence() != 2 {
			t.Fatal("baseline sequence", b.Sequence())
		}
		for _, seq := range []uint64{1, 2} {
			retained[seq] = bytes.Clone(b.Get(be(seq)))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "events.bolt")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	j, err := OpenJournal(JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i, kind := range []Type{OperatorChanged, SchedulingChanged} {
		rows, err := j.List(t.Context(), Query{Filter: Filter{Types: []Type{kind}}})
		if err != nil || len(rows) != 1 {
			t.Fatal("baseline event filter", rows, err)
		}
		e := rows[0]
		if e.Seq != uint64(i+1) || e.OperatorID != "exec-v1-a1" || e.InstanceID != "baseline-process" || e.MemberID != 1 || e.ReplicaID != 7 || e.ClusterID != "compat-cluster" || e.GroupID != "work" || e.StepSeq != 2 {
			t.Fatal("baseline event identity", e)
		}
	}
	added, err := j.Append(Event{Type: OperatorChanged, OperatorID: "exec-v2-a1"})
	if err != nil || added.Seq != 3 {
		t.Fatal("retained append sequence", added, err)
	}
	if err := j.db.View(func(tx *bolt.Tx) error {
		for seq, want := range retained {
			if !bytes.Equal(tx.Bucket(bucketEvents).Get(be(seq)), want) {
				t.Error("baseline event rewritten", seq)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenJournal(JournalOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	rows, err := j.List(t.Context(), Query{})
	if err != nil || len(rows) != 3 || rows[2].OperatorID != "exec-v2-a1" {
		t.Fatal("mixed legacy/current reopen", rows, err)
	}
}
