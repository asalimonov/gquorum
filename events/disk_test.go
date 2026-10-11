//go:build !functional

package events

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestLegacyEventJournalCompatibility(t *testing.T) {
	t.Parallel()
	for name, kind := range map[string]Type{"operator": OperatorChanged, "scheduling": SchedulingChanged} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			legacy, err := os.ReadFile(filepath.Join("testdata", "legacy-"+name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			e, err := decodeEvent(legacy)
			if err != nil || e.Type != kind || e.OperatorID != "exec-v4-a2" {
				t.Fatalf("legacy decode: %+v/%v", e, err)
			}
			raw, err := encodeEvent(e)
			if err != nil || !bytes.Equal(raw, legacy) {
				t.Fatalf("legacy encoding changed: %s/%v", raw, err)
			}
			public, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(public, []byte(`"operator_id":"exec-v4-a2"`)) || bytes.Contains(public, []byte("execution_id")) || !bytes.Contains(public, []byte(`"type":"`+string(kind)+`"`)) {
				t.Fatalf("public names: %s", public)
			}
			path := filepath.Join(t.TempDir(), "events.bolt")
			j, err := OpenJournal(JournalOptions{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			if err = j.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketEvents)
				if err := b.SetSequence(e.Seq); err != nil {
					return err
				}
				return b.Put(be(e.Seq), legacy)
			}); err != nil {
				t.Fatal(err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			j, err = OpenJournal(JournalOptions{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := j.Close(); err != nil {
					t.Error(err)
				}
			})
			got, err := j.List(context.Background(), Query{Filter: Filter{Types: []Type{kind}}})
			if err != nil || len(got) != 1 || got[0].OperatorID != e.OperatorID {
				t.Fatalf("journal filter/reopen: %+v/%v", got, err)
			}
			if err = j.db.View(func(tx *bolt.Tx) error {
				if !bytes.Equal(tx.Bucket(bucketEvents).Get(be(e.Seq)), legacy) {
					t.Error("journal read rewrote retained bytes")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			e.At = time.Now().UTC()
			added, err := j.Append(e)
			if err != nil || added.Seq != 10 || added.Type != kind {
				t.Fatalf("append after retained sequence: %+v/%v", added, err)
			}
			want, err := encodeEvent(added)
			if err != nil {
				t.Fatal(err)
			}
			if err = j.db.View(func(tx *bolt.Tx) error {
				if !bytes.Equal(tx.Bucket(bucketEvents).Get(be(added.Seq)), want) {
					t.Error("new append did not preserve disk encoding")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
