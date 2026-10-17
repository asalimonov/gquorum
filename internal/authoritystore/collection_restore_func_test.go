//go:build functional

package authoritystore

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

func TestCollectionJournalPagesSurviveRemovalAndOwnTheirRows(t *testing.T) {
	t.Parallel()
	s, _ := openLocal(t)
	for _, id := range []uint64{5, 1, 6, 2, 4, 3, 7} {
		prepareLocalCollection(t, s, id)
	}
	var after string
	var seen uint64
	for {
		page, err := s.Collections(t.Context(), after, 2)
		if err != nil || len(page.Records) == 0 || len(page.Records) > 2 {
			t.Fatal(page, err)
		}
		for _, row := range page.Records {
			seen++
			if row.Retired.Identity.ReplicaID != seen {
				t.Fatal("unstable page order", row.Retired.Identity)
			}
			receipt, err := s.CompleteCollection(t.Context(), row.Retired.Identity, row.Sequence, time.Unix(20, 0))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.RemoveCollection(t.Context(), receipt); err != nil {
				t.Fatal(err)
			}
		}
		if page.Next == "" {
			break
		}
		after = page.Next // Its row was deleted; the next scan must still be exclusive.
	}
	if seen != 7 {
		t.Fatal("page scan skipped a row", seen)
	}
	page, err := s.Collections(t.Context(), after, 1)
	if err != nil || len(page.Records) != 0 || page.Next != "" {
		t.Fatal(page, err)
	}
	for _, limit := range []int{-1, 1001} {
		if _, err := s.Collections(t.Context(), "", limit); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("invalid limit accepted", err)
		}
	}
	for _, cursor := range []string{"!", base64.RawURLEncoding.EncodeToString([]byte(`{"unknown":1}`))} {
		if _, err := s.Collections(t.Context(), cursor, 1); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("invalid cursor accepted", err)
		}
	}
	id := localCollection(7).Retired.Identity
	id.ClusterID = "foreign"
	encoded, _ := json.Marshal(id)
	if _, err := s.Collections(t.Context(), base64.RawURLEncoding.EncodeToString(encoded), 1); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("foreign journal cursor accepted", err)
	}
}

func TestCollectionJournalRejectsCorruptionWithoutReplacingEvidence(t *testing.T) {
	t.Parallel()
	modes := []string{"schema1", "missing bucket", "malformed", "nested bucket", "wrong key", "zero sequence", "sequence floor", "duplicate sequence", "foreign member", "wrong receipt", "unknown field"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, path := openLocal(t)
			row := prepareLocalCollection(t, s, 7)
			key, _ := collectionKey(row.Retired.Identity)
			err := s.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte(collectionBucket))
				switch mode {
				case "schema1":
					if err := tx.DeleteBucket([]byte(collectionBucket)); err != nil {
						return err
					}
					return tx.Bucket([]byte("format")).Put([]byte("schema_version"), []byte("1"))
				case "missing bucket":
					return tx.DeleteBucket([]byte(collectionBucket))
				case "malformed":
					return bucket.Put(key, []byte("{"))
				case "nested bucket":
					_, err := bucket.CreateBucket([]byte("nested"))
					return err
				case "wrong key":
					return put(bucket, []byte("wrong"), row)
				case "zero sequence":
					row.Sequence = 0
				case "sequence floor":
					return bucket.SetSequence(0)
				case "duplicate sequence":
					other := localCollection(8)
					other.Sequence = row.Sequence
					otherKey, _ := collectionKey(other.Retired.Identity)
					return put(bucket, otherKey, other)
				case "foreign member":
					row.MemberID++
				case "wrong receipt":
					row.Retirement.Key.Revision++
				case "unknown field":
					data, _ := json.Marshal(row)
					data = append(data[:len(data)-1], []byte(`,"unknown":1}`)...)
					return bucket.Put(key, data)
				}
				return put(bucket, key, row)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			want := fault.ErrUnreadable
			if mode == "schema1" {
				want = fault.ErrUnsupported
			}
			for _, freshness := range []Freshness{RetainedEvidence, FreshEvidence} {
				if reopened, err := Open(path, localIdentity(), freshness); !errors.Is(err, want) {
					if reopened != nil {
						reopened.Close()
					}
					t.Fatal("corruption was accepted or replaced", err)
				}
			}
			if _, err := LoadIdentity(path); !errors.Is(err, want) {
				t.Fatal("identity inspection skipped journal validation", err)
			}
		})
	}
}

func TestCollectionJournalSequenceOverflowDoesNotMutateExistingProof(t *testing.T) {
	t.Parallel()
	s, _ := openLocal(t)
	first := prepareLocalCollection(t, s, 7)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(collectionBucket)).SetSequence(math.MaxUint64)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrepareCollection(t.Context(), localCollection(8)); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("sequence wrapped", err)
	}
	duplicate, err := s.PrepareCollection(t.Context(), localCollection(7))
	before, _ := json.Marshal(first)
	after, _ := json.Marshal(duplicate)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("overflow prevented exact retry", duplicate, err)
	}
}
