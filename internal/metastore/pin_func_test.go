//go:build functional

package metastore

import (
	"bytes"
	"errors"
	"math"
	"os"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

func TestIdentityPinRejectsEveryPartialOrMalformedFieldWithoutRepair(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*bolt.Bucket) error
	}{
		{"only member", func(b *bolt.Bucket) error {
			if err := b.Delete(keyNodeName); err != nil {
				return err
			}
			return b.Delete(keyClusterID)
		}},
		{"missing member", func(b *bolt.Bucket) error { return b.Delete(keyMemberID) }},
		{"missing node", func(b *bolt.Bucket) error { return b.Delete(keyNodeName) }},
		{"missing cluster", func(b *bolt.Bucket) error { return b.Delete(keyClusterID) }},
		{"short member", func(b *bolt.Bucket) error { return b.Put(keyMemberID, []byte{1}) }},
		{"zero member", func(b *bolt.Bucket) error { return b.Put(keyMemberID, itob(0)) }},
		{"exhausted member", func(b *bolt.Bucket) error { return b.Put(keyMemberID, itob(math.MaxUint64)) }},
		{"empty node", func(b *bolt.Bucket) error { return b.Put(keyNodeName, []byte(" ")) }},
		{"invalid cluster", func(b *bolt.Bucket) error { return b.Put(keyClusterID, []byte("cluster\x00other")) }},
		{"unknown field", func(b *bolt.Bucket) error { return b.Put([]byte("unknown"), []byte("data")) }},
		{"invalid seen marker", func(b *bolt.Bucket) error { return b.Put(keyMemberSeen, []byte{0}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := openTestStore(t)
			if err := s.EnsureIdentity("node", "cluster", 1); err != nil {
				t.Fatal(err)
			}
			if err := s.db.Update(func(tx *bolt.Tx) error { return tc.edit(tx.Bucket(bucketIdentity)) }); err != nil {
				t.Fatal(err)
			}
			if err := s.EnsureIdentity("node", "cluster", 1); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt pin repaired/rebound", err)
			}
			if err := s.RecordReplica("work", ReplicaRecord{NodeName: "node", ClusterID: "cluster", ReplicaID: 7}); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt pin accepted by ledger", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(s.path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := LoadIdentity(s.path); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt pin loaded", err)
			}
			if _, err := PeekLocalLineage(s.path); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt lineage accepted", err)
			}
			after, err := os.ReadFile(s.path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("read-only identity checks changed database", err)
			}
		})
	}
}

func TestFreshnessIncludesOrphanLocalAndConsensusState(t *testing.T) {
	t.Parallel()
	for _, bucket := range [][]byte{bucketIdentity, bucketOverride, bucketLoadReplicas, bucketHardState, bucketEntries, bucketSnapshot, bucketApplied, bucketState, []byte("foreign")} {
		t.Run(string(bucket), func(t *testing.T) {
			t.Parallel()
			s := openTestStore(t)
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists(bucket)
				if err != nil {
					return err
				}
				return b.Put([]byte("retained"), []byte("data"))
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.EnsureIdentity("node", "cluster", 1); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("orphan state rebound", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(s.path)
			if err != nil {
				t.Fatal(err)
			}
			if fresh, err := StoreIsFresh(s.path); err != nil || fresh {
				t.Fatal("retained state declared fresh", fresh, err)
			}
			after, err := os.ReadFile(s.path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("freshness probe mutated database", err)
			}
		})
	}
}

func TestMetaSchemaProbeRefusesIncompleteAndUnversionedStores(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*bolt.Tx) error
		want error
	}{
		{"missing format", func(tx *bolt.Tx) error { return tx.DeleteBucket(bucketFormat) }, fault.ErrUnsupported},
		{"missing bucket", func(tx *bolt.Tx) error { return tx.DeleteBucket(bucketIdentity) }, fault.ErrUnreadable},
		{"bad version", func(tx *bolt.Tx) error { return tx.Bucket(bucketFormat).Put(keySchemaVersion, []byte("99")) }, fault.ErrUnsupported},
		{"unknown format field", func(tx *bolt.Tx) error { return tx.Bucket(bucketFormat).Put([]byte("unknown"), []byte("data")) }, fault.ErrUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := openTestStore(t)
			if err := s.db.Update(tc.edit); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadIdentity(s.path); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			if fresh, err := StoreIsFresh(s.path); fresh || !errors.Is(err, tc.want) {
				t.Fatal(fresh, err)
			}
			if reopened, err := Open(s.path); !errors.Is(err, tc.want) {
				if reopened != nil {
					_ = reopened.Close()
				}
				t.Fatal("damaged schema repaired", err)
			}
		})
	}
}
