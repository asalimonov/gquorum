//go:build functional

package metastore

import (
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

func TestReplicaLedgerReplacementCASPreservesNewIdentity(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.EnsureIdentity("a", "cluster", 1); err != nil {
		t.Fatal(err)
	}
	old := ReplicaRecord{NodeName: "a", ClusterID: "cluster", ReplicaID: 7, Origin: "fresh", CreatedAt: time.Unix(1, 0).UTC()}
	newRecord := old
	newRecord.ReplicaID = 8
	newRecord.Origin = "reseat"
	if err := s.RecordReplicaCAS("load", newRecord, 7); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("missing expected record replaced", err)
	}
	if err := s.RecordReplica("load", old); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordReplicaCAS("load", newRecord, 6); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if err := s.RecordReplicaCAS("load", newRecord, 7); err != nil {
		t.Fatal(err)
	}
	retry := newRecord
	retry.Origin = "resume"
	retry.CreatedAt = time.Unix(9, 0)
	if err := s.RecordReplicaCAS("load", retry, 7); err != nil {
		t.Fatal("repeat not idempotent", err)
	}
	if err := s.RecordReplicaCAS("load", old, 7); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale replacement rewound identity", err)
	}
	if err := s.ForgetReplica("load", 7); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("old cleanup erased replacement", err)
	}
	if err := s.ForgetReplica("load", 0); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	got, err := s.LookupReplica("load")
	if err != nil || !got.Found || got.Record != newRecord {
		t.Fatal(got.Record, got.Found, err)
	}
}

func TestReplicaLedgerRejectsCorruptAndForeignLocalRecords(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	if err := s.EnsureIdentity("a", "cluster", 1); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{"node_name":"a","cluster_id":"cluster","replica_id":0}`, `{"node_name":"foreign","cluster_id":"cluster","replica_id":7}`} {
		if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketLoadReplicas).Put([]byte("load"), []byte(raw)) }); err != nil {
			t.Fatal(err)
		}
		if found, err := s.LookupReplica("load"); found.Found || !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal(found.Found, err)
		}
		if err := s.RecordReplicaCAS("load", ReplicaRecord{NodeName: "a", ClusterID: "cluster", ReplicaID: 8}, 7); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal(err)
		}
		if err := s.ForgetReplica("load", 7); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal(err)
		}
	}
}
