//go:build functional

package authoritystore

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

func TestMetadataAcquisitionSurvivesRestartAndExactRebindRetry(t *testing.T) {
	t.Parallel()
	s, path := openLocal(t)
	check := func(want bool) {
		t.Helper()
		got, err := s.MetadataAcquired(t.Context())
		if err != nil || got != want {
			t.Fatal("metadata acquired", got, err)
		}
	}
	check(false)
	previous := s.Identity()
	if err := s.VerifyRebind(t.Context(), previous, previous, "exact-admission"); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("missing transition proof accepted", err)
	}
	if err := s.MarkMetadataAcquired(t.Context(), previous); err != nil {
		t.Fatal(err)
	}
	check(true)
	granted := previous
	granted.MemberID++
	if err := s.Rebind(t.Context(), previous, granted, "exact-admission"); err != nil {
		t.Fatal(err)
	}
	check(false)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	granted.InstanceID = "restarted"
	var err error
	s, err = Open(path, granted, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	check(false)
	if err := s.VerifyRebind(t.Context(), previous, granted, "exact-admission"); err != nil {
		t.Fatal("durable proof did not survive process change", err)
	}
	if err := s.VerifyRebind(t.Context(), previous, granted, "other-admission"); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("substituted transition proof accepted", err)
	}
	wrong := previous
	wrong.MemberID = granted.MemberID
	if err := s.VerifyRebind(t.Context(), wrong, granted, "exact-admission"); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("substituted former member accepted", err)
	}
	if err := s.MarkMetadataAcquired(t.Context(), previous); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("old member marked successor acquisition", err)
	}
	if err := s.MarkMetadataAcquired(t.Context(), granted); err != nil {
		t.Fatal(err)
	}
	if err := s.Rebind(t.Context(), previous, granted, "exact-admission"); err != nil {
		t.Fatal(err)
	}
	check(true) // an idempotent rebind must not erase acquisition evidence
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	granted.InstanceID = "restarted-again"
	s, err = Open(path, granted, false)
	if err != nil {
		t.Fatal(err)
	}
	check(true)
}

func TestMetadataAcquisitionRejectsForeignMarkerOnOpen(t *testing.T) {
	t.Parallel()
	s, path := openLocal(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		foreign := pin(s.Identity())
		foreign.MemberID++
		return put(tx.Bucket([]byte("identity")), []byte("metadata_acquired"), foreign)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(path, localIdentity(), false); !errors.Is(err, fault.ErrUnreadable) {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatal("foreign acquisition marker accepted", err)
	}
}
