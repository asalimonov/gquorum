//go:build functional

package walstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

func retiredFixture(t *testing.T) (*Store, storage.RetiredReplica) {
	t.Helper()
	s := openTest(t, t.TempDir(), "a", "test-cluster", false)
	r := storage.RetiredReplica{Identity: storage.Identity{NodeName: "a", ClusterID: "test-cluster", GroupID: "group", ReplicaID: 7}, Token: "group.quarantine-1000", QuarantinedAt: time.Unix(0, 1000).UTC()}
	dir := filepath.Join(s.opts.RootDir, r.Token)
	check(t, os.Mkdir(dir, dirMode))
	check(t, writeIdentity(dir, r.Identity))
	return s, r
}

func TestRetirementRejectsChangedProof(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"escape", "absolute", "live token", "timestamp", "identity", "owner", "directory link", "pin link", "missing pin", "live identical pin", "bound identical identity"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, original := retiredFixture(t)
			r := original
			dir := filepath.Join(s.opts.RootDir, original.Token)
			outside := t.TempDir()
			check(t, writeIdentity(outside, original.Identity))
			want := fault.ErrIdentityMismatch
			switch scenario {
			case "escape":
				r.Token = "../" + r.Token
				want = fault.ErrInvalid
			case "absolute":
				r.Token = dir
				want = fault.ErrInvalid
			case "live token":
				r.Token = "group"
				want = fault.ErrInvalid
			case "timestamp":
				r.QuarantinedAt = r.QuarantinedAt.Add(time.Nanosecond)
				want = fault.ErrInvalid
			case "identity":
				r.Identity.ReplicaID++
			case "owner":
				r.Identity.ClusterID = "foreign"
			case "directory link":
				check(t, os.RemoveAll(dir))
				check(t, os.Symlink(outside, dir))
				want = fault.ErrUnreadable
			case "pin link":
				check(t, os.Remove(filepath.Join(dir, IdentityFile)))
				check(t, os.Symlink(filepath.Join(outside, IdentityFile), filepath.Join(dir, IdentityFile)))
				want = fault.ErrUnreadable
			case "missing pin":
				check(t, os.Remove(filepath.Join(dir, IdentityFile)))
				want = fault.ErrUnreadable
			case "live identical pin":
				live := filepath.Join(s.opts.RootDir, "group")
				check(t, os.Mkdir(live, dirMode))
				check(t, writeIdentity(live, original.Identity))
			case "bound identical identity":
				check(t, s.BindReplica(context.Background(), original.Identity))
			}
			if err := s.Collect(context.Background(), r); !errors.Is(err, want) {
				t.Fatalf("collection = %v, want %v", err, want)
			}
			if _, err := os.Lstat(dir); err != nil {
				t.Fatalf("refusal removed retained record: %v", err)
			}
			if _, err := os.Stat(filepath.Join(outside, IdentityFile)); err != nil {
				t.Fatalf("collection damaged outside directory: %v", err)
			}
		})
	}
}

func TestRetirementSurvivesRestartAndPreservesSuccessor(t *testing.T) {
	t.Parallel()
	s, original := retiredFixture(t)
	check(t, s.Close())
	s = openTest(t, s.opts.RootDir, "a", "test-cluster", false)
	records, err := s.Retired(context.Background())
	check(t, err)
	if len(records) != 1 || records[0] != original {
		t.Fatalf("reconstructed records: %+v", records)
	}
	successor := original.Identity
	successor.ReplicaID++
	check(t, s.BindReplica(context.Background(), successor))
	live := filepath.Join(s.opts.RootDir, "group")
	check(t, os.Mkdir(live, dirMode))
	check(t, writeIdentity(live, successor))
	check(t, s.Collect(context.Background(), original))
	check(t, s.Collect(context.Background(), original))
	owner, found, err := readIdentity(live)
	check(t, err)
	if !found || owner != successor {
		t.Fatalf("successor changed: %+v", owner)
	}
	records, err = s.Retired(context.Background())
	check(t, err)
	if len(records) != 0 {
		t.Fatalf("collected entries still enumerated: %+v", records)
	}
}

func TestRetirementCancellationAndClosedStore(t *testing.T) {
	t.Parallel()
	s, r := retiredFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Retired(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Collect(ctx, r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	check(t, s.Close())
	if _, err := s.Retired(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if err := s.Collect(context.Background(), r); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
