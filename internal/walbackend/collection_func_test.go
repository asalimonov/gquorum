//go:build functional

package walbackend

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestCollectionRecoversEveryDeletionBoundary(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"before marker", "marker", "partial contents", "contents synced", "pin removed", "directory removed", "root synced"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			s, record := retiredFixture(t)
			dir := filepath.Join(s.opts.RootDir, record.Token)
			for _, name := range []string{"first", "second"} {
				check(t, os.WriteFile(filepath.Join(dir, name), []byte("payload"), fileMode))
			}
			if boundary != "before marker" {
				s.mu.Lock()
				_, err := s.prepareCollectionMarker(record)
				s.mu.Unlock()
				check(t, err)
			}
			switch boundary {
			case "partial contents":
				check(t, os.Remove(filepath.Join(dir, "first")))
			case "contents synced", "pin removed", "directory removed", "root synced":
				check(t, removeCollectionContents(t.Context(), dir))
				check(t, syncDir(dir))
			}
			if boundary == "pin removed" || boundary == "directory removed" || boundary == "root synced" {
				check(t, os.Remove(filepath.Join(dir, IdentityFile)))
			}
			if boundary == "directory removed" || boundary == "root synced" {
				check(t, os.Remove(dir))
			}
			if boundary == "root synced" {
				check(t, syncDir(s.opts.RootDir))
			}
			check(t, s.Close())
			s = openTest(t, s.opts.RootDir, "a", "test-cluster", false)
			for range 2 {
				check(t, s.Collect(t.Context(), record))
			}
			if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("partial deletion did not finish", err)
			}
			check(t, s.ForgetCollection(t.Context(), record))
			check(t, s.Close())
			s = openTest(t, s.opts.RootDir, "a", "test-cluster", false)
			check(t, s.ForgetCollection(t.Context(), record)) // Crash before journal removal.
			if err := s.Collect(t.Context(), record); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("forgotten proof authorized deletion", err)
			}
		})
	}
}

func TestMarkedCollectionRefusesForeignAndUnpinnedContents(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"foreign pin", "unpinned contents", "directory symlink", "pin symlink"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, record := retiredFixture(t)
			dir := filepath.Join(s.opts.RootDir, record.Token)
			s.mu.Lock()
			_, err := s.prepareCollectionMarker(record)
			s.mu.Unlock()
			check(t, err)
			outside := t.TempDir()
			check(t, writeIdentity(outside, record.Identity))
			var want error = fault.ErrUnreadable
			switch mode {
			case "foreign pin":
				foreign := record.Identity
				foreign.ReplicaID++
				check(t, writeIdentity(dir, foreign))
				want = fault.ErrIdentityMismatch
			case "unpinned contents":
				check(t, os.Remove(filepath.Join(dir, IdentityFile)))
				check(t, os.WriteFile(filepath.Join(dir, "unknown"), []byte("preserve"), fileMode))
			case "directory symlink":
				check(t, os.RemoveAll(dir))
				check(t, os.Symlink(outside, dir))
			case "pin symlink":
				check(t, os.Remove(filepath.Join(dir, IdentityFile)))
				check(t, os.Symlink(filepath.Join(outside, IdentityFile), filepath.Join(dir, IdentityFile)))
			}
			if err := s.Collect(t.Context(), record); !errors.Is(err, want) {
				t.Fatal("marker bypassed pin/contents checks", err, want)
			}
			if err := s.ForgetCollection(t.Context(), record); err == nil {
				t.Fatal("remaining retired location allowed marker release")
			}
			for _, path := range []string{dir, dir + collectionSuffix, filepath.Join(outside, IdentityFile)} {
				if _, err := os.Lstat(path); err != nil {
					t.Fatal("refusal lost evidence", path, err)
				}
			}
		})
	}
}

func TestCollectionDeletionErrorsPreserveProofForRetry(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses filesystem permission failures")
	}
	for _, boundary := range []string{"contents", "directory"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			s, record := retiredFixture(t)
			dir := filepath.Join(s.opts.RootDir, record.Token)
			check(t, os.WriteFile(filepath.Join(dir, "payload"), []byte("data"), fileMode))
			s.mu.Lock()
			_, err := s.prepareCollectionMarker(record)
			s.mu.Unlock()
			check(t, err)
			blocked := dir
			if boundary == "directory" {
				blocked = s.opts.RootDir
			}
			check(t, os.Chmod(blocked, 0o500))
			t.Cleanup(func() { _ = os.Chmod(blocked, dirMode) })
			if err := s.Collect(t.Context(), record); !errors.Is(err, os.ErrPermission) {
				t.Fatal("deletion error was not reported", err)
			}
			if _, err := os.Stat(dir + collectionSuffix); err != nil {
				t.Fatal("failed deletion lost marker", err)
			}
			if boundary == "contents" {
				if _, err := os.Stat(filepath.Join(dir, IdentityFile)); err != nil {
					t.Fatal("failed content removal lost pin", err)
				}
			} else if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
				t.Fatal("final directory removal did not leave recoverable empty directory", entries, err)
			}
			check(t, os.Chmod(blocked, dirMode))
			check(t, s.Collect(t.Context(), record))
		})
	}
}

func TestForgetCollectionRequiresExactMarkerAndAvailableRoot(t *testing.T) {
	t.Parallel()
	s, record := retiredFixture(t)
	check(t, s.Collect(t.Context(), record))
	wrong := record
	wrong.Identity.ReplicaID++
	if err := s.ForgetCollection(t.Context(), wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("wrong identity released marker", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.ForgetCollection(ctx, record); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	check(t, s.Close())
	if err := s.ForgetCollection(t.Context(), record); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	marker := filepath.Join(s.opts.RootDir, record.Token) + collectionSuffix
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("refused release removed marker", err)
	}
}

func TestForgetCollectionRefusesUnavailableOrReplacedRoot(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, record := retiredFixture(t)
			check(t, s.Collect(t.Context(), record))
			root := s.opts.RootDir
			moved := filepath.Join(t.TempDir(), "retained")
			check(t, os.Rename(root, moved))
			defer func() {
				_ = os.RemoveAll(root)
				check(t, os.Rename(moved, root))
			}()
			if mode == "replaced" {
				check(t, os.Mkdir(root, dirMode))
				check(t, writeCollectionMarker(filepath.Join(root, record.Token)+collectionSuffix, record))
			}
			if err := s.ForgetCollection(t.Context(), record); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("wrong root released collection proof", err)
			}
			if _, err := os.Stat(filepath.Join(moved, record.Token) + collectionSuffix); err != nil {
				t.Fatal("original proof disappeared", err)
			}
		})
	}
}
