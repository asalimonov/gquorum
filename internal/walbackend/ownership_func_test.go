//go:build functional

package walbackend

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
)

func TestWALLeaseAdoptionAndFailureOwnership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	lease, err := recoverydisk.AcquireRoot(ctx, root)
	check(t, err)
	t.Cleanup(func() { check(t, lease.Close()) })
	opts := Options{RootDir: root, NodeName: "a", ClusterID: "test-cluster", SegmentBytes: testSegmentBytes}
	wrong := opts
	wrong.RootDir = t.TempDir()
	if store, err := OpenWithLease(wrong, lease); !errors.Is(err, fault.ErrIdentityMismatch) {
		if store != nil {
			check(t, store.Close())
		}
		t.Fatal("foreign lease adopted", err)
	}
	check(t, lease.CheckRoot())
	if other, err := Open(opts); !errors.Is(err, fault.ErrUnavailable) {
		if other != nil {
			check(t, other.Close())
		}
		t.Fatal("failed adoption released caller lease", err)
	}
	store, err := OpenWithLease(opts, lease)
	check(t, err)
	t.Cleanup(func() { check(t, store.Close()) })
	retained, err := store.HasState(ctx)
	check(t, err)
	if retained {
		t.Fatal("stable lock treated as live replica data")
	}
	if err := os.Mkdir(filepath.Join(root, recoverydisk.Namespace), 0o700); err != nil {
		t.Fatal(err)
	}
	retained, err = store.HasState(ctx)
	check(t, err)
	if !retained {
		t.Fatal("unproven namespace ignored as archive history")
	}
	check(t, store.Close())
	next, err := Open(opts)
	check(t, err)
	check(t, next.Close())
	info, err := os.Stat(filepath.Join(root, recoverydisk.LockName))
	check(t, err)
	if !info.Mode().IsRegular() {
		t.Fatal("ownership release removed stable lock")
	}
}
