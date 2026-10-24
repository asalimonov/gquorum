//go:build functional

package walbackend

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/storage/wal"
)

func TestRefillRecreatesPipelineFiles(t *testing.T) {
	t.Parallel()
	// Register the test segment size through Open, never mutate the upstream global.
	s := openTest(t, t.TempDir(), "a", "test-cluster", true)
	dir := t.TempDir()
	dummy := &Store{stopC: make(chan struct{}), logger: slog.Default()}
	dummy.wg.Add(1)
	go dummy.refill(dir, nil)
	t.Cleanup(func() { close(dummy.stopC); dummy.wg.Wait() })
	deadline := time.Now().Add(3 * time.Second)
	for {
		ready := true
		for _, name := range []string{"0.tmp", "1.tmp"} {
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil || info.Size() != wal.SegmentSizeBytes {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refiller did not recreate both complete pipeline files")
		}
		time.Sleep(10 * time.Millisecond)
	}
	check(t, s.Close())
}
