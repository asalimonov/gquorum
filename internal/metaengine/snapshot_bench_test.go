//go:build !functional

package metaengine

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkMetaSnapshotMatrix(b *testing.B) {
	for _, size := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("%d-nodes", size), func(b *testing.B) {
			machine := snapshotMatrix(b, size)
			snapshot, err := machine.Snapshot(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			b.Run("serialize", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(snapshot)))
				for b.Loop() {
					result, err := machine.Snapshot(context.Background())
					if err != nil || len(result) != len(snapshot) {
						b.Fatal("snapshot changed", err)
					}
				}
				b.ReportMetric(float64(len(snapshot)), "snapshot_bytes")
			})
			b.Run("restore", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(snapshot)))
				for b.Loop() {
					restored := mustMachine(b, machine.rules, nil)
					if err := restored.Restore(context.Background(), snapshot); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(snapshot)), "snapshot_bytes")
			})
		})
	}
}
