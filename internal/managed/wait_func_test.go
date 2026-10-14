//go:build functional

package managed

import (
	"context"
	"testing"
	"time"
)

func await(ctx context.Context, t *testing.T, condition func() bool) {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}
