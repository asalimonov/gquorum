//go:build functional

package managed

import (
	"context"
	"testing"
	"time"
)

func await(t *testing.T, ctx context.Context, condition func() bool) {
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
