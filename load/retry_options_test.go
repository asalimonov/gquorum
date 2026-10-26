//go:build !functional

package load

import (
	"math"
	"testing"
	"time"
)

func TestProposalBackoffSaturatesWithoutDurationOverflow(t *testing.T) {
	t.Parallel()
	maximum := time.Duration(math.MaxInt64)
	for _, delay := range []time.Duration{1, maximum / 2, maximum - 1, maximum} {
		for range 100 {
			wait, next := proposalBackoff(delay, maximum)
			if wait < delay || wait > maximum || next < delay || next > maximum {
				t.Fatalf("delay=%v wait=%v next=%v maximum=%v", delay, wait, next, maximum)
			}
		}
	}
}
