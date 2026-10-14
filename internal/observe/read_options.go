package observe

import "time"

// ReadOptions sets the rotation, concurrency and freshness bounds for a read round.
type ReadOptions struct {
	Start, Parallel int
	MaxAge          time.Duration
}
