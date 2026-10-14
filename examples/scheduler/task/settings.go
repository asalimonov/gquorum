package task

import (
	"errors"
	"math"
	"time"
)

// SchemaVersion identifies the v2 command, snapshot and SQLite layout.
const SchemaVersion = 2

// Fixed example settings. They are code constants rather than replicated
// configuration: every replica runs the same binary.
const (
	HistoryLimit  = 1000 // retained terminal tasks
	QueueCapacity = 4    // waiting assignments per executor, plus one running
	BurstSize     = 3    // tasks per creation tick
	MaxBatch      = 10   // tasks per assign or revoke command
	BurstInterval = 3 * time.Second
	MinDuration   = 2 * time.Second
	MaxDuration   = 5 * time.Second
)

// CheckedInt converts a Raft identifier or index to SQLite's int64 range.
func CheckedInt(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, errors.New("identifier or index exceeds SQLite integer range")
	}
	return int64(value), nil
}
