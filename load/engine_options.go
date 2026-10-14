package load

import (
	"time"
)

// Timing configures Raft ticks and the WAL write wait.
type Timing struct {
	TickInterval                  time.Duration
	ElectionTicks, HeartbeatTicks int
	WALWriteWait                  time.Duration
}

// ReadOptions bounds quorum read waits, pending work and query payloads.
type ReadOptions struct {
	Timeout         time.Duration
	MaxPending      int
	MaxRequestBytes int
	MaxResultBytes  int
}

// EngineOptions configures replica timing, bounded work, reads and snapshot
// thresholds.
type EngineOptions struct {
	// MaxWatchers bounds local current-state subscriptions; zero selects 128.
	MaxWatchers                             int
	Reads                                   ReadOptions
	Timing                                  Timing
	ProposalTimeout                         time.Duration // default 30s; caller's shorter deadline wins
	SnapshotThreshold                       uint64
	MaxPendingProposals                     int
	MaxUncommittedBytes, MaxApplyBatchBytes uint64
}
