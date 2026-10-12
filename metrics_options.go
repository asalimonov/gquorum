package gquorum

import "time"

// MetricsOptions bounds local sampling and cluster observation. Zero fields
// select defaults. Raw history is process-local and is lost on restart.
type MetricsOptions struct {
	SampleInterval   time.Duration // default 1s
	CollectInterval  time.Duration // default 5s
	CollectTimeout   time.Duration // default 2s per metrics round or native load status read
	MaxSampleAge     time.Duration // default 15s
	HistoryRetention time.Duration // default 10m
	MaxHistoryPoints int           // default 600 per series
	MaxParallelReads int           // default 8 per pool; independent collector and native load status pools
}
