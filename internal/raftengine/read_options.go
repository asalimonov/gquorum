package raftengine

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

const (
	defaultReadTimeout         = 10 * time.Second
	defaultMaxPendingReads     = 256
	defaultMaxReadRequestBytes = 1 << 20
	defaultMaxReadResultBytes  = 4 << 20
)

// QueryMachine is optional. SupportsQuery lets adapters report absence without
// invoking an application callback or starting a quorum round.
type QueryMachine interface {
	SupportsQuery() bool
	Query(context.Context, []byte) (Result, error)
}

// ReadOptions bounds pending quorum reads, timeouts and query payload sizes.
type ReadOptions struct {
	Timeout         time.Duration
	MaxPending      int
	MaxRequestBytes int
	MaxResultBytes  int
}

// Validate checks nonnegative read settings and applies bounds to resolved
// defaults.
func (o ReadOptions) Validate() error {
	if o.Timeout < 0 || o.MaxPending < 0 || o.MaxRequestBytes < 0 || o.MaxResultBytes < 0 {
		return fault.New(fault.Invalid, "negative consistent-read duration or bound", nil)
	}
	return nil
}
func (o ReadOptions) withDefaults() ReadOptions {
	if o.Timeout == 0 {
		o.Timeout = defaultReadTimeout
	}
	if o.MaxPending == 0 {
		o.MaxPending = defaultMaxPendingReads
	}
	if o.MaxRequestBytes == 0 {
		o.MaxRequestBytes = defaultMaxReadRequestBytes
	}
	if o.MaxResultBytes == 0 {
		o.MaxResultBytes = defaultMaxReadResultBytes
	}
	return o
}
