package raftengine

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

// QueryMachine is optional. SupportsQuery lets adapters report absence without
// invoking an application callback or starting a quorum round.
type QueryMachine interface {
	SupportsQuery() bool
	Query(context.Context, []byte) (Result, error)
}

type ReadOptions struct {
	Timeout         time.Duration
	MaxPending      int
	MaxRequestBytes int
	MaxResultBytes  int
}

func (o ReadOptions) Validate() error {
	if o.Timeout < 0 || o.MaxPending < 0 || o.MaxRequestBytes < 0 || o.MaxResultBytes < 0 {
		return fault.New(fault.Invalid, "negative consistent-read duration or bound", nil)
	}
	return nil
}
func (o ReadOptions) withDefaults() ReadOptions {
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}
	if o.MaxPending == 0 {
		o.MaxPending = 256
	}
	if o.MaxRequestBytes == 0 {
		o.MaxRequestBytes = 1 << 20
	}
	if o.MaxResultBytes == 0 {
		o.MaxResultBytes = 4 << 20
	}
	return o
}
