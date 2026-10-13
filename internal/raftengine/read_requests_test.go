//go:build !functional

package raftengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func TestReadOptionsValidationAndDefaults(t *testing.T) {
	t.Parallel()
	d := (ReadOptions{}).withDefaults()
	if d.Timeout != 10*time.Second || d.MaxPending != 256 || d.MaxRequestBytes != 1<<20 || d.MaxResultBytes != 4<<20 {
		t.Fatal(d)
	}
	for _, o := range []ReadOptions{{Timeout: -1}, {MaxPending: -1}, {MaxRequestBytes: -1}, {MaxResultBytes: -1}} {
		if !errors.Is(o.Validate(), fault.ErrInvalid) {
			t.Fatal(o)
		}
	}
	if got := (Options{Reads: ReadOptions{MaxPending: -1}}).ValidateSettings(); !errors.Is(got, fault.ErrInvalid) {
		t.Fatal(got)
	}
}

func TestReadAdmissionOwnsSlotUntilActualCompletion(t *testing.T) {
	t.Parallel()
	// Only the admission queue is exercised: no running engine or application.
	g := &Group{started: true, ready: make(chan struct{}), reads: make(chan *readRequest, 1), readSlots: make(chan struct{}, 1)}
	close(g.ready)
	ctx, cancel := context.WithCancel(context.Background())
	req := &readRequest{ctx: ctx, data: []byte("owned"), reply: make(chan response, 1)}
	if err := g.enqueueRead(req); err != nil {
		t.Fatal(err)
	}
	owned := <-g.reads
	cancel()
	next := &readRequest{ctx: context.Background(), reply: make(chan response, 1)}
	if err := g.enqueueRead(next); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("canceled caller released owned slot", err)
	}
	g.finishRead(owned, Result{Data: []byte("late result")}, nil)
	if res := <-owned.reply; !errors.Is(res.err, context.Canceled) || len(res.result.Data) != 0 {
		t.Fatal(res)
	}
	if len(g.readSlots) != 0 || req.data != nil {
		t.Fatal("owned request not released")
	}
	if err := g.enqueueRead(next); err != nil {
		t.Fatal(err)
	}
	g.drainReadQueue(fault.ErrStopped)
	if res := <-next.reply; !errors.Is(res.err, fault.ErrStopped) {
		t.Fatal(res.err)
	}
	if len(g.readSlots) != 0 {
		t.Fatal("shutdown leaked admission")
	}
}

func TestReadAdmissionGatesAndResultOwnership(t *testing.T) {
	t.Parallel()
	g := &Group{ready: make(chan struct{}), reads: make(chan *readRequest, 1), readSlots: make(chan struct{}, 1)}
	req := &readRequest{ctx: context.Background(), reply: make(chan response, 1)}
	if err := g.enqueueRead(req); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	g.started = true
	if err := g.enqueueRead(req); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	close(g.ready)
	if err := g.enqueueRead(req); err != nil {
		t.Fatal(err)
	}
	data := []byte("value")
	g.finishRead(<-g.reads, Result{Data: data}, nil)
	data[0] = 'x'
	if res := <-req.reply; string(res.result.Data) != "value" {
		t.Fatal("result aliases application memory")
	}
	g.stopped = true
	if err := g.enqueueRead(req); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
