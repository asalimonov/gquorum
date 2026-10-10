package raftengine

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type writeKind uint8

const (
	writeReady writeKind = iota + 1
	writeApplied
	writeSnapshot
)

type writeJob struct {
	kind     writeKind
	rd       raft.Ready
	snapshot raftpb.Snapshot
	applied  uint64
	conf     raftpb.ConfState
	request  *request
}
type writeResult struct {
	kind writeKind
	err  error
}

func (g *Group) writer() {
	defer g.wg.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var hint uint64
	var cs raftpb.ConfState
	dirty := false
	var lastWrite time.Time
	flush := func() {
		if !dirty {
			return
		}
		ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
		g.stallSince.Store(time.Now().UnixNano())
		err := g.opts.Store.SaveApplied(ctx, g.opts.GroupID, hint, cs)
		g.stallSince.Store(0)
		cancel()
		if err == nil {
			dirty = false
		}
	}
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			flush()
		case job := <-g.writeC:
			var err error
			if job.kind == writeApplied {
				if job.applied > hint {
					hint, cs, dirty = job.applied, copyConf(job.conf), true
				}
			} else {
				if job.kind == writeReady && g.opts.Timing.WALWriteWait > 0 && !lastWrite.IsZero() && time.Since(lastWrite) < 10*time.Millisecond {
					timer := time.NewTimer(g.opts.Timing.WALWriteWait)
					select {
					case <-timer.C:
					case <-g.ctx.Done():
					}
					timer.Stop()
				}
				err = g.retry(func(ctx context.Context) error {
					if job.kind == writeSnapshot {
						if err := g.opts.Store.SaveSnapshot(ctx, g.opts.GroupID, job.snapshot); err != nil {
							return err
						}
						return g.opts.Store.Compact(ctx, g.opts.GroupID, job.snapshot.Metadata.Index)
					}
					if !raft.IsEmptySnap(job.rd.Snapshot) {
						if err := g.opts.Store.SaveSnapshot(ctx, g.opts.GroupID, job.rd.Snapshot); err != nil {
							return err
						}
					}
					return g.opts.Store.SaveReady(ctx, g.opts.GroupID, storage.ReadyWrite{Entries: job.rd.Entries, HardState: job.rd.HardState})
				})
				lastWrite = time.Now()
				if err == nil {
					flush()
				}
			}
			select {
			case g.writeDone <- writeResult{job.kind, err}:
			case <-g.ctx.Done():
				return
			}
		}
	}
}
func (g *Group) retry(write func(context.Context) error) error {
	g.stallSince.Store(time.Now().UnixNano())
	defer g.stallSince.Store(0)
	backoff := 200 * time.Millisecond
	for {
		if err := g.ctx.Err(); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
		err := write(ctx)
		cancel()
		if err == nil {
			return nil
		}
		if errors.Is(err, fault.ErrInvalid) || errors.Is(err, fault.ErrIdentityMismatch) || errors.Is(err, fault.ErrUnreadable) || errors.Is(err, fault.ErrUnsupported) || errors.Is(err, fault.ErrStopped) {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-g.ctx.Done():
			timer.Stop()
			return g.ctx.Err()
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}
func (g *Group) sender() {
	defer g.wg.Done()
	for {
		select {
		case <-g.ctx.Done():
			return
		case messages := <-g.sendC:
			ctx, cancel := context.WithTimeout(g.ctx, g.opts.Timing.TickInterval*time.Duration(g.opts.Timing.ElectionTicks)/2)
			err := g.opts.Transport.Send(ctx, messages)
			cancel()
			if err != nil {
				for _, m := range messages {
					g.ReportUnreachable(m.To)
					if m.Type == raftpb.MsgSnap {
						g.ReportSnapshot(m.To, raft.SnapshotFailure)
					}
				}
			}
		}
	}
}
