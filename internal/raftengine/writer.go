package raftengine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const writeRetryBackoff = 200 * time.Millisecond

type writeKind uint8

const (
	writeReady writeKind = iota + 1
	writeSnapshot
	writeRecoveryCheckpoint
	writeRecoveryFinish
)

type writeJob struct {
	ingress  *ownedSnapshotTicket
	kind     writeKind
	rd       raft.Ready
	snapshot raftpb.Snapshot
	request  *request
	proof    *model.CheckpointProof
}

// appliedHint carries the latest applied index from the run loop to the
// writer, which persists it on its timer.
type appliedHint struct {
	mu      sync.Mutex
	applied uint64
	conf    raftpb.ConfState
	saved   uint64
}

func (h *appliedHint) record(applied uint64, cs raftpb.ConfState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if applied > h.applied {
		h.applied, h.conf = applied, copyConf(cs)
	}
}

type pendingHint struct {
	Index     uint64
	ConfState raftpb.ConfState
	Dirty     bool
}

// pending returns a hint newer than the last saved one.
func (h *appliedHint) pending() pendingHint {
	h.mu.Lock()
	defer h.mu.Unlock()
	return pendingHint{Index: h.applied, ConfState: copyConf(h.conf), Dirty: h.applied > h.saved}
}

func (h *appliedHint) markSaved(applied uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.saved = max(h.saved, applied)
}

type writeResult struct {
	kind writeKind
	err  error
}

func (g *Group) writer() {
	defer g.wg.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	metricsTicker := time.NewTicker(g.opts.Metrics.SampleInterval)
	defer metricsTicker.Stop()
	g.sampleWriter(time.Now())
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.flushAppliedHint()
		case now := <-metricsTicker.C:
			g.sampleWriter(now)
		case job := <-g.writeC:
			fullSnapshot := job.kind == writeSnapshot || !raft.IsEmptySnap(job.rd.Snapshot)
			err := g.retry(func(ctx context.Context) error { return g.persistJob(ctx, job) }, fullSnapshot)
			select {
			case g.writeDone <- writeResult{job.kind, err}:
			case <-g.ctx.Done():
				return
			}
		}
	}
}

//revive:disable-next-line:flag-parameter // Full snapshots use the owner lifetime; ordinary WAL attempts stay bounded.
func (g *Group) retry(write func(context.Context) error, fullSnapshot bool) (result error) {
	g.beginWriteMetric(time.Now())
	defer func() { g.finishWriteMetric(time.Now(), result) }()
	g.stallSince.Store(time.Now().UnixNano())
	defer g.stallSince.Store(0)
	backoff := writeRetryBackoff
	for {
		if err := g.ctx.Err(); err != nil {
			return err
		}
		ctx, cancel := g.ctx, context.CancelFunc(func() {})
		if !fullSnapshot {
			ctx, cancel = context.WithTimeout(g.ctx, 5*time.Second)
		}
		started := time.Now()
		err := write(ctx)
		cancel()
		fatal := errors.Is(err, fault.ErrInvalid) || errors.Is(err, fault.ErrIdentityMismatch) ||
			errors.Is(err, fault.ErrUnreadable) ||
			errors.Is(err, fault.ErrUnsupported) || errors.Is(err, fault.ErrStopped)
		g.meter.ObservePersistence(metrics.PersistenceObservation{Latency: time.Since(started),
			Retry: err != nil && !fatal && g.ctx.Err() == nil})
		if err == nil {
			return nil
		}
		if fatal {
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

// The applied index is only a recovery hint: recovery always restores the
// snapshot and replays the committed tail. It is persisted on the ticker
// rather than after every Ready, which would add a second fsync per batch.
// Applied entries were committed by an earlier durable Ready, so the hint
// never exceeds the stored commit.
func (g *Group) flushAppliedHint() {
	hint := g.hint.pending()
	if !hint.Dirty {
		return
	}
	ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
	started := time.Now()
	g.beginWriteMetric(started)
	g.stallSince.Store(time.Now().UnixNano())
	err := g.opts.Store.SaveApplied(ctx, g.opts.GroupID, hint.Index, hint.ConfState)
	g.meter.ObservePersistence(metrics.PersistenceObservation{Latency: time.Since(started), Retry: err != nil})
	g.finishWriteMetric(time.Now(), err)
	g.stallSince.Store(0)
	cancel()
	if err == nil {
		g.hint.markSaved(hint.Index)
	}
}

func (g *Group) persistJob(ctx context.Context, job writeJob) error {
	if job.kind == writeRecoveryCheckpoint || job.kind == writeRecoveryFinish {
		return g.persistRecoveryCheckpoint(ctx, job)
	}
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
	return g.opts.Store.SaveReady(ctx, g.opts.GroupID, storage.ReadyWrite{Entries: job.rd.Entries,
		HardState: job.rd.HardState})
}
