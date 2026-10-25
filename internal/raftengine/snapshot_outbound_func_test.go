//go:build functional

package raftengine

import (
	"context"
	"errors"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/memstore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type snapshotJobDouble struct {
	started   chan context.Context
	release   chan struct{}
	discarded atomic.Int32
	runs      atomic.Int32
}

func (j *snapshotJobDouble) Run(ctx context.Context) { j.runs.Add(1); j.started <- ctx; <-j.release }
func (j *snapshotJobDouble) Discard(error)           { j.discarded.Add(1) }

type snapshotTransportDouble struct {
	ordinary chan []raftpb.Message
	prepared chan SnapshotOffer
	job      SnapshotJob
	failure  error
}

func (d *snapshotTransportDouble) Send(_ context.Context, m []raftpb.Message) error {
	d.ordinary <- m
	return nil
}
func (d *snapshotTransportDouble) PrepareSnapshot(o SnapshotOffer) (SnapshotJob, error) {
	d.prepared <- o
	return d.job, d.failure
}

// One native engine with a protocol double for its learner; no second replica.
// The test drives the owner synchronously and may start independent send workers.
func outboundSnapshotLeader(t *testing.T, transport Transport) *Group {
	t.Helper()
	return outboundSnapshotLeaderForGroup(t, transport, "load")
}
func outboundSnapshotLeaderForGroup(t *testing.T, transport Transport, groupID string) *Group {
	t.Helper()
	var s storage.Store
	create := New
	if groupID == topology.MetaGroupID {
		meta, err := metastore.Open(filepath.Join(t.TempDir(), "meta.bolt"))
		must(t, err)
		t.Cleanup(func() { must(t, meta.Close()) })
		s = &metastore.Adapter{Store: meta}
		create = NewMeta
	} else {
		memory, err := memstore.New(memstore.Options{NodeName: "1", ClusterID: "cluster"})
		must(t, err)
		t.Cleanup(func() { must(t, memory.Close()) })
		s = memory
	}
	g, err := create(Options{ClusterID: "cluster", GroupID: groupID, NodeName: "1", ReplicaID: 1, Mode: Founding,
		Peers: []raft.Peer{{ID: 1}}, FoundingLearners: []raft.Peer{{ID: 2}}, Store: s, Machine: &machine{}, Transport: transport,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(t, err)
	g.ctx, g.cancel = context.WithCancel(t.Context())
	t.Cleanup(func() { g.cancel(); g.wg.Wait(); must(t, g.Stop(testContext(t))) })
	must(t, g.rn.Campaign())
	settleIngressReady(t, g)
	if g.rn.BasicStatus().RaftState != raft.StateLeader {
		t.Fatal("not leader")
	}
	for len(g.sendC) > 0 {
		<-g.sendC
	}
	return g
}
func outboundSnapshotMessage(t *testing.T, g *Group) raftpb.Message {
	t.Helper()
	ss, err := g.ms.Snapshot()
	must(t, err)
	return raftpb.Message{Type: raftpb.MsgSnap, From: 1, To: 2, Term: g.rn.BasicStatus().Term, Snapshot: &ss}
}
func newSnapshotTransportDouble(t *testing.T) (*snapshotTransportDouble, *snapshotJobDouble) {
	t.Helper()
	job := &snapshotJobDouble{started: make(chan context.Context, 1), release: make(chan struct{})}
	return &snapshotTransportDouble{ordinary: make(chan []raftpb.Message, 8), prepared: make(chan SnapshotOffer, 8), job: job}, job
}
func TestSnapshotOutbound_HeartbeatContinuesAndEpochJoins(t *testing.T) {
	t.Parallel()
	d, job := newSnapshotTransportDouble(t)
	g := outboundSnapshotLeader(t, d)
	release := closeIngressGate(t, job.release)
	m := outboundSnapshotMessage(t, g)
	rd, jobs, err := g.ownReady(raft.Ready{Messages: []raftpb.Message{
		{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: m.Term, Context: []byte("before")}, m,
		{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: m.Term, Context: []byte("after")},
	}})
	must(t, err)
	offer := <-d.prepared
	if len(jobs) != 1 || len(rd.Messages) != 2 || string(rd.Messages[0].Context) != "before" || string(rd.Messages[1].Context) != "after" {
		t.Fatal(rd)
	}
	if &offer.Message.Snapshot.Data[0] != &m.Snapshot.Data[0] || offer.PublicationGeneration == 0 || offer.Attempt.RunID != g.nonce {
		t.Fatal("pin or source identity lost")
	}
	g.dispatchSnapshotJobs(jobs)
	var running context.Context
	select {
	case running = <-job.started:
	case <-testContext(t).Done():
		t.Fatal("job not started")
	}
	g.wg.Add(1)
	go g.sender()
	g.sendC <- rd.Messages
	select {
	case got := <-d.ordinary:
		if len(got) != 2 || got[0].Type != raftpb.MsgHeartbeat {
			t.Fatal(got)
		}
	case <-testContext(t).Done():
		t.Fatal("snapshot blocked ordinary send")
	}
	must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: m.Term + 1}))
	g.refreshSnapshotEpoch()
	select {
	case <-running.Done():
	case <-testContext(t).Done():
		t.Fatal("deposed epoch remained live")
	}
	g.cancel()
	joined := make(chan struct{})
	go func() { g.wg.Wait(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("blocked job not owned")
	default:
	}
	release()
	select {
	case <-joined:
	case <-testContext(t).Done():
		t.Fatal("job not joined")
	}
	if job.runs.Load() != 1 || job.discarded.Load() != 0 {
		t.Fatal("job entered twice")
	}
}

func TestSnapshotOutbound_UnstartedDiscardAndAdmissionFailure(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"epoch", "stop", "failed-ready", "capacity", "permanent"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			d, job := newSnapshotTransportDouble(t)
			g := outboundSnapshotLeader(t, d)
			closeIngressGate(t, job.release)
			if mode == "capacity" {
				d.job = nil
				d.failure = fault.ErrOverloaded
			}
			if mode == "permanent" {
				d.job = nil
				d.failure = fault.ErrUnsupported
			}
			m := outboundSnapshotMessage(t, g)
			_, jobs, err := g.ownReady(raft.Ready{Messages: []raftpb.Message{m}})
			must(t, err)
			offer := <-d.prepared
			switch mode {
			case "epoch":
				must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: m.Term + 1}))
				g.dispatchSnapshotJobs(jobs)
				g.wg.Wait()
			case "stop":
				g.cancel()
				g.dispatchSnapshotJobs(jobs)
			case "failed-ready":
				discardSnapshotJobs(jobs, errors.New("persistence failure"))
				discardSnapshotJobs(jobs, fault.ErrStopped)
			default:
				record := g.snapshotReports[2]
				if len(jobs) != 0 || record.pending == nil || record.pending.attempt != offer.Attempt {
					t.Fatal("refusal lost")
				}
				delay := time.Until(record.pending.due)
				if mode == "capacity" && (delay <= 0 || delay > time.Second) {
					t.Fatal(delay)
				}
				if mode == "permanent" && (delay < 29*time.Second || delay > 30*time.Second) {
					t.Fatal(delay)
				}
				return
			}
			if job.runs.Load() != 0 || job.discarded.Load() != 1 {
				t.Fatal(job.runs.Load(), job.discarded.Load())
			}
		})
	}
}

func TestSnapshotOutbound_NativeReportsAreExactAndDelayed(t *testing.T) {
	t.Parallel()
	d, job := newSnapshotTransportDouble(t)
	g := outboundSnapshotLeader(t, d)
	closeIngressGate(t, job.release)
	// Reject the initial append at the compacted boundary. The native learner
	// enters StateSnapshot and emits a real snapshot Ready.
	must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: g.rn.BasicStatus().Term, Reject: true, Index: 1, RejectHint: 0}))
	settleIngressReady(t, g)
	var offer SnapshotOffer
	select {
	case offer = <-d.prepared:
	case <-testContext(t).Done():
		t.Fatal("native snapshot not offered", g.rn.Status().Progress[2])
	}
	// The job may be running after native durability; it need not finish to prove
	// the engine's coalescing report and cooldown boundaries.
	if p := g.rn.Status().Progress[2]; p.State != tracker.StateSnapshot {
		t.Fatal(p)
	}
	g.snapshotReportWake <- struct{}{}
	bad := offer.Attempt
	bad.Generation++
	g.ReportSnapshotAttempt(bad, raft.SnapshotFinish)
	if g.snapshotReports[2].pending != nil {
		t.Fatal("stale attempt reported")
	}
	g.ReportSnapshotAttemptError(offer.Attempt, fault.ErrOverloaded)
	report := g.snapshotReports[2].pending
	g.drainSnapshotReports(report.due.Add(-time.Nanosecond))
	if g.rn.Status().Progress[2].State != tracker.StateSnapshot {
		t.Fatal("cooldown bypassed")
	}
	g.drainSnapshotReports(report.due)
	if g.rn.Status().Progress[2].State == tracker.StateSnapshot || g.snapshotReports[2].pending != nil {
		t.Fatal("saturated wake lost report")
	}
}

func TestSnapshotOutbound_LegacyCopyAndPublicationGeneration(t *testing.T) {
	t.Parallel()
	g := outboundSnapshotLeader(t, &captureTransport{})
	m := outboundSnapshotMessage(t, g)
	generation := g.snapshotPublication
	rd, jobs, err := g.ownReady(raft.Ready{Messages: []raftpb.Message{m}})
	must(t, err)
	if len(jobs) != 0 || len(rd.Messages) != 1 || &rd.Messages[0].Snapshot.Data[0] == &m.Snapshot.Data[0] {
		t.Fatal("legacy ownership changed")
	}
	g.ReportSnapshot(2, raft.SnapshotFinish)
	if g.snapshotReports[2].pending == nil {
		t.Fatal("legacy report lost")
	}
	must(t, g.beginSnapshot(nil))
	if g.pending == nil {
		t.Fatal("expected local snapshot")
	}
	write := <-g.writeC
	must(t, g.persistJob(t.Context(), write))
	must(t, g.finishWrite(write.kind))
	if g.snapshotPublication <= generation {
		t.Fatal("publication not advanced")
	}
	if &m.Snapshot.Data[0] == nil || len(m.Snapshot.Data) == 0 {
		t.Fatal("old artifact pin lost")
	}
}

func TestSnapshotOutbound_BackoffSurvivesEpochButResetsOnMatch(t *testing.T) {
	t.Parallel()
	g := outboundSnapshotLeader(t, &captureTransport{})
	offer, err := g.prepareSnapshotOffer(outboundSnapshotMessage(t, g))
	must(t, err)
	record := g.snapshotReports[2]
	record.backoff = 8 * time.Second
	g.NoteSnapshotProgress(offer.Attempt, metrics.SnapshotCatchUp{TotalBytes: 100, VerifiedBytes: 8, ProgressSequence: 1, Destination: metrics.ReplicaIdentity{ReplicaID: 2}})
	if record.backoff != 8*time.Second {
		t.Fatal("bytes reset failed cooldown")
	}
	must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: offer.Attempt.Term + 1}))
	g.refreshSnapshotEpoch()
	if record.backoff != 8*time.Second || record.attempt.Generation != 0 {
		t.Fatal("epoch reset cooldown or retained report")
	}
	must(t, g.rn.Campaign())
	settleIngressReady(t, g)
	if g.rn.BasicStatus().RaftState != raft.StateLeader {
		t.Fatal("leadership not renewed")
	}
	// Native acknowledgement advances historical Match; only this resets retry.
	must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: g.rn.BasicStatus().Term, Index: 1}))
	g.refreshSnapshotEpoch()
	if record.backoff != 0 {
		t.Fatal("Match advance did not reset cooldown")
	}
}

type snapshotReadyFailure struct{ storage.Store }

func (*snapshotReadyFailure) SaveReady(context.Context, string, storage.ReadyWrite) error {
	return fault.ErrUnreadable
}

func TestSnapshotOutbound_FailedReadyDiscardsBeforeDispatch(t *testing.T) {
	t.Parallel()
	d, job := newSnapshotTransportDouble(t)
	g := outboundSnapshotLeader(t, d)
	closeIngressGate(t, job.release)
	data, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application, RequestID: "pending", Correlation: "pending", Data: []byte("pending")})
	must(t, err)
	must(t, g.rn.Propose(data))
	must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: g.rn.BasicStatus().Term, Reject: true, Index: 1, RejectHint: 0}))
	must(t, g.beginReady())
	if g.pending == nil || len(g.pending.snapshotJobs) != 1 || job.runs.Load() != 0 {
		t.Fatal("bulk crossed undurable boundary")
	}
	g.opts.Store = &snapshotReadyFailure{Store: g.opts.Store}
	must(t, g.Start(t.Context()))
	select {
	case <-g.done:
	case <-testContext(t).Done():
		t.Fatal("failed engine did not join")
	}
	if !errors.Is(g.exitError(), fault.ErrUnreadable) || job.discarded.Load() != 1 || job.runs.Load() != 0 {
		t.Fatal(g.exitError(), job.discarded.Load(), job.runs.Load())
	}
}

func TestSnapshotOutbound_LegacyFailureReportsRemainImmediate(t *testing.T) {
	t.Parallel()
	for _, groupID := range []string{"load", topology.MetaGroupID} {
		t.Run(groupID, func(t *testing.T) {
			t.Parallel()
			g := outboundSnapshotLeaderForGroup(t, &captureTransport{}, groupID)
			must(t, g.rn.Step(raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: g.rn.BasicStatus().Term, Reject: true, Index: 1, RejectHint: 0}))
			settleIngressReady(t, g)
			if g.rn.Status().Progress[2].State != tracker.StateSnapshot {
				t.Fatal("native snapshot was not offered")
			}
			g.ReportSnapshot(2, raft.SnapshotFailure)
			record := g.snapshotReports[2]
			if record.pending == nil || record.pending.due.After(time.Now()) || record.backoff != 0 {
				t.Fatal("legacy failure acquired managed cooldown", record)
			}
			g.drainSnapshotReports(time.Now())
			if g.rn.Status().Progress[2].State == tracker.StateSnapshot || record.pending != nil {
				t.Fatal("legacy failure was delayed")
			}
		})
	}
}
func TestSnapshotOutbound_LegacyDoesNotRequireDigestGeneration(t *testing.T) {
	t.Parallel()
	g := outboundSnapshotLeader(t, &captureTransport{})
	g.snapshotPublication = 0
	rd, jobs, err := g.ownReady(raft.Ready{Messages: []raftpb.Message{outboundSnapshotMessage(t, g)}})
	must(t, err)
	if len(rd.Messages) != 1 || len(jobs) != 0 {
		t.Fatal("legacy snapshot discarded by bulk-only generation rule")
	}
}
func TestSnapshotOutbound_RecoveryCheckpointRenewsPublication(t *testing.T) {
	t.Parallel()
	g := outboundSnapshotLeader(t, &captureTransport{})
	for _, index := range []uint64{g.applied, g.applied} {
		before := g.snapshotPublication
		term, err := g.ms.Term(index)
		must(t, err)
		data, err := encodeSnapshot(g.opts, []byte("checkpoint"), g.authority)
		must(t, err)
		g.pending = &writeJob{kind: writeRecoveryCheckpoint, snapshot: raftpb.Snapshot{Data: data,
			Metadata: raftpb.SnapshotMetadata{Index: index, Term: term, ConfState: copyConf(g.cs)}},
			request: &request{reply: make(chan response, 1)}, proof: new(model.CheckpointProof)}
		must(t, g.finishRecoveryCheckpoint())
		if g.snapshotPublication != before+1 {
			t.Fatal("checkpoint artifact reused publication generation")
		}
	}
}

func TestSnapshotOutbound_EpisodePublishesOnlyFromNativeOwner(t *testing.T) {
	t.Parallel()
	g := outboundSnapshotLeader(t, &captureTransport{})
	g.opts.SnapshotPolicy = SnapshotPolicy{TotalTimeout: 10 * time.Second, ProgressTimeout: 2 * time.Second}
	offer, err := g.prepareSnapshotOffer(outboundSnapshotMessage(t, g))
	must(t, err)
	native := g.rn.Status().Progress[2]
	sample := metrics.SnapshotCatchUp{DeliveryID: 1, SnapshotIndex: offer.Message.Snapshot.Metadata.Index,
		SnapshotTerm: offer.Message.Snapshot.Metadata.Term, LeaderTerm: offer.Attempt.Term, TotalBytes: 100, VerifiedBytes: 50,
		ProgressSequence: 1, ProgressTimeout: 2 * time.Second, RemainingTotal: 10 * time.Second, Phase: "sending",
		Destination: metrics.ReplicaIdentity{ClusterID: "cluster", GroupID: "load", NodeName: "two", ReplicaID: 2, InstanceID: "process", RunID: "receiver-1"}}
	sequence := g.raftSample.Raft.Sequence
	g.NoteSnapshotProgress(offer.Attempt, sample)
	if g.raftSample.Raft.Sequence != sequence {
		t.Fatal("worker published a Raft generation")
	}
	g.sampleRaftMetrics(time.Now())
	var observed metrics.PeerProgress
	for _, peer := range g.raftSample.Progress {
		if peer.ReplicaID == 2 {
			observed = peer
		}
	}
	if observed.Snapshot == nil || observed.Snapshot.VerifiedBytes != 50 || observed.Snapshot.EpisodeRemaining <= 0 ||
		observed.Match != native.Match || observed.RecentActive != native.RecentActive || observed.AckSequence != 0 {
		t.Fatal("snapshot sample missing or manufactured native evidence", observed)
	}
	original := g.snapshotEpisodes[2].Deadline
	g.ReportSnapshotAttemptTimeout(offer.Attempt)
	g.sampleRaftMetrics(time.Now())
	if !g.snapshotEpisodes[2].Failed {
		t.Fatal("terminal timeout not consumed by owner")
	}
	offer, err = g.prepareSnapshotOffer(outboundSnapshotMessage(t, g))
	must(t, err)
	sample.DeliveryID++
	sample.Destination.RunID = "receiver-2"
	sample.EpisodeFailed = false
	sample.EpisodeRemaining = time.Hour
	g.NoteSnapshotProgress(offer.Attempt, sample)
	g.sampleRaftMetrics(time.Now())
	if !g.snapshotEpisodes[2].Failed || g.snapshotEpisodes[2].Deadline != original {
		t.Fatal("receiver churn renewed bounded protection")
	}
	g.cs = copyConf(*g.rn.ApplyConfChange(raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}}}))
	g.refreshSnapshotEpoch()
	if len(g.snapshotEpisodes) != 0 {
		t.Fatal("retired native peer retained episode")
	}
}
