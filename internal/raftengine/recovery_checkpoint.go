package raftengine

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type recoveryCheckpointStore interface {
	SaveRecoveryCheckpoint(context.Context, model.SeedKey, raftpb.Snapshot) error
	VerifyRecoveryCheckpoint(context.Context, model.SeedKey, model.Position) (model.CheckpointProof, error)
	FinishRecovery(context.Context, model.SeedKey, model.Consumed) error
}

// checkpointMemory preserves RawNode's stable Storage pointer when a portable
// application snapshot replaces the seed at exactly the same Raft position.
type checkpointMemory struct {
	*raft.MemoryStorage
	mu         sync.Mutex
	normalized raftpb.Snapshot
}

func (m *checkpointMemory) Snapshot() (raftpb.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ss, err := m.MemoryStorage.Snapshot()
	if err == nil && m.normalized.Metadata.Index == ss.Metadata.Index && len(m.normalized.Data) > 0 {
		return m.normalized, nil
	}
	return ss, err
}

func (m *checkpointMemory) normalize(ss raftpb.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.normalized = ss
}

// NewRecoveryContext restores under the accepted startup owner's lifetime, with
// no ordinary five-second restore cutoff and no fresh-founding fallback.
func NewRecoveryContext(ctx context.Context, opts Options, key model.SeedKey) (*Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.validateMode(loadEngine); err != nil {
		return nil, err
	}
	options := ownGroupOptions(opts.defaults())
	identity := storage.Identity{ClusterID: options.ClusterID, NodeName: options.NodeName,
		GroupID: options.GroupID, ReplicaID: options.ReplicaID}
	if identity != key.Destination || options.MemberID != key.MemberID || key.RecoveryID == "" ||
		key.PlanDigest == "" || !options.ManagedAuthority || options.InitialAuthority != nil ||
		options.Mode != Joining {
		return nil, fault.ErrOperationAuthorization
	}
	if _, ok := options.Store.(recoveryCheckpointStore); !ok {
		return nil, fault.ErrUnsupported
	}
	state, err := loadGroupRecovery(ctx, options)
	if err != nil {
		return nil, err
	}
	if state.recovery.Snapshot.Metadata.Index == 0 {
		return nil, fault.ErrUnreadable
	}
	g, err := newRestoredCore(ctx, options, state)
	if err == nil {
		owned := key
		g.recoveryKey = &owned
	}
	return g, err
}

// RecoveryCheckpoint serializes normalization with application and writer work.
// Its caller's cancellation ends only the wait after acceptance by the engine.
func (g *Group) RecoveryCheckpoint(ctx context.Context, key model.SeedKey) (model.CheckpointProof, error) {
	if g.recoveryKey == nil || *g.recoveryKey != key {
		return model.CheckpointProof{}, fault.ErrOperationAuthorization
	}
	result, err := g.submit(ctx, g.snapshots, &request{ctx: ctx, recoveryCheckpoint: &key,
		reply: make(chan response, 1)})
	if err != nil {
		return model.CheckpointProof{}, err
	}
	var proof model.CheckpointProof
	err = json.Unmarshal(result.Data, &proof)
	return proof, err
}

// FinishRecovery restores ordinary durability at the same joined writer boundary.
func (g *Group) FinishRecovery(ctx context.Context, key model.SeedKey, proof model.Consumed) error {
	if g.recoveryKey == nil || *g.recoveryKey != key {
		return fault.ErrOperationAuthorization
	}
	proof.HostReceipt = proof.HostReceipt.Clone()
	proof.GroupReceipt = proof.GroupReceipt.Clone()
	_, err := g.submit(ctx, g.snapshots, &request{ctx: ctx, recoveryCheckpoint: &key,
		consumed: &proof, reply: make(chan response, 1)})
	return err
}

func (g *Group) beginRecoveryCheckpoint(req *request) error {
	if req.ctx.Err() != nil {
		return req.ctx.Err()
	}
	if g.recoveryKey == nil || req.recoveryCheckpoint == nil || *g.recoveryKey != *req.recoveryCheckpoint {
		return fault.ErrOperationAuthorization
	}
	if _, ok := g.opts.Store.(recoveryCheckpointStore); !ok {
		return fault.ErrUnsupported
	}
	job := writeJob{kind: writeRecoveryCheckpoint, request: req}
	if req.consumed != nil {
		job.kind = writeRecoveryFinish
	} else {
		if g.applied < g.recoveryCommit {
			return fault.ErrReplicaNotReady
		}
		data, err := g.opts.Machine.Snapshot(g.ctx)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return fault.ErrInvalid
		}
		data, err = encodeSnapshot(g.opts, data, g.authority)
		if err != nil {
			return err
		}
		term, err := g.ms.Term(g.applied)
		if err != nil {
			return err
		}
		job.snapshot = raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: g.applied, Term: term,
			ConfState: copyConf(g.cs)}}
		job.proof = new(model.CheckpointProof)
	}
	g.pending = &job
	g.writeC <- job
	return nil
}

func (g *Group) persistRecoveryCheckpoint(ctx context.Context, job writeJob) error {
	store, ok := g.opts.Store.(recoveryCheckpointStore)
	if !ok {
		return fault.ErrUnsupported
	}
	key := *job.request.recoveryCheckpoint
	if job.kind == writeRecoveryFinish {
		return store.FinishRecovery(ctx, key, *job.request.consumed)
	}
	if err := store.SaveRecoveryCheckpoint(ctx, key, job.snapshot); err != nil {
		return err
	}
	proof, err := store.VerifyRecoveryCheckpoint(ctx, key, model.Position{Index: job.snapshot.Metadata.Index,
		Term: job.snapshot.Metadata.Term})
	if err == nil {
		*job.proof = proof
	}
	return err
}

func (g *Group) finishRecoveryCheckpoint() error {
	job := g.pending
	if job.kind == writeRecoveryFinish {
		g.pending = nil
		answer(job.request, Result{}, nil)
		return nil
	}
	if job.snapshot.Metadata.Index > g.snapshotIndex {
		cs := copyConf(job.snapshot.Metadata.ConfState)
		if _, err := g.ms.CreateSnapshot(job.snapshot.Metadata.Index, &cs, job.snapshot.Data); err != nil {
			return err
		}
		if err := g.ms.Compact(job.snapshot.Metadata.Index); err != nil {
			return err
		}
	} else {
		g.checkpointMemory.normalize(job.snapshot)
	}
	g.snapshotIndex = job.snapshot.Metadata.Index
	data, err := json.Marshal(*job.proof)
	if err != nil {
		return err
	}
	g.pending = nil
	answer(job.request, Result{Data: data}, nil)
	return nil
}
