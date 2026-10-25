package walbackend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap/snappb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// SaveRecoveryCheckpoint synchronously replaces a complete normalized snapshot,
// including at the initial seed index. Ordinary SaveSnapshot keeps its old rules.
func SaveRecoveryCheckpoint(ctx context.Context, s *Store, key model.SeedKey, ss raftpb.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	err := requireProtection(s, key)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	g, err := s.openGroup(key.Destination.GroupID)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return fault.ErrStopped
	}
	if err := saveCheckpointImage(g, ss); err != nil {
		return err
	}
	proof, err := checkpointProof(g, model.Position{Index: ss.Metadata.Index, Term: ss.Metadata.Term})
	if err != nil {
		return err
	}
	if err := s.retainRecoveryCheckpoint(ctx, key, proof); err != nil {
		return err
	}
	g.checkpoint = &proof
	return nil
}

func atomicRecoverySnapshot(g *group, ss raftpb.Snapshot) error {
	raw, err := ss.Marshal()
	if err != nil {
		return err
	}
	wrapped := snappb.Snapshot{Crc: crc32.Checksum(raw, crc32.MakeTable(crc32.Castagnoli)), Data: raw}
	data, err := wrapped.Marshal()
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(g.dir, SnapDir))
	if err != nil {
		return err
	}
	defer root.Close()
	name := fmt.Sprintf("%016x-%016x.snap", ss.Metadata.Term, ss.Metadata.Index)
	return recoverydisk.AtomicWrite(root, name, data, true)
}

// VerifyRecoveryCheckpoint returns the actual durable full-snapshot digest.
func VerifyRecoveryCheckpoint(ctx context.Context, s *Store, key model.SeedKey,
	point model.Position) (model.CheckpointProof, error) {
	if err := ctx.Err(); err != nil {
		return model.CheckpointProof{}, err
	}
	s.mu.Lock()
	err := requireProtection(s, key)
	s.mu.Unlock()
	if err != nil {
		return model.CheckpointProof{}, err
	}
	g, err := s.openGroup(key.Destination.GroupID)
	if err != nil {
		return model.CheckpointProof{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return model.CheckpointProof{}, fault.ErrStopped
	}
	if g.checkpoint == nil || g.checkpoint.Position != point {
		return model.CheckpointProof{}, fault.ErrOperationAuthorization
	}
	proof, err := checkpointProof(g, point)
	if err == nil && proof != *g.checkpoint {
		return model.CheckpointProof{}, fault.ErrIdentityMismatch
	}
	return proof, err
}

func checkpointProof(g *group, point model.Position) (model.CheckpointProof, error) {
	if point.Index == 0 || point.Term == 0 || point.Index > g.hs.Commit {
		return model.CheckpointProof{}, fault.ErrIdentityMismatch
	}
	matched := false
	for _, marker := range g.markers {
		if marker.Index == point.Index && marker.Term == point.Term {
			matched = true
			break
		}
	}
	if !matched {
		return model.CheckpointProof{}, fault.ErrIdentityMismatch
	}
	name := fmt.Sprintf("%016x-%016x.snap", point.Term, point.Index)
	ss, err := snap.Read(zap.NewNop(), filepath.Join(g.dir, SnapDir, name))
	if err != nil {
		return model.CheckpointProof{}, err
	}
	if _, err := raftstate.DecodeRaftSnapshot(*ss, g.identity.ClusterID, g.identity.GroupID); err != nil {
		return model.CheckpointProof{}, err
	}
	raw, err := ss.Marshal()
	if err != nil {
		return model.CheckpointProof{}, err
	}
	digest := sha256.Sum256(raw)
	return model.CheckpointProof{Identity: g.identity, Position: point, Digest: hex.EncodeToString(digest[:])}, nil
}

func (s *Store) retainRecoveryCheckpoint(ctx context.Context, key model.SeedKey,
	proof model.CheckpointProof) error {
	found, err := s.findSeedControl(ctx, key)
	if err != nil {
		return err
	}
	if found == nil {
		return fault.ErrOperationAuthorization
	}
	root, err := s.rootLease.OpenRecoveryRoot(found.name)
	if err != nil {
		return err
	}
	defer root.Close()
	found.control.Checkpoint = &proof
	return writeSeedControl(root, found.name, found.control, true)
}

func saveCheckpointImage(g *group, ss raftpb.Snapshot) error {
	if ss.Metadata.Index > g.hs.Commit || ss.Metadata.Term > g.hs.Term {
		return fault.ErrInvalid
	}
	if _, err := raftstate.DecodeRaftSnapshot(ss, g.identity.ClusterID, g.identity.GroupID); err != nil {
		return err
	}
	var previous uint64
	viewErr := g.state.View(func(tx *bolt.Tx) error {
		previous = getU64(tx.Bucket(bucketMeta),
			keySnapmark)
		return nil
	})
	if viewErr != nil {
		return viewErr
	}
	if ss.Metadata.Index < previous {
		return fault.ErrInvalid
	}
	if err := atomicRecoverySnapshot(g, ss); err != nil {
		return err
	}
	if ss.Metadata.Index > previous {
		if err := persistSnapshotCommit(g, ss); err != nil {
			return err
		}
	} else {
		if err := g.w.Sync(); err != nil {
			return err
		}
	}
	updateErr := g.state.Update(func(tx *bolt.Tx) error {
		return putU64(tx.Bucket(bucketMeta), keySnapmark,
			ss.Metadata.Index)
	})
	if updateErr != nil {
		return updateErr
	}
	return g.state.Sync()
}
