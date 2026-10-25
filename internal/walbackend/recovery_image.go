package walbackend

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.uber.org/zap"
)

func (s *Store) writeSeedImage(ctx context.Context, stage string, seed model.SeedSpec,
	retained storage.Recovery) (model.WALEvidence, error) {
	id := seed.Key.Destination
	dir := filepath.Join(s.opts.RootDir, stage)
	if err := os.Mkdir(filepath.Join(dir, SnapDir), dirMode); err != nil {
		return model.WALEvidence{}, err
	}
	state, err := openState(filepath.Join(dir, StateFile), false)
	if err != nil {
		return model.WALEvidence{}, err
	}
	defer state.Close()
	if err := writeIdentity(dir, id); err != nil {
		return model.WALEvidence{}, err
	}
	metadata, err := json.Marshal(diskIdentity{SchemaVersion: 1, Identity: id})
	if err != nil {
		return model.WALEvidence{}, err
	}
	w, err := wal.Create(zap.NewNop(), filepath.Join(dir, WALDir), metadata)
	if err != nil {
		return model.WALEvidence{}, err
	}
	g := &group{dir: dir, state: state, w: w, ss: newSnapshotter(dir), identity: id}
	if err := persistSeedImage(g, retained); err != nil {
		return model.WALEvidence{}, err
	}

	if err := state.Sync(); err != nil {
		return model.WALEvidence{}, err
	}
	if err := state.Close(); err != nil {
		return model.WALEvidence{}, err
	}
	syncErr := errors.Join(syncDir(filepath.Join(dir, WALDir)), syncDir(filepath.Join(dir, SnapDir)), syncDir(dir))
	if syncErr != nil {
		return model.WALEvidence{}, syncErr
	}
	return ReadRecovery(ctx, filepath.Dir(dir), id.GroupID,
		model.ReadBounds{MaxRecordBytes: seed.Resources.MaxRecordBytes, MaxGroupBytes: seed.Resources.MaxGroupBytes})
}

func persistSeedImage(g *group, retained storage.Recovery) error {
	if err := persistSnapshotFile(g, retained.Snapshot); err != nil {
		return joinClose(err, g.w)
	}
	if err := persistSnapshotCommit(g, retained.Snapshot); err != nil {
		return joinClose(err, g.w)
	}
	if err := g.w.Save(retained.HardState, retained.Entries); err != nil {
		return joinClose(err, g.w)
	}
	if err := g.w.Sync(); err != nil {
		return joinClose(err, g.w)
	}
	if err := g.w.Close(); err != nil {
		return err
	}
	cs, err := retained.AppliedConfState.Marshal()
	if err != nil {
		return err
	}
	err = g.state.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if err := putU64(b, keySnapmark, retained.Snapshot.Metadata.Index); err != nil {
			return err
		}
		if err := putU64(b, keyApplied, retained.AppliedHint); err != nil {
			return err
		}
		return b.Put(keyConfState, cs)
	})
	if err != nil {
		return err
	}
	return nil
}
