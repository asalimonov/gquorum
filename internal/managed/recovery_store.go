package managed

import (
	"context"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/internal/walbackend"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

type recoveryStore interface {
	storage.Store
	RecoveryRoot() string
	ProtectRecovery(context.Context, model.SeedKey) error
	InstallRecoverySeed(context.Context, model.SeedSpec) (model.SeedMarker, error)
	ReadRecoverySeed(context.Context, model.SeedKey) (model.SeedMarker, bool, error)
	VerifyRecoveryCheckpoint(context.Context, model.SeedKey, model.Position) (model.CheckpointProof, error)
	SaveRecoveryCheckpoint(context.Context, model.SeedKey, raftpb.Snapshot) error
	FinishRecovery(context.Context, model.SeedKey, model.Consumed) error
}

type recoveryBackend struct{ *walbackend.Store }

// RecoveryStore wraps only a Node-owned WAL backend, preserving its normal Store API.
func RecoveryStore(store *walbackend.Store) storage.Store { return &recoveryBackend{Store: store} }
func (s *recoveryBackend) RecoveryRoot() string           { return walbackend.RecoveryRoot(s.Store) }
func (s *recoveryBackend) ProtectRecovery(ctx context.Context, key model.SeedKey) error {
	return walbackend.ProtectRecovery(ctx, s.Store, key)
}
func (s *recoveryBackend) InstallRecoverySeed(ctx context.Context, seed model.SeedSpec) (model.SeedMarker, error) {
	return walbackend.InstallRecoverySeed(ctx, s.Store, seed, func(ctx context.Context,
		retained storage.Recovery) (model.Position, error) {
		result, err := raftengine.Replay(ctx, raftengine.ReplayOptions{Identity: seed.Source.Identity,
			MemberID: seed.Source.MemberID, InstanceID: seed.Key.RecoveryID}, retained, raftengine.DiscardMachine{})
		return model.Position{Index: result.Index, Term: result.Term}, err
	})
}

//revive:disable-next-line:function-result-limit Exact marker lookup keeps absence separate from a refusal.
func (s *recoveryBackend) ReadRecoverySeed(ctx context.Context, key model.SeedKey) (model.SeedMarker, bool, error) {
	return walbackend.ReadRecoverySeed(ctx, s.Store, key)
}
func (s *recoveryBackend) VerifyRecoveryCheckpoint(ctx context.Context, key model.SeedKey,
	point model.Position) (model.CheckpointProof, error) {
	return walbackend.VerifyRecoveryCheckpoint(ctx, s.Store, key, point)
}
func (s *recoveryBackend) SaveRecoveryCheckpoint(ctx context.Context, key model.SeedKey, ss raftpb.Snapshot) error {
	return walbackend.SaveRecoveryCheckpoint(ctx, s.Store, key, ss)
}
func (s *recoveryBackend) FinishRecovery(ctx context.Context, key model.SeedKey, proof model.Consumed) error {
	return walbackend.FinishRecovery(ctx, s.Store, key, proof)
}

var _ recoveryStore = (*recoveryBackend)(nil)
