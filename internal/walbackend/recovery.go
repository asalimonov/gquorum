package walbackend

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

// RecoveryRoot reports the already owned WAL root without acquiring resources.
func RecoveryRoot(s *Store) string { return s.opts.RootDir }

func validateSeedKey(s *Store, key model.SeedKey) error {
	id := key.Destination
	if key.RecoveryID == "" || key.PlanDigest == "" || key.MemberID == 0 || id.ReplicaID == 0 ||
		id.ClusterID != s.opts.ClusterID || id.NodeName != s.opts.NodeName {
		return fault.ErrIdentityMismatch
	}
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return err
	}
	return model.ValidateRequest(model.Request{RecoveryID: key.RecoveryID, Mode: model.Rebuild,
		ClusterID: id.ClusterID, Actor: "recovery", Comment: "seed validation"})
}

// ProtectRecovery forces safe durability before acquiring an exact group writer.
func ProtectRecovery(ctx context.Context, s *Store, key model.SeedKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateSeedKey(s, key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fault.ErrStopped
	}
	if old, ok := s.protection[key.Destination.GroupID]; ok {
		if old != key {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	if s.groups[key.Destination.GroupID] != nil {
		return fault.New(fault.OperationAuthorization, "recovery protection must precede writer acquisition", nil)
	}
	s.protection[key.Destination.GroupID] = key
	return nil
}

func requireProtection(s *Store, key model.SeedKey) error {
	if err := validateSeedKey(s, key); err != nil {
		return err
	}
	if s.closed {
		return fault.ErrStopped
	}
	if s.protection[key.Destination.GroupID] != key {
		return fault.ErrOperationAuthorization
	}
	return s.rootLease.CheckPath(s.opts.RootDir)
}

// FinishRecovery releases only this exact group's protection after consumption.
// The engine invokes it at a serialized writer boundary.
func FinishRecovery(ctx context.Context, s *Store, key model.SeedKey, proof model.Consumed) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if proof.Identity != key.Destination || proof.Checkpoint.Index == 0 || proof.SnapshotDigest == "" {
		return fault.ErrIdentityMismatch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if saved, ok := s.finished[key]; ok {
		if reflect.DeepEqual(saved, proof) {
			return nil
		}
		return fault.ErrIdentityMismatch
	}
	if err := requireProtection(s, key); err != nil {
		return err
	}
	g := s.groups[key.Destination.GroupID]
	if g == nil {
		return fault.ErrUnavailable
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return fault.ErrStopped
	}
	if g.checkpoint == nil || g.checkpoint.Position != proof.Checkpoint {
		return fault.ErrOperationAuthorization
	}
	actual, err := checkpointProof(g, proof.Checkpoint)
	if err != nil {
		return err
	}
	if actual.Digest != proof.SnapshotDigest {
		return fault.ErrIdentityMismatch
	}
	if err := validateConsumedCheckpoint(g, proof); err != nil {
		return err
	}
	if s.opts.DisableFSync {
		g.state.NoSync = true
		g.w.SetUnsafeNoFsync()
	}
	g.checkpoint = nil
	proof.HostReceipt = proof.HostReceipt.Clone()
	proof.GroupReceipt = proof.GroupReceipt.Clone()
	s.finished[key] = proof
	delete(s.protection, key.Destination.GroupID)
	return nil
}

func seedControlPath(name, group string) string {
	return filepath.Join("@gquorum-recovery", name, "seed-state", group+".json")
}

func joinClose(err error, closer interface{ Close() error }) error {
	return errors.Join(err, closer.Close())
}

func validateConsumedCheckpoint(g *group, proof model.Consumed) error {
	if proof.GroupReceipt.Key.GroupID != g.identity.GroupID ||
		proof.GroupReceipt.Key.ClusterID != g.identity.ClusterID ||
		proof.GroupReceipt.AppliedIndex > proof.Checkpoint.Index ||
		proof.GroupReceipt.AppliedTerm > proof.Checkpoint.Term ||
		proof.GroupReceipt.Key != proof.HostReceipt.Key ||
		proof.HostReceipt.Resource.Kind != topology.AuthorityHost ||
		proof.GroupReceipt.Resource.Kind != topology.AuthorityGroup {
		return fault.ErrIdentityMismatch
	}
	name := fmt.Sprintf("%016x-%016x.snap", proof.Checkpoint.Term, proof.Checkpoint.Index)
	snapshot, err := snap.Read(zap.NewNop(), filepath.Join(g.dir, SnapDir, name))
	if err != nil {
		return err
	}
	decoded, err := raftstate.DecodeRaftSnapshot(*snapshot, g.identity.ClusterID, g.identity.GroupID)
	if err != nil {
		return err
	}
	if proof.HostReceipt.Resource.NodeName != g.identity.NodeName ||
		decoded.Authority.Grant == nil || decoded.Authority.Receipt == nil ||
		!reflect.DeepEqual(*decoded.Authority.Receipt, proof.GroupReceipt) {
		return fault.ErrIdentityMismatch
	}
	return proof.HostReceipt.Validate(*decoded.Authority.Grant)
}
