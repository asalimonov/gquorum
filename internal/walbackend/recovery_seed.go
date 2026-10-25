package walbackend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
)

// SeedReplay is supplied by the library adapter, never an application extension.
type SeedReplay func(context.Context, storage.Recovery) (model.Position, error)

const seedMarkerFile = "@recovery-seed.json"
const (
	seedBuilding  = "building"
	seedPrepared  = "prepared"
	seedInstalled = "installed"
	seedAcquired  = "acquired"
	seedConsumed  = "consumed"
)

type seedControl struct {
	Schema     uint32                 `json:"schema"`
	State      string                 `json:"state"`
	Marker     model.SeedMarker       `json:"marker"`
	Checkpoint *model.CheckpointProof `json:"checkpoint,omitempty"`
}

type seedLocation struct {
	name    string
	control seedControl
}

// InstallRecoverySeed publishes one complete image without ever replacing live data.
// A different retry grant is handled later by the ordinary engine, never by reseeding.
func InstallRecoverySeed(ctx context.Context, s *Store, seed model.SeedSpec,
	replay SeedReplay) (model.SeedMarker, error) {
	if err := ctx.Err(); err != nil {
		return model.SeedMarker{}, err
	}
	if err := validateSeedSpec(s, seed); err != nil {
		return model.SeedMarker{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requireProtection(s, seed.Key); err != nil {
		return model.SeedMarker{}, err
	}
	root, err := s.rootLease.OpenRecoveryRoot(seed.BackupName)
	if err != nil {
		return model.SeedMarker{}, err
	}
	defer root.Close()
	found, err := s.findSeedControl(ctx, seed.Key)
	if err != nil {
		return model.SeedMarker{}, err
	}
	if found != nil {
		if found.name != seed.BackupName || found.control.Marker.Source != seed.Source ||
			!found.control.Marker.Grant.Equal(seed.Grant) {
			// Only the managed authority catalog can authorize a retry; installation is exact.
			return model.SeedMarker{}, fault.ErrOperationAuthorization
		}
		if found.control.State == seedBuilding {
			return s.prepareRecoverySeed(ctx, root, seed, replay)
		}
		return s.resumeSeed(ctx, root, *found)
	}
	groupID := seed.Key.Destination.GroupID
	if _, err := root.Lstat(groupID); !errors.Is(err, os.ErrNotExist) {
		return model.SeedMarker{}, fault.New(fault.Unreadable, "unproven live recovery destination", err)
	}
	if s.groups[groupID] != nil {
		return model.SeedMarker{}, fault.ErrOperationAuthorization
	}
	return s.prepareRecoverySeed(ctx, root, seed, replay)
}

func (s *Store) prepareRecoverySeed(ctx context.Context, root *os.Root, seed model.SeedSpec,
	replay SeedReplay) (model.SeedMarker, error) {
	groupID := seed.Key.Destination.GroupID
	base := filepath.Join(recoverydisk.Namespace, seed.BackupName)
	sourceDir := filepath.Join(base, "backup", seed.Source.GroupID)
	if err := s.rootLease.ValidateRecoveryArtifact(ctx, sourceDir, model.ManifestMaxBytes); err != nil {
		return model.SeedMarker{}, err
	}
	limits, err := model.ResolveResources(seed.Resources)
	if err != nil {
		return model.SeedMarker{}, err
	}
	evidence, err := ReadRecovery(ctx, filepath.Join(s.opts.RootDir, base, "backup"), seed.Source.GroupID,
		model.ReadBounds{MaxRecordBytes: limits.MaxRecordBytes, MaxGroupBytes: limits.MaxGroupBytes})
	if err != nil {
		return model.SeedMarker{}, err
	}
	retained, err := transformRecoverySeed(ctx, seed, evidence, replay)
	if err != nil {
		return model.SeedMarker{}, err
	}
	stage := filepath.Join(base, "seed", groupID)
	if err := s.beginSeedBuild(ctx, root, seed); err != nil {
		return model.SeedMarker{}, err
	}
	if err := s.prepareSeedStage(root, stage, base); err != nil {
		return model.SeedMarker{}, err
	}
	image, err := s.writeSeedImage(ctx, stage, seed, retained)
	if err != nil {
		return model.SeedMarker{}, err
	}
	marker := seedMarker(seed, image.Digest)
	data, err := model.EncodeCanonical(marker)
	if err != nil {
		return model.SeedMarker{}, err
	}
	if err := recoverydisk.AtomicWrite(root, filepath.Join(stage, seedMarkerFile), data, false); err != nil {
		return model.SeedMarker{}, err
	}
	control := seedControl{Schema: model.SchemaVersion, State: seedPrepared, Marker: marker}
	if err := s.seedBoundary("prepared"); err != nil {
		return model.SeedMarker{}, err
	}
	if err := writeSeedControl(root, seed.BackupName, control, true); err != nil {
		return model.SeedMarker{}, err
	}
	return s.resumeSeed(ctx, root, seedLocation{name: seed.BackupName, control: control})
}

func (s *Store) prepareSeedStage(root *os.Root, stage, base string) error {
	if _, err := root.Lstat(stage); err == nil {
		validateErr := s.rootLease.ValidateRecoveryArtifact(context.Background(), stage, model.ManifestMaxBytes)
		if validateErr != nil {
			return validateErr
		}
		abandoned := filepath.Join(base, "abandoned", fmt.Sprintf("%d", time.Now().UnixNano()))
		if err := recoverydisk.EnsureRecoveryDirectory(root, abandoned); err != nil {
			return err
		}
		target := filepath.Join(abandoned, filepath.Base(stage))
		if err := recoverydisk.RenameNoReplace(root, stage, target); err != nil {
			return err
		}
		if err := recoverydisk.SyncParents(root, stage, target); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return recoverydisk.EnsureRecoveryDirectory(root, stage)
}

func writeSeedControl(root *os.Root, name string, control seedControl, replace bool) error {
	path := seedControlPath(name, control.Marker.Destination.GroupID)
	if err := recoverydisk.EnsureRecoveryDirectory(root, filepath.Dir(path)); err != nil {
		return err
	}
	data, err := model.EncodeCanonical(control)
	if err != nil {
		return err
	}
	return recoverydisk.AtomicWrite(root, path, data, replace)
}

func (s *Store) resumeSeed(ctx context.Context, root *os.Root, found seedLocation) (model.SeedMarker, error) {
	marker := found.control.Marker
	live := marker.Destination.GroupID
	_, err := root.Lstat(live)
	if err == nil {
		err = s.reconcilePublishedSeed(ctx, root, found)
	} else if errors.Is(err, os.ErrNotExist) {
		err = s.publishPreparedSeed(ctx, root, found)
	}
	if err != nil {
		return model.SeedMarker{}, err
	}
	if found.control.State == seedPrepared {
		if err := s.seedBoundary("installed"); err != nil {
			return model.SeedMarker{}, err
		}
		found.control.State = seedInstalled
		if err := writeSeedControl(root, found.name, found.control, true); err != nil {
			return model.SeedMarker{}, err
		}
	}
	return marker, nil
}

func seedStage(found seedLocation) string {
	return filepath.Join(recoverydisk.Namespace, found.name, "seed", found.control.Marker.Destination.GroupID)
}

func (s *Store) reconcilePublishedSeed(ctx context.Context, root *os.Root, found seedLocation) error {
	marker := found.control.Marker
	stage := seedStage(found)
	if _, err := root.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
		return fault.New(fault.Conflict, "staged and live seed conflict", err)
	}
	if err := s.verifySeedLive(ctx, marker); err != nil {
		return err
	}
	if found.control.State != seedAcquired && found.control.State != seedConsumed {
		if err := s.verifySeedImage(ctx, marker.Destination.GroupID, marker); err != nil {
			return err
		}
	}
	if err := s.seedBoundary("parent_sync"); err != nil {
		return err
	}
	return recoverydisk.SyncParents(root, stage, marker.Destination.GroupID)
}

func (s *Store) publishPreparedSeed(ctx context.Context, root *os.Root, found seedLocation) error {
	if found.control.State != seedPrepared {
		return fault.New(fault.Unreadable, "installed recovery identity has lost its store", nil)
	}
	stage := seedStage(found)
	if _, err := root.Lstat(stage); err != nil {
		return fault.New(fault.Unreadable, "prepared recovery image is missing", err)
	}
	if err := s.verifySeedImage(ctx, stage, found.control.Marker); err != nil {
		return err
	}
	if err := s.seedBoundary("rename"); err != nil {
		return err
	}
	live := found.control.Marker.Destination.GroupID
	if err := recoverydisk.RenameNoReplace(root, stage, live); err != nil {
		return err
	}
	if err := s.seedBoundary("parent_sync"); err != nil {
		return err
	}
	return recoverydisk.SyncParents(root, stage, live)
}

func (s *Store) verifySeedLive(ctx context.Context, marker model.SeedMarker) error {
	relative := marker.Destination.GroupID
	if err := s.rootLease.ValidateRecoveryArtifact(ctx, relative, model.ManifestMaxBytes); err != nil {
		return err
	}
	var actual model.SeedMarker
	data, err := readBoundedFile(ctx, filepath.Join(s.opts.RootDir, relative, seedMarkerFile),
		model.ManifestMaxBytes)
	if err != nil {
		return err
	}
	if err := model.DecodeStrict(data, &actual, model.ManifestMaxBytes); err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, marker) {
		return fault.ErrIdentityMismatch
	}
	// Once acquired, ordinary retained recovery validates the evolving B store.
	pin, err := readIdentity(filepath.Join(s.opts.RootDir, relative))
	if err != nil {
		return err
	}
	if !pin.Found || pin.Identity != marker.Destination {
		return fault.ErrIdentityMismatch
	}
	for _, name := range []string{StateFile, WALDir, SnapDir} {
		if _, err := os.Lstat(filepath.Join(s.opts.RootDir, relative, name)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) verifySeedImage(ctx context.Context, relative string, marker model.SeedMarker) error {
	if err := s.rootLease.ValidateRecoveryArtifact(ctx, relative, model.ManifestMaxBytes); err != nil {
		return err
	}
	limits, err := s.seedReadBounds(marker.RecoveryID)
	if err != nil {
		return err
	}
	evidence, err := ReadRecovery(ctx, filepath.Join(s.opts.RootDir, filepath.Dir(relative)),
		filepath.Base(relative), limits)
	if err != nil {
		return err
	}
	if evidence.Identity != marker.Destination || evidence.Digest != marker.ImageDigest {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func (s *Store) findSeedControl(ctx context.Context, key model.SeedKey) (*seedLocation, error) {
	names, err := s.rootLease.RecoveryNames()
	if err != nil {
		return nil, err
	}
	var result *seedLocation
	for _, name := range names {
		path := seedControlPath(name, key.Destination.GroupID)
		data, err := readBoundedFile(ctx, filepath.Join(s.opts.RootDir, path), model.ManifestMaxBytes)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		control, err := decodeSeedControl(data)
		if err != nil {
			return nil, err
		}
		m := control.Marker
		if m.RecoveryID != key.RecoveryID {
			continue
		}
		if m.PlanDigest != key.PlanDigest || m.Destination != key.Destination || m.MemberID != key.MemberID {
			return nil, fault.ErrIdentityMismatch
		}
		if result != nil {
			return nil, fault.ErrConflict
		}
		result = &seedLocation{name: name, control: control}
	}
	return result, nil
}

// ReadRecoverySeed reconciles immutable provenance, not an evolving live checksum.
//
//revive:disable-next-line:function-result-limit Exact marker lookup keeps absence separate from a refusal.
func ReadRecoverySeed(ctx context.Context, s *Store, key model.SeedKey) (model.SeedMarker, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := requireProtection(s, key); err != nil {
		return model.SeedMarker{}, false, err
	}
	found, err := s.findSeedControl(ctx, key)
	if err != nil || found == nil {
		return model.SeedMarker{}, false, err
	}
	if found.control.State == seedBuilding {
		_, err := os.Lstat(filepath.Join(s.opts.RootDir, key.Destination.GroupID))
		if !errors.Is(err, os.ErrNotExist) {
			return model.SeedMarker{}, false, fault.New(fault.Unreadable, "unpublished seed has live data", err)
		}
		return found.control.Marker, false, nil
	}
	root, err := s.rootLease.OpenRecoveryRoot(found.name)
	if err != nil {
		return model.SeedMarker{}, false, err
	}
	defer root.Close()
	marker, err := s.resumeSeed(ctx, root, *found)
	return marker, err == nil, err
}

func (s *Store) seedBoundary(point string) error {
	if s.recoveryBoundary != nil {
		return s.recoveryBoundary(point)
	}
	return nil
}

func validateSeedControl(control seedControl) error {
	switch control.State {
	case seedBuilding, seedPrepared, seedInstalled, seedAcquired, seedConsumed:
	default:
		return fault.ErrUnreadable
	}
	if control.Schema != model.SchemaVersion {
		return fault.ErrUnsupported
	}
	if err := model.ValidateSeedMarker(control.Marker); err != nil {
		return err
	}
	return validateSeedCheckpoint(control)
}

func decodeSeedControl(data []byte) (seedControl, error) {
	var control seedControl
	if err := model.DecodeStrict(data, &control, model.ManifestMaxBytes); err != nil {
		return seedControl{}, err
	}
	return control, validateSeedControl(control)
}
func (s *Store) seedReadBounds(id string) (model.ReadBounds, error) {
	names, err := s.rootLease.RecoveryNames()
	if err != nil {
		return model.ReadBounds{}, err
	}
	for _, name := range names {
		manifest, err := s.rootLease.RecoveryManifest(name)
		if err != nil {
			return model.ReadBounds{}, err
		}
		if manifest.Request.RecoveryID == id {
			return model.ReadBounds{MaxRecordBytes: manifest.Resources.MaxRecordBytes,
				MaxGroupBytes: manifest.Resources.MaxGroupBytes}, nil
		}
	}
	return model.ReadBounds{}, fault.ErrOperationAuthorization
}

func validateSeedCheckpoint(control seedControl) error {
	point := control.Checkpoint
	if point == nil {
		return nil
	}
	if control.State != seedAcquired && control.State != seedConsumed {
		return fault.ErrUnreadable
	}
	source := control.Marker.Source
	if point.Identity != control.Marker.Destination || point.Position.Index < source.Commit.Index ||
		point.Position.Term < source.Commit.Term || len(point.Digest) != sha256.Size*2 {
		return fault.ErrIdentityMismatch
	}
	if _, err := hex.DecodeString(point.Digest); err != nil {
		return fault.ErrUnreadable
	}
	return nil
}

func (s *Store) beginSeedBuild(ctx context.Context, root *os.Root, seed model.SeedSpec) error {
	found, err := s.findSeedControl(ctx, seed.Key)
	if err != nil {
		return err
	}
	if found != nil {
		if found.control.State != seedBuilding {
			return fault.ErrOperationAuthorization
		}
		return nil
	}
	stage := filepath.Join(recoverydisk.Namespace, seed.BackupName, "seed", seed.Key.Destination.GroupID)
	if _, err := root.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
		return fault.New(fault.Unreadable, "unproven recovery staging directory", err)
	}
	const unfinishedDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	control := seedControl{Schema: model.SchemaVersion, State: seedBuilding, Marker: seedMarker(seed, unfinishedDigest)}
	return writeSeedControl(root, seed.BackupName, control, false)
}
