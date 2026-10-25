package recoveryinspect

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// SourceState owns source readers and reconstructed metadata for one leased call.
// It contains no caller application, live engine or background work.
type SourceState struct {
	owner           *recoverydisk.Owner
	paths           model.Paths
	resources       model.Resources
	identity        topology.Identity
	metadata        storage.Recovery
	ledger          map[string]metastore.ReplicaRecord
	authority       model.AuthorityEvidence
	inventory       metaengine.RecoveryInventory
	checkpoint      []byte
	metadataDigest  string
	authorityDigest string
	local           []localArtifact
	metaReader      *metastore.Reader
	authorityReader *authoritystore.Reader
}

func readSource(ctx context.Context, paths model.Paths, resources model.Resources) (SourceState, error) {
	state := SourceState{paths: paths, resources: resources}
	if err := state.readMetadata(ctx); err != nil {
		return state, errors.Join(err, state.close())
	}
	if err := state.readAuthority(ctx); err != nil {
		return state, errors.Join(err, state.close())
	}
	local, _, err := ClassifyReplicas(ctx, state)
	if err != nil {
		return state, errors.Join(err, state.close())
	}
	state.inventory.Replicas = local
	return state, nil
}

func (s *SourceState) readMetadata(ctx context.Context) error {
	reader, err := metastore.OpenReadOnly(ctx, filepath.Join(s.paths.StorageDir, "_meta.bolt"),
		s.resources.MaxMetadataBytes)
	if err != nil {
		return err
	}
	s.metaReader = reader
	pin, err := reader.Identity()
	if err != nil {
		return err
	}
	s.identity = topology.Identity{NodeName: pin.NodeName, ClusterID: pin.ClusterID, MemberID: pin.MemberID}
	retained, ledger, err := reader.Recovery(ctx)
	if err != nil {
		return err
	}
	s.metadata, s.ledger = retained, ledger
	snapshot, err := raftstate.DecodeRaftSnapshot(retained.Snapshot, pin.ClusterID, topology.MetaGroupID)
	if err != nil {
		return err
	}
	var header struct {
		Rules topology.Rules `json:"rules"`
	}
	if err := json.Unmarshal(snapshot.Application, &header); err != nil {
		return err
	}
	machine, err := metaengine.NewMachine(header.Rules, nil)
	if err != nil {
		return err
	}
	_, err = raftengine.Replay(ctx, raftengine.ReplayOptions{
		Identity: storage.Identity{NodeName: pin.NodeName, ClusterID: pin.ClusterID,
			GroupID: topology.MetaGroupID, ReplicaID: pin.MemberID},
		MemberID: pin.MemberID, InstanceID: "offline-inspection",
	}, retained, machine)
	if err != nil {
		return err
	}
	s.inventory, err = machine.RecoveryInventory(ctx)
	if err != nil {
		return err
	}
	if s.inventory.Approved.ClusterID != pin.ClusterID {
		return fault.ErrIdentityMismatch
	}
	s.checkpoint, err = machine.Snapshot(ctx)
	if err != nil {
		return err
	}
	s.metadataDigest, err = reader.Digest(ctx)
	return err
}

func (s *SourceState) readAuthority(ctx context.Context) error {
	reader, err := authoritystore.OpenReadOnly(ctx, filepath.Join(s.paths.StorageDir, "_authority.bolt"),
		s.resources.MaxMetadataBytes)
	if err != nil {
		return err
	}
	s.authorityReader = reader
	evidence, err := reader.Evidence(ctx)
	if err != nil {
		return err
	}
	if evidence.Identity != s.identity || !evidence.MetadataAcquired {
		return fault.ErrIdentityMismatch
	}
	s.authority = evidence
	s.authorityDigest, err = reader.Digest(ctx)
	return err
}

func (s *SourceState) close() error {
	var err error
	if s.authorityReader != nil {
		err = s.authorityReader.Close()
	}
	if s.metaReader != nil {
		err = errors.Join(err, s.metaReader.Close())
	}
	return err
}
