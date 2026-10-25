package walbackend

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func transformRecoverySeed(ctx context.Context, seed model.SeedSpec, evidence model.WALEvidence,
	replay SeedReplay) (storage.Recovery, error) {
	source := seed.Source
	if evidence.Identity != source.Identity || evidence.Digest != source.Digest ||
		evidence.Recovery.HardState.Term != source.HardStateTerm ||
		evidence.Recovery.HardState.Commit != source.Commit.Index ||
		evidence.LastLog != source.LastLog || evidence.Bytes != source.Bytes {
		return storage.Recovery{}, fault.ErrIdentityMismatch
	}
	retained, err := model.CommittedPrefix(evidence.Recovery)
	if err != nil {
		return storage.Recovery{}, err
	}
	snapshot := retained.Snapshot
	if snapshot.Metadata.Index != source.Snapshot.Index || snapshot.Metadata.Term != source.Snapshot.Term {
		return storage.Recovery{}, fault.ErrIdentityMismatch
	}
	if replay == nil {
		return storage.Recovery{}, fault.ErrOperationAuthorization
	}
	position, err := replay(ctx, retained)
	if err != nil {
		return storage.Recovery{}, err
	}
	if position != source.Commit {
		return storage.Recovery{}, fault.ErrUnreadable
	}
	decoded, err := raftstate.DecodeRaftSnapshot(snapshot, source.Identity.ClusterID, source.GroupID)
	if err != nil {
		return storage.Recovery{}, err
	}
	grant := seed.Grant.Clone()
	data, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema,
		ClusterID: seed.Key.Destination.ClusterID, GroupID: source.GroupID,
		Application: slices.Clone(decoded.Application),
		Authority: raftstate.AuthorityState{Grant: &grant, InstallIndex: source.Snapshot.Index,
			InstallTerm: source.Snapshot.Term}})
	if err != nil {
		return storage.Recovery{}, err
	}
	snapshot.Data = data
	snapshot.Metadata.ConfState = raftpb.ConfState{Voters: []uint64{seed.Key.Destination.ReplicaID}}
	entries, err := transformSeedEntries(retained.Entries)
	if err != nil {
		return storage.Recovery{}, err
	}
	return storage.Recovery{Snapshot: snapshot, Entries: entries,
		HardState:   raftpb.HardState{Term: source.HardStateTerm, Commit: source.Commit.Index},
		AppliedHint: source.Snapshot.Index, AppliedConfState: snapshot.Metadata.ConfState}, nil
}

func validateSeedSpec(s *Store, seed model.SeedSpec) error {
	if err := validateSeedAcceptance(s, seed); err != nil {
		return err
	}
	if err := validateSeedKey(s, seed.Key); err != nil {
		return err
	}
	marker := seedMarker(seed, "0000000000000000000000000000000000000000000000000000000000000000")
	if err := model.ValidateSeedMarker(marker); err != nil {
		return err
	}
	grant := seed.Grant
	target := topology.AuthorityTarget{NodeName: seed.Key.Destination.NodeName, MemberID: seed.Key.MemberID,
		ReplicaID: seed.Key.Destination.ReplicaID}
	if grant.Action == nil || grant.Action.Kind != topology.StepCreateFoundingGroup ||
		len(grant.Targets) != 1 || grant.Targets[0] != target {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func seedMarker(seed model.SeedSpec, digest string) model.SeedMarker {
	return model.SeedMarker{Schema: model.SchemaVersion, RecoveryID: seed.Key.RecoveryID,
		PlanDigest: seed.Key.PlanDigest,
		Source:     seed.Source, Destination: seed.Key.Destination, MemberID: seed.Key.MemberID,
		Grant: seed.Grant.Clone(), ImageDigest: digest}
}

func validateSeedAcceptance(s *Store, seed model.SeedSpec) error {
	manifest, err := s.rootLease.RecoveryManifest(seed.BackupName)
	if err != nil {
		return err
	}
	key := seed.Key
	if manifest.Request.Mode != model.Rebuild || manifest.SuccessorStartedAt == nil ||
		manifest.Request.RecoveryID != key.RecoveryID || manifest.PlanDigest != key.PlanDigest ||
		manifest.Identity.ClusterID != key.Destination.ClusterID ||
		manifest.Identity.NodeName != key.Destination.NodeName ||
		manifest.Identity.MemberID != key.MemberID || seed.Resources != manifest.Resources {
		return fault.ErrOperationAuthorization
	}
	if manifest.Phase != model.Importing && manifest.Phase != model.AwaitingStart &&
		manifest.Phase != model.Validating {
		return fault.ErrOperationAuthorization
	}
	foundFloor := false
	for _, floor := range manifest.Floors {
		if floor.GroupID == key.Destination.GroupID && floor.NextID == key.Destination.ReplicaID {
			foundFloor = true
			break
		}
	}
	if !foundFloor {
		return fault.ErrIdentityMismatch
	}
	for _, source := range manifest.Sources {
		if source == seed.Source && source.GroupID == key.Destination.GroupID {
			return nil
		}
	}
	return fault.ErrIdentityMismatch
}

func transformSeedEntries(source []raftpb.Entry) ([]raftpb.Entry, error) {
	entries := make([]raftpb.Entry, len(source))
	for i, entry := range source {
		entries[i] = raftpb.Entry{Index: entry.Index, Term: entry.Term, Type: raftpb.EntryNormal}
		if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
			payload, decodeErr := raftstate.DecodeEntry(entry.Data)
			if decodeErr != nil {
				return nil, decodeErr
			}
			if payload.Kind == raftstate.Application {
				entries[i].Data = slices.Clone(entry.Data)
			}
		}
	}
	return entries, nil
}
