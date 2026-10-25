//go:build functional

package walbackend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func seedGrant(cluster string, replica, member uint64) topology.AuthorityGrant {
	action := topology.AuthorizedStep{OperationID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, StepSeq: 1,
		GroupID: "load", Kind: topology.StepCreateFoundingGroup, ExpectedVotersDigest: topology.VoterSetDigest(nil),
		ResultVotersDigest: topology.VoterSetDigest([]uint64{replica})}
	return topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: cluster, GroupID: "load", Revision: 10},
		Purpose: topology.AuthorityEffect, OperationID: action.OperationID, Action: &action,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "load"},
			{Kind: topology.AuthorityHost, GroupID: "load", NodeName: "n1"}},
		Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: member, ReplicaID: replica}}}
}

func seedSource(t *testing.T, root string, noTail bool) model.WALEvidence {
	t.Helper()
	store := openTest(t, root, "n1", "cluster", false)
	id := storage.Identity{NodeName: "n1", ClusterID: "cluster", GroupID: "load", ReplicaID: 7}
	check(t, store.BindReplica(t.Context(), id))
	grant := seedGrant("cluster", 7, 1)
	raw, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: "cluster", GroupID: "load",
		Application: []byte("source-application"), Authority: raftstate.AuthorityState{Grant: &grant, InstallIndex: 10, InstallTerm: 4}})
	check(t, err)
	check(t, store.SaveSnapshot(t.Context(), "load", raftpb.Snapshot{Data: raw,
		Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 4, ConfState: raftpb.ConfState{Voters: []uint64{7}}}}))
	if !noTail {
		app, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application,
			Correlation: "exact-correlation", RequestID: "dedup-key", Data: []byte("committed-business-command")})
		check(t, err)
		admin, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.ObserveEffect,
			Correlation: "effect", Grant: &grant, Action: grant.Action})
		check(t, err)
		check(t, store.SaveReady(t.Context(), "load", storage.ReadyWrite{Entries: []raftpb.Entry{
			{Index: 11, Term: 4, Data: app}, {Index: 12, Term: 4, Data: admin}, {Index: 13, Term: 5, Data: app},
			{Index: 14, Term: 5, Data: []byte("uncommitted")}}, HardState: raftpb.HardState{Term: 5, Vote: 7, Commit: 13}}))
		check(t, store.SaveApplied(t.Context(), "load", 12, raftpb.ConfState{Voters: []uint64{7}}))

		// Model an interrupted new-term batch: source retains this suffix, B must exclude it.
		writer, err := store.openGroup("load")
		check(t, err)
		check(t, writer.w.Save(raftpb.HardState{}, []raftpb.Entry{{Index: 14, Term: 6, Data: []byte("unsynced-new-term")}}))
		check(t, writer.w.Sync())
	}
	check(t, store.Close())
	evidence, err := ReadRecovery(t.Context(), root, "load", model.ReadBounds{MaxRecordBytes: 64 << 20, MaxGroupBytes: 1 << 30})
	check(t, err)
	return evidence
}

func seedFixtureOwned(t *testing.T, noTail bool) (*Store, model.SeedSpec, *recoverydisk.Owner) {
	t.Helper()
	paths := model.Paths{StorageDir: t.TempDir(), LoadWALDir: t.TempDir()}
	var err error
	paths.StorageDir, err = filepath.EvalSymlinks(paths.StorageDir)
	check(t, err)
	paths.LoadWALDir, err = filepath.EvalSymlinks(paths.LoadWALDir)
	check(t, err)
	evidence := seedSource(t, paths.LoadWALDir, noTail)
	commit := model.Position{Index: 13, Term: 5}
	if noTail {
		commit = model.Position{Index: 10, Term: 4}
	}
	source := model.SeedSource{GroupID: "load", Identity: evidence.Identity, MemberID: 1, SourceVersion: 1,
		Snapshot: model.Position{Index: 10, Term: 4}, Commit: commit, LastLog: evidence.LastLog,
		HardStateTerm: evidence.Recovery.HardState.Term, Digest: evidence.Digest, Bytes: evidence.Bytes}
	manifest := recoverytest.Manifest(paths, "seed", time.Now().UTC(), model.Importing)
	manifest.Approved.Groups = append(manifest.Approved.Groups, topology.Group{ID: "load", NextReplicaID: 8,
		Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 7, Role: topology.ReplicaVoter}}})
	manifest.Floors = append(manifest.Floors, model.AllocationFloor{GroupID: "load", NextID: 8})
	manifest.Sources = append(manifest.Sources, source)
	manifest.Seeds = []model.SeedProgress{{GroupID: "load", State: "pending"}}
	manifest.Archive = append(manifest.Archive, model.ArchiveEntry{Root: "wal", Name: "load", Kind: "directory",
		Classification: "source", EvidenceDigest: evidence.Digest, Moved: true})
	digest, err := model.DigestAccepted(model.AcceptedFromManifest(manifest))
	check(t, err)
	manifest.PlanDigest = digest
	manifest.Acceptance.PlanDigest = digest
	data, err := model.EncodeManifest(manifest)
	check(t, err)
	dir := filepath.Join(paths.StorageDir, recoverydisk.Namespace, manifest.BackupName)
	check(t, os.MkdirAll(dir, 0o700))
	check(t, os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0o600))
	backup := filepath.Join(paths.LoadWALDir, recoverydisk.Namespace, manifest.BackupName, "backup")
	check(t, os.MkdirAll(backup, 0o700))
	check(t, os.Rename(filepath.Join(paths.LoadWALDir, "load"), filepath.Join(backup, "load")))
	owner, err := recoverydisk.Acquire(t.Context(), paths)
	check(t, err)
	t.Cleanup(func() { check(t, owner.Close()) })
	_, err = owner.PendingStartup(t.Context())
	check(t, err)
	lease, err := owner.TakeWALLease()
	check(t, err)
	prewrite := false
	store, err := OpenWithLease(Options{RootDir: paths.LoadWALDir, NodeName: "n1", ClusterID: "recovered",
		SegmentBytes: testSegmentBytes, DisableFSync: true, Prewrite: &prewrite}, lease)
	check(t, err)
	t.Cleanup(func() { check(t, store.Close()) })
	key := model.SeedKey{RecoveryID: manifest.Request.RecoveryID, PlanDigest: digest,
		Destination: storage.Identity{ClusterID: "recovered", NodeName: "n1", GroupID: "load", ReplicaID: 8}, MemberID: 2}
	check(t, ProtectRecovery(t.Context(), store, key))
	return store, model.SeedSpec{Key: key, BackupName: manifest.BackupName, Source: source, Resources: manifest.Resources,
		Grant: seedGrant("recovered", 8, 2)}, owner
}

func TestRecoverySeedTransformsCommittedPrefixAndPreservesBackup(t *testing.T) {
	t.Parallel()
	store, seed := seedFixture(t, false)
	archived := filepath.Join(store.opts.RootDir, recoverydisk.Namespace, seed.BackupName, "backup")
	before := sourceHashes(t, archived)
	marker, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	retained, err := ReadRecovery(t.Context(), store.opts.RootDir, "load", offlineLimits())
	check(t, err)
	source, err := ReadRecovery(t.Context(), archived, "load", offlineLimits())
	check(t, err)
	if len(retained.Recovery.Entries) != 3 || retained.Recovery.HardState != (raftpb.HardState{Term: 5, Commit: 13}) ||
		retained.Recovery.AppliedHint != 10 || !reflect.DeepEqual(retained.Recovery.Snapshot.Metadata.ConfState.Voters, []uint64{8}) {
		t.Fatal(retained.Recovery)
	}
	for i, entry := range retained.Recovery.Entries {
		original := source.Recovery.Entries[i]
		if entry.Index != original.Index || entry.Term != original.Term || entry.Type != raftpb.EntryNormal {
			t.Fatal(entry, original)
		}
		if i == 1 {
			if len(entry.Data) != 0 {
				t.Fatal("source authority retained")
			}
		} else if !bytes.Equal(entry.Data, original.Data) {
			t.Fatal("application command was rewritten")
		}
	}
	wrapper, err := raftstate.DecodeRaftSnapshot(retained.Recovery.Snapshot, "recovered", "load")
	check(t, err)
	if string(wrapper.Application) != "source-application" || wrapper.Authority.InstallIndex != 10 ||
		wrapper.Authority.InstallTerm != 4 || wrapper.Authority.Receipt != nil || !wrapper.Authority.Grant.Equal(seed.Grant) {
		t.Fatal(wrapper)
	}
	if !reflect.DeepEqual(before, sourceHashes(t, archived)) {
		t.Fatal("A backup changed")
	}
	again, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	if !reflect.DeepEqual(marker, again) {
		t.Fatal("idempotence lost")
	}
	check(t, store.BindReplica(t.Context(), seed.Key.Destination))
	check(t, store.Ensure(t.Context(), "load"))
	if store.groups["load"].state.NoSync {
		t.Fatal("ordinary DisableFSync leaked into recovery")
	}
	if err := os.RemoveAll(filepath.Join(store.opts.RootDir, "load")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadRecoverySeed(t.Context(), store, seed.Key); err == nil {
		t.Fatal("acquired missing identity was reseeded")
	}
}

// checkpointBackend is the same private capability as the Node-owned adapter.
type checkpointBackend struct{ *Store }

type seedTransport struct{}

func (seedTransport) Send(ctx context.Context, _ []raftpb.Message) error { return ctx.Err() }

func (s checkpointBackend) SaveRecoveryCheckpoint(ctx context.Context, key model.SeedKey, ss raftpb.Snapshot) error {
	return SaveRecoveryCheckpoint(ctx, s.Store, key, ss)
}
func (s checkpointBackend) VerifyRecoveryCheckpoint(ctx context.Context, key model.SeedKey, p model.Position) (model.CheckpointProof, error) {
	return VerifyRecoveryCheckpoint(ctx, s.Store, key, p)
}
func (s checkpointBackend) FinishRecovery(ctx context.Context, key model.SeedKey, p model.Consumed) error {
	return FinishRecovery(ctx, s.Store, key, p)
}

type portableMachine struct {
	applied  []raftengine.Command
	restored []byte
	restore  func(context.Context) error
}

func (m *portableMachine) Restore(ctx context.Context, data []byte) error {
	m.restored = bytes.Clone(data)
	if m.restore != nil {
		return m.restore(ctx)
	}
	return ctx.Err()
}
func (m *portableMachine) Apply(ctx context.Context, c raftengine.Command) (raftengine.Result, error) {
	m.applied = append(m.applied, c)
	return raftengine.Result{}, ctx.Err()
}
func (*portableMachine) Snapshot(ctx context.Context) ([]byte, error) {
	return []byte("portable-B"), ctx.Err()
}

func seedEngineOptions(store *Store, seed model.SeedSpec, m *portableMachine) raftengine.Options {
	return raftengine.Options{Store: checkpointBackend{store}, Machine: m, Transport: seedTransport{}, NodeName: "n1", ClusterID: "recovered",
		GroupID: "load", ReplicaID: 8, MemberID: 2, InstanceID: "new-run", ManagedAuthority: true, Mode: raftengine.Joining}
}

func TestRecoverySameIndexCheckpointIsPortableAndAcquiredChecksumCanEvolve(t *testing.T) {
	t.Parallel()
	store, seed := seedFixture(t, true)
	marker, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	machine := new(portableMachine)
	core, err := raftengine.NewRecoveryContext(t.Context(), seedEngineOptions(store, seed, machine), seed.Key)
	check(t, err)
	check(t, core.Start(t.Context()))
	t.Cleanup(func() { check(t, core.Stop(context.Background())) })
	check(t, core.WaitReady(t.Context()))
	proof, err := core.RecoveryCheckpoint(t.Context(), seed.Key)
	check(t, err)
	if proof.Position != seed.Source.Commit || proof.Digest == "" || string(machine.restored) != "source-application" {
		t.Fatal("checkpoint lacked exact source/normalized position", proof, machine)
	}
	check(t, core.Stop(t.Context()))
	check(t, store.Close())
	// Ordinary retained startup needs neither source bytes nor InitialAuthority.
	opts := store.opts
	reopened, err := Open(opts)
	check(t, err)
	t.Cleanup(func() { check(t, reopened.Close()) })
	next := new(portableMachine)
	normal := seedEngineOptions(reopened, seed, next)
	normal.Store = reopened
	retained, err := raftengine.NewContext(t.Context(), normal)
	check(t, err)
	check(t, retained.Stop(t.Context()))
	if string(next.restored) != "portable-B" {
		t.Fatal("same-index snapshot did not normalize", string(next.restored))
	}
	evidence, err := reopened.Load(t.Context(), "load")
	check(t, err)
	if evidence.Snapshot.Metadata.Index != 10 || bytes.Contains(evidence.Snapshot.Data, []byte("source-application")) {
		t.Fatal("normalized snapshot unavailable", evidence.Snapshot)
	}
	if marker.ImageDigest == proof.Digest {
		t.Fatal("image and checkpoint unexpectedly conflated")
	}
}

func TestRecoveryRestoreUsesOwnedLifetimeBeyondOrdinaryDeadline(t *testing.T) {
	t.Parallel()
	store, seed := seedFixture(t, true)
	_, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	machine := &portableMachine{restore: func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	result := make(chan error, 1)
	go func() {
		core, err := raftengine.NewRecoveryContext(t.Context(), seedEngineOptions(store, seed, machine), seed.Key)
		if core != nil {
			_ = core.Stop(context.Background())
		}
		result <- err
	}()
	select {
	case <-entered:
	case err := <-result:
		t.Fatal("restore never entered", err)
	}
	timer := time.NewTimer(5100 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-result:
		t.Fatal("restore ended before owner released", err)
	case <-timer.C:
	}
	close(release)
	check(t, <-result)
}

func TestRecoverySeedRejectsSourceCorruptionAndUnacceptedBackup(t *testing.T) {
	t.Parallel()
	store, seed := seedFixture(t, false)
	wrong := seed
	wrong.BackupName = "arbitrary"
	if _, err := InstallRecoverySeed(t.Context(), store, wrong, seedReplay(wrong)); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal(err)
	}
	path := filepath.Join(store.opts.RootDir, recoverydisk.Namespace, seed.BackupName, "backup", "load", IdentityFile)
	check(t, os.WriteFile(path, []byte("broken"), 0o600))
	if _, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed)); err == nil {
		t.Fatal("corruption accepted")
	}
	if _, err := os.Stat(filepath.Join(store.opts.RootDir, "load")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial live seed", err)
	}
}

func seedReplay(seed model.SeedSpec) SeedReplay {
	return func(ctx context.Context, retained storage.Recovery) (model.Position, error) {
		result, err := raftengine.Replay(ctx, raftengine.ReplayOptions{Identity: seed.Source.Identity, MemberID: seed.Source.MemberID,
			InstanceID: seed.Key.RecoveryID}, retained, raftengine.DiscardMachine{})
		return model.Position{Index: result.Index, Term: result.Term}, err
	}
}

func TestRecoverySeedResumesPublicationBoundariesWithoutClobber(t *testing.T) {
	t.Parallel()
	for _, point := range []string{"prepared", "rename", "parent_sync", "installed", "acquired"} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			store, seed := seedFixture(t, true)
			injected := errors.New("interrupted " + point)
			store.recoveryBoundary = func(at string) error {
				if at == point {
					return injected
				}
				return nil
			}
			_, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
			if point == "acquired" {
				check(t, err)
				check(t, store.BindReplica(t.Context(), seed.Key.Destination))
				err = store.Ensure(t.Context(), "load")
			}
			if !errors.Is(err, injected) {
				t.Fatal("boundary not reached", point, err)
			}
			store.recoveryBoundary = nil
			if point == "prepared" {
				pending, installed, err := ReadRecoverySeed(t.Context(), store, seed.Key)
				check(t, err)
				if installed || !pending.Grant.Equal(seed.Grant) {
					t.Fatal("building intent was lost or treated as installed", pending)
				}
			}
			marker, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
			check(t, err)
			if marker.Destination != seed.Key.Destination {
				t.Fatal(marker)
			}
			check(t, store.BindReplica(t.Context(), seed.Key.Destination))
			check(t, store.Ensure(t.Context(), "load"))
			if point == "prepared" {
				names, err := os.ReadDir(filepath.Join(store.opts.RootDir, recoverydisk.Namespace, seed.BackupName, "abandoned"))
				check(t, err)
				if len(names) != 1 {
					t.Fatal("incomplete stage not retained", names)
				}
			}
		})
	}
}

func TestRecoveryInstalledMissingImageCannotBeReconstructed(t *testing.T) {
	t.Parallel()
	store, seed := seedFixture(t, true)
	_, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	check(t, os.RemoveAll(filepath.Join(store.opts.RootDir, "load")))
	if _, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed)); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(store.opts.RootDir, "load")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refilled missing B", err)
	}
}

func TestRecoveryCheckpointSurvivesIntermediateSnapshotAndRetryKeepsSeedGrant(t *testing.T) {
	t.Parallel()
	store, seed := seedFixture(t, false)
	marker, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	machine := new(portableMachine)
	core, err := raftengine.NewRecoveryContext(t.Context(), seedEngineOptions(store, seed, machine), seed.Key)
	check(t, err)
	check(t, core.Start(t.Context()))
	t.Cleanup(func() { check(t, core.Stop(context.Background())) })
	check(t, core.WaitReady(t.Context()))
	if len(machine.applied) != 2 || machine.applied[0].RequestID != "dedup-key" || machine.applied[0].Index != 11 ||
		machine.applied[1].Index != 13 {
		t.Fatal("committed application replay changed", machine.applied)
	}
	waitSeedLeader(t, core)
	fence, err := core.InstallAuthority(t.Context(), seed.Grant)
	check(t, err)
	receipt, err := core.ObserveAuthorizedEffect(t.Context(), seed.Grant)
	check(t, err)
	if fence.AppliedIndex != 10 || fence.AppliedTerm != 4 || receipt.AppliedIndex <= 13 || receipt.AppliedTerm <= 5 {
		t.Fatal("installation position conflated with effect position", fence, receipt)
	}
	proof, err := core.RecoveryCheckpoint(t.Context(), seed.Key)
	check(t, err)
	retry := seed.Grant.Clone()
	retry.Key.Revision++
	retry.Action.Attempt++
	retry.Action.OperationID = topology.MakeOperationID(retry.Action.Version, retry.Action.Attempt)
	retry.OperationID = retry.Action.OperationID
	nextFence, err := core.InstallAuthority(t.Context(), retry)
	check(t, err)
	nextReceipt, err := core.ObserveAuthorizedEffect(t.Context(), retry)
	check(t, err)
	if nextFence.AppliedIndex <= receipt.AppliedIndex || nextReceipt.AppliedIndex <= nextFence.AppliedIndex ||
		nextReceipt.AppliedTerm < receipt.AppliedTerm {
		t.Fatal("retry authority regressed", fence, nextFence, receipt, nextReceipt)
	}
	check(t, core.Snapshot(t.Context()))
	actual, err := VerifyRecoveryCheckpoint(t.Context(), store, seed.Key, proof.Position)
	check(t, err)
	if actual != proof {
		t.Fatal("newer legitimate snapshot lost durable checkpoint", actual, proof)
	}
	retained, found, err := ReadRecoverySeed(t.Context(), store, seed.Key)
	check(t, err)
	if !found || !reflect.DeepEqual(retained, marker) {
		t.Fatal("retry rewrote seed provenance", retained, marker)
	}
	nextProof, err := core.RecoveryCheckpoint(t.Context(), seed.Key)
	check(t, err)
	other := seed.Key
	other.Destination.GroupID = "another-pending-group"
	check(t, ProtectRecovery(t.Context(), store, other))
	host := topology.EffectReceipt{Key: retry.Key, Action: retry.Action,
		Resource: topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "load", NodeName: "n1"},
		Target:   retry.Targets[0], Outcome: topology.EffectApplied, LocalSequence: 1}
	consumed := model.Consumed{Identity: seed.Key.Destination, Checkpoint: nextProof.Position,
		SnapshotDigest: nextProof.Digest, HostReceipt: host, GroupReceipt: nextReceipt}
	wrong := consumed
	wrong.GroupReceipt = receipt
	if err := core.FinishRecovery(t.Context(), seed.Key, wrong); err == nil {
		t.Fatal("released durability with superseded receipt")
	}
	check(t, core.FinishRecovery(t.Context(), seed.Key, consumed))
	check(t, core.FinishRecovery(t.Context(), seed.Key, consumed))
	if !store.groups["load"].state.NoSync || store.protection[other.Destination.GroupID] != other {
		t.Fatal("durability release changed the wrong group")
	}
	check(t, core.Stop(t.Context()))
	check(t, store.Close())
}

func TestRecoveryRestoreCancellationJoinsCallback(t *testing.T) {
	t.Parallel()
	store, seed := seedFixture(t, true)
	_, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	entered := make(chan struct{})
	returned := make(chan struct{})
	machine := &portableMachine{restore: func(ctx context.Context) error { close(entered); <-ctx.Done(); close(returned); return ctx.Err() }}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := raftengine.NewRecoveryContext(ctx, seedEngineOptions(store, seed, machine), seed.Key)
		result <- err
	}()
	select {
	case <-entered:
	case err := <-result:
		t.Fatal("restore not entered", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-returned:
	default:
		t.Fatal("owner returned before Restore joined")
	}
}

func waitSeedLeader(t *testing.T, core *raftengine.Group) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		state, err := core.LeadershipState()
		check(t, err)
		if state.Epoch.Context != nil {
			check(t, core.ReadBarrier(ctx))
			return
		}
		select {
		case <-state.Changed:
		case <-ctx.Done():
			t.Fatal("no singleton leader", ctx.Err())
		}
	}
}

func seedFixture(t *testing.T, noTail bool) (*Store, model.SeedSpec) {
	t.Helper()
	store, seed, _ := seedFixtureOwned(t, noTail)
	return store, seed
}

func TestRecoveryAcquiredRestartUsesPortableBWithoutReadingBackup(t *testing.T) {
	t.Parallel()
	store, seed, owner := seedFixtureOwned(t, false)
	marker, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed))
	check(t, err)
	machine := new(portableMachine)
	core, err := raftengine.NewRecoveryContext(t.Context(), seedEngineOptions(store, seed, machine), seed.Key)
	check(t, err)
	check(t, core.Start(t.Context()))
	t.Cleanup(func() { check(t, core.Stop(context.Background())) })
	check(t, core.WaitReady(t.Context()))
	proof, err := core.RecoveryCheckpoint(t.Context(), seed.Key)
	check(t, err)
	check(t, core.Stop(t.Context()))
	check(t, store.Close())
	paths := owner.Paths()
	check(t, owner.Close())
	// Once B was acquired, A is not refill input, even before consumption is recorded.
	check(t, os.RemoveAll(filepath.Join(paths.LoadWALDir, recoverydisk.Namespace, seed.BackupName, "backup")))
	nextOwner, err := recoverydisk.Acquire(t.Context(), paths)
	check(t, err)
	t.Cleanup(func() { check(t, nextOwner.Close()) })
	_, err = nextOwner.PendingStartup(t.Context())
	check(t, err)
	lease, err := nextOwner.TakeWALLease()
	check(t, err)
	nextStore, err := OpenWithLease(store.opts, lease)
	check(t, err)
	t.Cleanup(func() { check(t, nextStore.Close()) })
	check(t, ProtectRecovery(t.Context(), nextStore, seed.Key))
	retained, found, err := ReadRecoverySeed(t.Context(), nextStore, seed.Key)
	check(t, err)
	if !found || !reflect.DeepEqual(marker, retained) {
		t.Fatal("evolving store lost seed provenance", retained)
	}
	nextMachine := new(portableMachine)
	nextCore, err := raftengine.NewRecoveryContext(t.Context(), seedEngineOptions(nextStore, seed, nextMachine), seed.Key)
	check(t, err)
	check(t, nextCore.Stop(t.Context()))
	if string(nextMachine.restored) != "portable-B" {
		t.Fatal("restart restored original A", string(nextMachine.restored))
	}
	actual, err := VerifyRecoveryCheckpoint(t.Context(), nextStore, seed.Key, proof.Position)
	check(t, err)
	if actual != proof {
		t.Fatal("checkpoint lost through restart", actual, proof)
	}
}

func TestRecoveryValidatesNativeConfigurationBeforeReplacingIt(t *testing.T) {
	t.Parallel()
	evidence := seedSource(t, t.TempDir(), true)
	learner := seedGrant("cluster", 7, 1)
	learner.Key.Revision++
	learner.Action.Kind = topology.StepAddLearner
	learner.Action.NodeName = "n2"
	learner.Action.ReplicaID = 9
	learner.Action.ExpectedVotersDigest = topology.VoterSetDigest([]uint64{7})
	learner.Action.ResultVotersDigest = learner.Action.ExpectedVotersDigest
	learner.Resources = learner.Resources[:1]
	learner.Targets = append(learner.Targets, topology.AuthorityTarget{NodeName: "n2", MemberID: 3, ReplicaID: 9})
	install, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.InstallAuthority,
		Correlation: "install", Grant: &learner})
	check(t, err)
	payload, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.ManagedConfiguration,
		Correlation: "native-change", Grant: &learner, Action: learner.Action})
	check(t, err)
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 9}}, Context: payload}
	config, err := cc.Marshal()
	check(t, err)
	app, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application,
		Correlation: "app", RequestID: "stable", Data: []byte("business")})
	check(t, err)
	evidence.Recovery.Entries = []raftpb.Entry{{Index: 11, Term: 4, Data: install},
		{Index: 12, Term: 4, Type: raftpb.EntryConfChangeV2, Data: config}, {Index: 13, Term: 4, Data: app}}
	evidence.Recovery.HardState = raftpb.HardState{Commit: 13, Term: 4}
	evidence.LastLog = model.Position{Index: 13, Term: 4}
	evidence.Digest = digestWAL(evidence)
	seed := model.SeedSpec{Source: model.SeedSource{GroupID: "load", Identity: evidence.Identity, MemberID: 1,
		Snapshot: model.Position{Index: 10, Term: 4}, Commit: evidence.LastLog, LastLog: evidence.LastLog, HardStateTerm: 4,
		Digest: evidence.Digest, Bytes: evidence.Bytes}, Key: model.SeedKey{RecoveryID: "test",
		Destination: storage.Identity{GroupID: "load", ClusterID: "recovered", NodeName: "n1", ReplicaID: 8}},
		Grant: seedGrant("recovered", 8, 2)}
	result, err := transformRecoverySeed(t.Context(), seed, evidence, seedReplay(seed))
	check(t, err)
	if len(result.Entries[0].Data) != 0 || result.Entries[1].Type != raftpb.EntryNormal || len(result.Entries[1].Data) != 0 ||
		!bytes.Equal(result.Entries[2].Data, app) || !bytes.Equal(evidence.Recovery.Entries[1].Data, config) {
		t.Fatal("configuration transformation changed source or application", result.Entries)
	}
	evidence.Recovery.Entries[1].Data = []byte("invalid-native-change")
	evidence.Digest = digestWAL(evidence)
	seed.Source.Digest = evidence.Digest
	if _, err := transformRecoverySeed(t.Context(), seed, evidence, seedReplay(seed)); err == nil {
		t.Fatal("source native corruption was hidden by replacement")
	}
}

func TestRecoveryRefusesUnprovenStagingAndForeignLiveDirectory(t *testing.T) {
	t.Parallel()
	for _, where := range []string{"stage", "live"} {
		t.Run(where, func(t *testing.T) {
			t.Parallel()
			store, seed := seedFixture(t, true)
			dir := filepath.Join(store.opts.RootDir, "load")
			if where == "stage" {
				dir = filepath.Join(store.opts.RootDir, recoverydisk.Namespace, seed.BackupName, "seed", "load")
			}
			check(t, os.MkdirAll(dir, 0o700))
			check(t, os.WriteFile(filepath.Join(dir, "foreign-evidence"), []byte("preserve"), 0o600))
			before := sourceHashes(t, dir)
			if _, err := InstallRecoverySeed(t.Context(), store, seed, seedReplay(seed)); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, sourceHashes(t, dir)) {
				t.Fatal("foreign evidence changed")
			}
		})
	}
}
