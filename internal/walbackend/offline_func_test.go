//go:build functional

package walbackend

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/storagetest"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
)

func offlineWAL(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	s := boundTest(t, root, false)
	check(t, s.SaveReady(t.Context(), "group", storage.ReadyWrite{Entries: storagetest.Entries(1, 5, 1), HardState: raftpb.HardState{Term: 1, Vote: 7, Commit: 4}}))
	check(t, s.SaveSnapshot(t.Context(), "group", raftpb.Snapshot{Data: []byte("application-at-2"), Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{7}}}}))
	check(t, s.SaveApplied(t.Context(), "group", 3, raftpb.ConfState{Voters: []uint64{7}}))
	check(t, s.Close())
	return root
}
func offlineLimits() model.ReadBounds {
	return model.ReadBounds{MaxRecordBytes: 4 << 20, MaxGroupBytes: 8 << 20}
}
func sourceHashes(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		out[path] = hex.EncodeToString(sum[:])
		return nil
	})
	check(t, err)
	return out
}
func TestOfflineWALReadsCommitAndTailWithoutSourceWrites(t *testing.T) {
	t.Parallel()
	root := offlineWAL(t)
	before := sourceHashes(t, root)
	r, err := ReadRecovery(t.Context(), root, "group", offlineLimits())
	check(t, err)
	if r.Recovery.Snapshot.Metadata.Index != 2 || r.Recovery.HardState.Commit != 4 || r.LastLog.Index != 5 || r.Recovery.AppliedHint != 3 || len(r.Recovery.Entries) != 3 || r.Identity.ReplicaID != 7 || r.Digest == "" {
		t.Fatal(r)
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("read-only WAL changed source")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ReadRecovery(ctx, root, "group", offlineLimits()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	small := offlineLimits()
	small.MaxRecordBytes = 1
	if _, err := ReadRecovery(t.Context(), root, "group", small); err == nil {
		t.Fatal("record budget ignored")
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("refusal changed source")
	}
}
func TestOfflineSnapshotCRCRefusesWithoutBrokenRename(t *testing.T) {
	t.Parallel()
	root := offlineWAL(t)
	names, err := snapshotNames(filepath.Join(root, "group", SnapDir))
	check(t, err)
	path := filepath.Join(root, "group", SnapDir, names[len(names)-1])
	data, err := os.ReadFile(path)
	check(t, err)
	data[len(data)-1] ^= 1
	check(t, os.WriteFile(path, data, 0o600))
	before := sourceHashes(t, root)
	if _, err := ReadRecovery(t.Context(), root, "group", offlineLimits()); err == nil {
		t.Fatal("corrupt required snapshot accepted")
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("snapshot reader renamed or repaired corrupt evidence")
	}
}

func TestOfflineUnacknowledgedHigherTermTailIsReportedWithoutChangingCommitOrEvidence(t *testing.T) {
	t.Parallel()
	root := offlineWAL(t)
	s := boundTest(t, root, false)
	g, err := s.openGroup("group")
	check(t, err)
	// Model etcd's entries-before-HardState write order interrupted before the
	// new term is durable. Disaster recovery preserves this discarded suffix in
	// its evidence, while ordinary restart remains strict.
	check(t, g.w.Save(raftpb.HardState{}, []raftpb.Entry{{Index: 6, Term: 2, Data: []byte("unacknowledged")}}))
	check(t, g.w.Sync())
	check(t, s.Close())
	before := sourceHashes(t, root)
	result, err := ReadRecovery(t.Context(), root, "group", offlineLimits())
	check(t, err)
	if result.Recovery.HardState.Commit != 4 || result.Recovery.HardState.Term != 1 || result.LastLog.Index != 6 || result.LastLog.Term != 2 {
		t.Fatal("unsynced tail changed the retained commit or lost evidence", result)
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Code == "unsynced_tail_term" && !issue.Blocking {
			found = true
		}
	}
	if !found {
		t.Fatal("unsynced final batch was not reported", result.Issues)
	}
	prefix, err := model.CommittedPrefix(result.Recovery)
	check(t, err)
	if len(prefix.Entries) != 2 || prefix.Entries[len(prefix.Entries)-1].Index != 4 || prefix.HardState != result.Recovery.HardState {
		t.Fatal("replay prefix changed commit or included unsynced data", prefix)
	}
	again, err := ReadRecovery(t.Context(), root, "group", offlineLimits())
	check(t, err)
	if result.Digest != again.Digest {
		t.Fatal("stable unsynced evidence changed digest")
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("inconsistent unacknowledged tail was repaired or discarded")
	}
}

func TestOfflineCRCValidCommittedRewriteRefusesButExactRetryPreservesSuffix(t *testing.T) {
	t.Parallel()
	for _, conflicting := range []bool{true, false} {
		t.Run(map[bool]string{true: "conflicting", false: "exact"}[conflicting], func(t *testing.T) {
			t.Parallel()
			root := offlineWAL(t)
			original, err := ReadRecovery(t.Context(), root, "group", offlineLimits())
			check(t, err)
			s := boundTest(t, root, false)
			g, err := s.openGroup("group")
			check(t, err)
			entry := original.Recovery.Entries[0]
			if conflicting {
				entry.Data = []byte("CRC-valid conflicting committed value")
			}
			// Bypass Store validation to persist actual validly framed corruption,
			// not a CRC failure that masks the committed-prefix check.
			check(t, g.w.Save(raftpb.HardState{}, []raftpb.Entry{entry}))
			check(t, s.Close())
			before := sourceHashes(t, root)
			result, err := ReadRecovery(t.Context(), root, "group", offlineLimits())
			if conflicting {
				if err == nil {
					t.Fatal("CRC-valid committed rewrite accepted")
				}
			} else {
				check(t, err)
				if result.Recovery.HardState.Commit != 4 || result.LastLog.Index != 4 || len(result.Recovery.Entries) != 2 {
					t.Fatal("exact retry lost the committed suffix or retained the overwritten uncommitted tail", result)
				}
			}
			if !reflect.DeepEqual(before, sourceHashes(t, root)) {
				t.Fatal("source changed during committed retry validation")
			}
		})
	}
}

func TestOfflineNewerTermCannotPrecedeLowerHardStateOrCrossCommit(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"later lower term", "committed higher term"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			root := offlineWAL(t)
			s := boundTest(t, root, false)
			g, err := s.openGroup("group")
			check(t, err)
			if shape == "committed higher term" {
				check(t, g.w.Save(raftpb.HardState{Term: 1, Vote: 7, Commit: 6}, nil))
			}
			check(t, g.w.Save(raftpb.HardState{}, []raftpb.Entry{{Index: 6, Term: 2, Data: []byte("new term")}}))
			if shape == "later lower term" {
				check(t, g.w.Save(raftpb.HardState{Term: 1, Vote: 7, Commit: 4}, nil))
			}
			check(t, s.Close())
			before := sourceHashes(t, root)
			if _, err := ReadRecovery(t.Context(), root, "group", offlineLimits()); err == nil {
				t.Fatal("contradictory newer-term source accepted", shape)
			}
			if !reflect.DeepEqual(before, sourceHashes(t, root)) {
				t.Fatal("refusal changed source")
			}
		})
	}
}
func TestOfflineEarlierOversizedLengthCannotBecomeOlderCommit(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s := boundTest(t, root, false)
	segmentedSeed(t, s, 10)
	check(t, s.SaveSnapshot(t.Context(), "group", raftpb.Snapshot{Data: []byte("complete"), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{7}}}}))
	check(t, s.Close())
	wdir := filepath.Join(root, "group", WALDir)
	names, err := segmentNames(wdir)
	check(t, err)
	if len(names) < 2 {
		t.Fatal("fixture needs a later segment")
	}
	file, err := os.OpenFile(filepath.Join(wdir, names[0].name), os.O_WRONLY, 0)
	check(t, err)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], 1<<20)
	_, err = file.WriteAt(header[:], 0)
	check(t, err)
	check(t, file.Close())
	before := sourceHashes(t, root)
	if _, err := ReadRecovery(t.Context(), root, "group", offlineLimits()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("earlier segment truncation did not refuse", err)
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("corrupt length evidence changed")
	}
}
func TestOfflineFinalTornRecordNeedsCompleteRetainedBoundaries(t *testing.T) {
	t.Parallel()
	root := offlineWAL(t)
	wdir := filepath.Join(root, "group", WALDir)
	names, err := segmentNames(wdir)
	check(t, err)
	path := filepath.Join(wdir, names[len(names)-1].name)
	file, err := os.Open(path)
	check(t, err)
	info, err := file.Stat()
	check(t, err)
	frames := strictFrames{ctx: t.Context(), limits: offlineLimits(), final: true, file: names[len(names)-1].name}
	for {
		_, err := frames.next(file, info.Size())
		if errors.Is(err, io.EOF) {
			break
		}
		check(t, err)
	}
	check(t, file.Close())
	file, err = os.OpenFile(path, os.O_WRONLY, 0)
	check(t, err)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], 1024)
	_, err = file.WriteAt(header[:], frames.offset)
	check(t, err)
	_, err = file.WriteAt([]byte{0xff, 0xff, 0xff}, frames.offset+8)
	check(t, err)
	check(t, file.Close())
	before := sourceHashes(t, root)
	result, err := ReadRecovery(t.Context(), root, "group", offlineLimits())
	check(t, err)
	if result.TornTail == nil || result.TornTail.Offset != frames.offset || result.Recovery.HardState.Commit != 4 || result.LastLog.Index != 5 {
		t.Fatal("torn tail changed retained boundaries", result)
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("torn source was repaired")
	}
	// Contradictory durable applied evidence cannot be clamped back to that prefix.
	db, err := openState(filepath.Join(root, "group", StateFile), false)
	check(t, err)
	check(t, db.Update(func(tx *bolt.Tx) error { return putU64(tx.Bucket(bucketMeta), keyApplied, 6) }))
	check(t, db.Close())
	if _, err := ReadRecovery(t.Context(), root, "group", offlineLimits()); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("contradictory applied hint accepted", err)
	}
}

func TestOfflineSnapshotAboveCommitIsReportedWithoutPromotion(t *testing.T) {
	t.Parallel()
	root := offlineWAL(t)
	path := filepath.Join(root, "group", SnapDir, "0000000000000009-0000000000000014.snap")
	check(t, os.WriteFile(path, []byte("not a committed snapshot"), 0o600))
	before := sourceHashes(t, root)
	result, err := ReadRecovery(t.Context(), root, "group", offlineLimits())
	check(t, err)
	found := false
	for _, issue := range result.Issues {
		if issue.Code == "orphan_snapshot_file" {
			found = true
		}
	}
	if !found || result.Recovery.HardState.Commit != 4 || result.Recovery.Snapshot.Metadata.Index != 2 {
		t.Fatal("orphan snapshot changed retained commit", result)
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("orphan source evidence changed")
	}
}

func TestOfflineWALGapRefusesWithoutRepair(t *testing.T) {
	t.Parallel()
	root := offlineWAL(t)
	s := boundTest(t, root, false)
	g, err := s.openGroup("group")
	check(t, err)
	// Bypass the public Store validation to capture a structurally valid WAL
	// frame with a missing logical index. The source reader must detect the gap.
	check(t, g.w.Save(raftpb.HardState{Term: 1, Vote: 7, Commit: 7}, []raftpb.Entry{{Index: 7, Term: 1, Data: []byte("gap")}}))
	check(t, s.Close())
	before := sourceHashes(t, root)
	if _, err := ReadRecovery(t.Context(), root, "group", offlineLimits()); err == nil {
		t.Fatal("CRC-valid logical entry gap accepted")
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("gap source changed")
	}
}

func TestOfflineEarlierCRCWithZeroSectorsIsNotTornTail(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s := boundTest(t, root, false)
	segmentedSeed(t, s, 10)
	check(t, s.SaveSnapshot(t.Context(), "group", raftpb.Snapshot{Data: []byte("complete"), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{7}}}}))
	check(t, s.Close())
	dir := filepath.Join(root, "group", WALDir)
	names, err := segmentNames(dir)
	check(t, err)
	if len(names) < 2 {
		t.Fatal("requires a later segment")
	}
	path := filepath.Join(dir, names[0].name)
	file, err := os.Open(path)
	check(t, err)
	info, err := file.Stat()
	check(t, err)
	frames := strictFrames{ctx: t.Context(), limits: offlineLimits(), file: names[0].name}
	var offset int64
	for {
		offset = frames.offset
		record, err := frames.next(file, info.Size())
		check(t, err)
		if record.Type == wal.EntryType {
			break
		}
	}
	check(t, file.Close())
	// In the etcd Record protobuf, field 1 is type and field 2 begins its CRC.
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	check(t, err)
	var value [1]byte
	_, err = file.ReadAt(value[:], offset+11)
	check(t, err)
	value[0] ^= 1
	_, err = file.WriteAt(value[:], offset+11)
	check(t, err)
	check(t, file.Close())
	before := sourceHashes(t, root)
	if _, err := ReadRecovery(t.Context(), root, "group", offlineLimits()); err == nil {
		t.Fatal("earlier CRC corruption treated as final torn sector")
	}
	if !reflect.DeepEqual(before, sourceHashes(t, root)) {
		t.Fatal("CRC source changed")
	}
}
