package walbackend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strconv"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

// ReadRecovery reads one managed WAL source without acquiring writers or repairing it.
func ReadRecovery(ctx context.Context, root, group string, limits model.ReadBounds) (model.WALEvidence, error) {
	if err := ctx.Err(); err != nil {
		return model.WALEvidence{}, err
	}
	if err := topology.ValidateLoadID(group); err != nil {
		return model.WALEvidence{}, err
	}
	dir := filepath.Join(root, group)
	if err := validateOfflineDirectories(dir); err != nil {
		return model.WALEvidence{}, err
	}
	pin, err := readOfflinePin(ctx, dir, group)
	if err != nil {
		return model.WALEvidence{}, err
	}
	summary := strictLog{}
	if err := scanStrictLog(ctx, filepath.Join(dir, WALDir), limits, &summary); err != nil {
		return model.WALEvidence{}, err
	}
	committed, issues, err := committedSnapshotMarkers(summary.result)
	if err != nil {
		return model.WALEvidence{}, err
	}
	snapshot, _, err := readSnapshotStrict(ctx, filepath.Join(dir, SnapDir), committed, limits.MaxGroupBytes)
	if err != nil {
		return model.WALEvidence{}, err
	}
	result, err := readOfflineTail(ctx, filepath.Join(dir, WALDir), snapshot, limits)
	if err != nil {
		return model.WALEvidence{}, err
	}
	if summary.result.Recovery.HardState != result.Recovery.HardState ||
		!bytes.Equal(summary.result.Metadata, result.Metadata) {
		return model.WALEvidence{}, fault.ErrConflict
	}
	orphans, err := orphanSnapshotFiles(ctx, filepath.Join(dir, SnapDir), result.Recovery.HardState.Commit)
	if err != nil {
		return model.WALEvidence{}, err
	}
	issues = append(issues, orphans...)
	result.Identity = pin.Identity
	result.Recovery.Snapshot = snapshot
	result.Issues = append(result.Issues, issues...)
	if err := validateWALMetadata(result, pin); err != nil {
		return model.WALEvidence{}, err
	}
	if err := loadOfflineApplied(ctx, dir, &result, limits.MaxGroupBytes); err != nil {
		return model.WALEvidence{}, err
	}
	if err := validateOfflineRecovery(&result, limits); err != nil {
		return model.WALEvidence{}, err
	}
	result.Digest = digestWAL(result)
	return result, nil
}
func validateOfflineDirectories(dir string) error {
	for _, path := range []string{dir, filepath.Join(dir, WALDir), filepath.Join(dir, SnapDir)} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fault.ErrUnreadable
		}
	}
	return nil
}

//revive:disable-next-line:function-result-limit Snapshot selection returns its retained orphan diagnostics.
func committedSnapshotMarkers(evidence model.WALEvidence) ([]walpb.Snapshot, []model.Issue, error) {
	var markers []walpb.Snapshot
	var issues []model.Issue
	seen := make(map[uint64]uint64)
	for _, marker := range evidence.SnapshotMarkers {
		if marker.Index > 0 && marker.Term == 0 {
			return nil, nil, fault.ErrUnreadable
		}
		if term, present := seen[marker.Index]; present && term != marker.Term {
			return nil, nil, fault.ErrUnreadable
		}
		seen[marker.Index] = marker.Term
		if marker.Index > evidence.Recovery.HardState.Commit {
			issues = append(issues, model.Issue{Code: "orphan_snapshot_marker",
				Message: "snapshot marker exceeds retained commit", Remedy: model.RemedyInvestigate})
			continue
		}
		markers = append(markers, walpb.Snapshot{Index: marker.Index, Term: marker.Term})
	}
	return markers, issues, nil
}
func validateWALMetadata(result model.WALEvidence, pin diskIdentity) error {
	var retained diskIdentity
	if err := model.DecodeStrict(result.Metadata, &retained, model.HeaderMaxBytes); err != nil {
		return err
	}
	if retained != pin {
		return &IdentityMismatchError{Stored: retained.Identity, Resolved: pin.Identity}
	}
	return nil
}
func loadOfflineApplied(ctx context.Context, dir string, result *model.WALEvidence, maxBytes uint64) error {
	db, err := recoverydisk.OpenReadOnlyDB(ctx, filepath.Join(dir, StateFile), maxBytes)
	if err != nil {
		return err
	}
	defer db.Close()
	state, err := readApplied(db)
	if err != nil {
		return err
	}
	result.Recovery.AppliedHint = state.Index
	result.Recovery.AppliedConfState = state.ConfState
	return db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketMeta)
		for _, key := range [][]byte{keyFloor, keySnapmark} {
			data := bucket.Get(key)
			if data != nil && len(data) != uint64ByteCount {
				return fault.ErrUnreadable
			}
		}
		if getU64(bucket, keyFloor) > result.Recovery.Snapshot.Metadata.Index {
			return fault.ErrUnreadable
		}
		return nil
	})
}
func validateOfflineRecovery(result *model.WALEvidence, limits model.ReadBounds) error {
	r := &result.Recovery
	point := walpb.Snapshot{Index: r.Snapshot.Metadata.Index, Term: r.Snapshot.Metadata.Term}
	if err := validateRecoveryTail(r.Entries, r.HardState, point); err != nil {
		return err
	}
	if r.AppliedHint > r.HardState.Commit || r.Snapshot.Metadata.Term > r.HardState.Term {
		return fault.ErrUnreadable
	}
	previous := r.Snapshot.Metadata.Term
	for _, entry := range r.Entries {
		if entry.Term < previous || entry.Index <= r.HardState.Commit && entry.Term > r.HardState.Term {
			return fault.ErrUnreadable
		}
		previous = entry.Term
	}
	if len(r.Entries) == 0 {
		result.LastLog = model.Position{Index: r.Snapshot.Metadata.Index, Term: r.Snapshot.Metadata.Term}
	}
	snapshotSize := r.Snapshot.Size()
	if snapshotSize < 0 {
		return fault.ErrUnreadable
	}
	snapshotBytes := uint64(snapshotSize)
	if result.Bytes > limits.MaxGroupBytes || snapshotBytes > limits.MaxGroupBytes-result.Bytes {
		return fault.ErrUnreadable
	}
	result.Bytes += snapshotBytes
	if result.TornTail != nil {
		result.Issues = append(result.Issues, model.Issue{Code: "torn_final_record",
			Message: fmt.Sprintf("ignored final WAL record at %s:%d after complete retained boundaries",
				result.TornTail.File, result.TornTail.Offset), Remedy: model.RemedyInvestigate})
	}
	return nil
}

func digestWAL(value model.WALEvidence) string {
	sum := sha256.New()
	metadata, _ := model.EncodeCanonical(value.Identity)
	writeDigestBytes(sum, metadata)
	writeDigestBytes(sum, value.Metadata)
	snapshot := value.Recovery.Snapshot
	writeDigestUint(sum, snapshot.Metadata.Index)
	writeDigestUint(sum, snapshot.Metadata.Term)
	configuration, _ := snapshot.Metadata.ConfState.Marshal()
	writeDigestBytes(sum, configuration)
	writeDigestBytes(sum, snapshot.Data)
	hardState := value.Recovery.HardState
	for _, n := range []uint64{hardState.Term, hardState.Vote, hardState.Commit, value.Recovery.AppliedHint} {
		writeDigestUint(sum, n)
	}
	for _, entry := range value.Recovery.Entries {
		writeDigestBytes(sum, []byte(strconv.FormatInt(int64(entry.Type), decimalBase)))
		for _, n := range []uint64{entry.Index, entry.Term, uint64(len(entry.Data))} {
			writeDigestUint(sum, n)
		}
		_, _ = sum.Write(entry.Data)
	}
	hint, _ := value.Recovery.AppliedConfState.Marshal()
	writeDigestBytes(sum, hint)
	evidence, _ := model.EncodeCanonical(struct {
		Markers []model.Position
		Torn    *model.TornTail
	}{value.SnapshotMarkers, value.TornTail})
	writeDigestBytes(sum, evidence)
	return hex.EncodeToString(sum.Sum(nil))
}
func writeDigestUint(sum hash.Hash, value uint64) {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], value)
	_, _ = sum.Write(data[:])
}

func writeDigestBytes(sum hash.Hash, data []byte) {
	writeDigestUint(sum, uint64(len(data)))
	_, _ = sum.Write(data)
}

func readOfflinePin(ctx context.Context, dir, group string) (diskIdentity, error) {
	pinBytes, err := readBoundedFile(ctx, filepath.Join(dir, IdentityFile), model.HeaderMaxBytes)
	if err != nil {
		return diskIdentity{}, err
	}
	var pin diskIdentity
	if err := model.DecodeStrict(pinBytes, &pin, model.HeaderMaxBytes); err != nil {
		return diskIdentity{}, err
	}
	if pin.SchemaVersion != 1 {
		return diskIdentity{}, fault.ErrUnsupported
	}
	if pin.Identity.GroupID != group || pin.Identity.NodeName == "" || pin.Identity.ClusterID == "" ||
		pin.Identity.ReplicaID == 0 {
		return diskIdentity{}, fault.ErrIdentityMismatch
	}
	return pin, nil
}

func readOfflineTail(ctx context.Context, dir string, snapshot raftpb.Snapshot,
	limits model.ReadBounds) (model.WALEvidence, error) {
	point := walpb.Snapshot{Index: snapshot.Metadata.Index, Term: snapshot.Metadata.Term}
	snapshotSize := snapshot.Size()
	if snapshotSize < 0 || uint64(snapshotSize) > limits.MaxGroupBytes {
		return model.WALEvidence{}, fault.ErrUnreadable
	}
	tail := strictLog{point: point, collect: true, tailLimit: limits.MaxGroupBytes - uint64(snapshotSize)}
	err := scanStrictLog(ctx, dir, limits, &tail)
	result := tail.result
	if err == nil && !tail.matched {
		err = wal.ErrSnapshotNotFound
	}
	if err == nil {
		err = tail.classifyUnsyncedTail()
		result = tail.result
	}
	if err != nil {
		return model.WALEvidence{}, err
	}
	return result, nil
}
