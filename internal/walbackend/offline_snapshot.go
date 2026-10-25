package walbackend

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap/snappb"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

// readSnapshotStrict uses the latest committed marker and never calls the
// snapshotter fallback path that renames corrupt source files to .broken.
//
//revive:disable-next-line:function-result-limit The target reader returns snapshot, diagnostics and error.
func readSnapshotStrict(ctx context.Context, dir string, markers []walpb.Snapshot,
	maxBytes uint64) (raftpb.Snapshot, []model.Issue, error) {
	var chosen walpb.Snapshot
	for _, marker := range markers {
		if marker.Index > chosen.Index {
			chosen = marker
		}
		if marker.Index == chosen.Index && marker.Term != chosen.Term {
			return raftpb.Snapshot{}, nil, fault.ErrUnreadable
		}
	}
	if chosen.Index == 0 || chosen.Term == 0 {
		return raftpb.Snapshot{}, nil, fault.New(fault.Unsupported,
			"source requires a complete committed snapshot", nil)
	}
	name := fmt.Sprintf("%016x-%016x.snap", chosen.Term, chosen.Index)
	data, err := readBoundedFile(ctx, filepath.Join(dir, name), maxBytes)
	if err != nil {
		return raftpb.Snapshot{}, nil, err
	}
	var encoded snappb.Snapshot
	if err := encoded.Unmarshal(data); err != nil {
		return raftpb.Snapshot{}, nil, err
	}
	if len(encoded.Data) == 0 || encoded.Crc == 0 {
		return raftpb.Snapshot{}, nil, snap.ErrEmptySnapshot
	}
	if crc32.Checksum(encoded.Data, walCRCTable) != encoded.Crc {
		return raftpb.Snapshot{}, nil, snap.ErrCRCMismatch
	}
	var snapshot raftpb.Snapshot
	if err := snapshot.Unmarshal(encoded.Data); err != nil {
		return raftpb.Snapshot{}, nil, err
	}
	if snapshot.Metadata.Index != chosen.Index || snapshot.Metadata.Term != chosen.Term {
		return raftpb.Snapshot{}, nil, fault.ErrIdentityMismatch
	}
	return snapshot, nil, nil
}
func readBoundedFile(ctx context.Context, path string, maxBytes uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxBytes == 0 || maxBytes > math.MaxInt64-1 {
		return nil, fault.ErrInvalid
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 {
		return nil, fault.ErrUnreadable
	}
	if info.Size() > int64(maxBytes) {
		return nil, unreadable("retained file exceeds recovery byte budget", nil)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if uint64(len(data)) > maxBytes {
		return nil, fault.ErrUnreadable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

func orphanSnapshotFiles(ctx context.Context, dir string, commit uint64) ([]model.Issue, error) {
	names, err := snapshotNames(dir)
	if err != nil {
		return nil, err
	}
	var issues []model.Issue
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var term, index uint64
		if _, err := fmt.Sscanf(name, "%016x-%016x.snap", &term, &index); err != nil {
			return nil, fault.ErrUnreadable
		}
		if name != fmt.Sprintf("%016x-%016x.snap", term, index) {
			return nil, fault.ErrUnreadable
		}
		if index > commit {
			issues = append(issues, model.Issue{Code: "orphan_snapshot_file",
				Message: "uncommitted snapshot file retained: " + name, Remedy: model.RemedyInvestigate})
		}
	}
	return issues, nil
}
