package walstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

// readLog uses the upstream record decoder while owning its input descriptors.
// etcd 3.6 OpenForRead/ReadAll does not release those descriptors on every early
// corruption return; explicit ownership makes failed probes safe to repeat.
func readLog(dir string, point walpb.Snapshot) ([]byte, raftpb.HardState, []raftpb.Entry, error) {
	segments, err := segmentNames(dir)
	if err != nil {
		return nil, raftpb.HardState{}, nil, err
	}
	start := -1
	for i, segment := range segments {
		if segment.index <= point.Index {
			start = i
		}
	}
	if start < 0 {
		return nil, raftpb.HardState{}, nil, wal.ErrSnapshotNotFound
	}
	var readers []fileutil.FileReader
	for i, segment := range segments[start:] {
		if i > 0 && segment.seq != segments[start+i-1].seq+1 {
			return nil, raftpb.HardState{}, nil, fmt.Errorf("WAL segment sequence has a gap")
		}
		f, err := os.Open(filepath.Join(dir, segment.name))
		if err != nil {
			return nil, raftpb.HardState{}, nil, err
		}
		defer f.Close()
		readers = append(readers, fileutil.NewFileReader(f))
	}
	decoder := wal.NewDecoder(readers...)
	var metadata []byte
	var hs raftpb.HardState
	var entries []raftpb.Entry
	matched := false
	for {
		var record walpb.Record
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, hs, nil, err
		}
		switch record.Type {
		case wal.EntryType:
			var entry raftpb.Entry
			if err := entry.Unmarshal(record.Data); err != nil {
				return nil, hs, nil, err
			}
			if entry.Index > point.Index {
				offset := entry.Index - point.Index - 1
				if offset > uint64(len(entries)) {
					return nil, hs, nil, fmt.Errorf("WAL entry gap at %d", entry.Index)
				}
				entries = append(entries[:offset], entry)
			}
		case wal.StateType:
			if err := hs.Unmarshal(record.Data); err != nil {
				return nil, hs, nil, err
			}
		case wal.MetadataType:
			if metadata != nil && !bytes.Equal(metadata, record.Data) {
				return nil, hs, nil, wal.ErrMetadataConflict
			}
			metadata = bytes.Clone(record.Data)
		case wal.CrcType:
			if decoder.LastCRC() != 0 {
				if err := record.Validate(decoder.LastCRC()); err != nil {
					return nil, hs, nil, err
				}
			}
			decoder.UpdateCRC(record.Crc)
		case wal.SnapshotType:
			var marker walpb.Snapshot
			if err := marker.Unmarshal(record.Data); err != nil {
				return nil, hs, nil, err
			}
			if marker.Index == point.Index {
				if marker.Term != point.Term {
					return nil, hs, nil, wal.ErrSnapshotMismatch
				}
				matched = true
			}
		default:
			return nil, hs, nil, fmt.Errorf("unknown WAL record type %d", record.Type)
		}
	}
	if !matched {
		return nil, hs, nil, wal.ErrSnapshotNotFound
	}
	return metadata, hs, entries, nil
}
