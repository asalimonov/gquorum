package walbackend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

type strictLog struct {
	point     walpb.Snapshot
	result    model.WALEvidence
	matched   bool
	collect   bool
	bytes     uint64
	tailLimit uint64
}

func readLogStrict(ctx context.Context, dir string, point walpb.Snapshot,
	limits model.ReadBounds) (model.WALEvidence, error) {
	state := strictLog{point: point, collect: true, tailLimit: limits.MaxGroupBytes}
	if err := scanStrictLog(ctx, dir, limits, &state); err != nil {
		return model.WALEvidence{}, err
	}
	if !state.matched {
		return model.WALEvidence{}, wal.ErrSnapshotNotFound
	}
	return state.result, nil
}
func scanStrictLog(ctx context.Context, dir string, limits model.ReadBounds, state *strictLog) error {
	if limits.MaxRecordBytes == 0 || limits.MaxGroupBytes == 0 {
		return fault.ErrInvalid
	}
	names, err := segmentNames(dir)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return unreadable("WAL has no retained segments", nil)
	}
	decoder := strictFrames{ctx: ctx, limits: limits}
	for i, name := range names {
		if i > 0 && (names[i-1].seq == math.MaxUint64 || name.seq != names[i-1].seq+1) {
			return unreadable("WAL segment sequence has a gap", nil)
		}
		decoder.offset = 0
		decoder.final = i == len(names)-1
		decoder.file = name.name
		if err := scanStrictSegment(filepath.Join(dir, name.name), &decoder, state); err != nil {
			return err
		}
	}
	state.result.TornTail = decoder.torn
	return nil
}
func scanStrictSegment(path string, decoder *strictFrames, state *strictLog) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fault.ErrUnreadable
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	for {
		record, err := decoder.next(file, info.Size())
		if errors.Is(err, io.EOF) {
			return file.Close()
		}
		if err != nil {
			return errors.Join(err, file.Close())
		}
		if err := state.record(record, decoder.limits.MaxGroupBytes); err != nil {
			return errors.Join(err, file.Close())
		}
	}
}
func (s *strictLog) record(record walpb.Record, maxBytes uint64) error {
	switch record.Type {
	case wal.CrcType:
		return nil
	case wal.MetadataType:
		if s.result.Metadata != nil && !bytes.Equal(s.result.Metadata, record.Data) {
			return wal.ErrMetadataConflict
		}
		s.result.Metadata = bytes.Clone(record.Data)
		return nil
	case wal.StateType:
		var next raftpb.HardState
		if err := next.Unmarshal(record.Data); err != nil {
			return err
		}
		if err := validateStrictHardState(s.result.Recovery.HardState, next); err != nil {
			return err
		}
		s.result.Recovery.HardState = next
		return nil
	case wal.SnapshotType:
		var marker walpb.Snapshot
		if err := marker.Unmarshal(record.Data); err != nil {
			return err
		}
		if marker.Index == s.point.Index {
			if marker.Term != s.point.Term {
				return wal.ErrSnapshotMismatch
			}
			s.matched = true
		}
		s.result.SnapshotMarkers = append(s.result.SnapshotMarkers, model.Position{Index: marker.Index,
			Term: marker.Term})
		return nil
	case wal.EntryType:
		return s.entry(record.Data, min(maxBytes, s.tailLimit))
	default:
		return fmt.Errorf("unknown WAL record type %d", record.Type)
	}
}
func (s *strictLog) entry(data []byte, maxBytes uint64) error {
	var entry raftpb.Entry
	if err := entry.Unmarshal(data); err != nil {
		return err
	}
	if entry.Index == 0 || entry.Term == 0 {
		return fault.ErrUnreadable
	}
	if !s.collect || entry.Index <= s.point.Index {
		s.result.LastLog = model.Position{Index: entry.Index, Term: entry.Term}
		return nil
	}
	offset := entry.Index - s.point.Index - 1
	if offset > uint64(len(s.result.Recovery.Entries)) {
		return unreadable("WAL entry gap", nil)
	}
	if entry.Index <= s.result.Recovery.HardState.Commit && offset < uint64(len(s.result.Recovery.Entries)) {
		if err := verifyStrictCommittedEntry(s.result.Recovery.Entries[offset], entry); err != nil {
			return err
		}
		// A partial repeated Ready cannot erase the durable committed suffix.
		// Its old uncommitted suffix can still be replaced, as with etcd replay.
		keep := s.result.Recovery.HardState.Commit - s.point.Index
		return s.truncate(min(keep, uint64(len(s.result.Recovery.Entries))))
	}
	if err := s.truncate(offset); err != nil {
		return err
	}
	size, err := strictEntrySize(entry)
	if err != nil {
		return err
	}
	if s.bytes > maxBytes || size > maxBytes-s.bytes {
		return unreadable("WAL tail exceeds recovery byte budget", nil)
	}
	s.bytes += size
	s.result.LastLog = model.Position{Index: entry.Index, Term: entry.Term}
	s.result.Recovery.Entries = append(s.result.Recovery.Entries, entry)
	s.result.Bytes = s.bytes
	return nil
}

func (s *strictLog) truncate(offset uint64) error {
	for _, old := range s.result.Recovery.Entries[offset:] {
		size, err := strictEntrySize(old)
		if err != nil {
			return err
		}
		s.bytes -= size
	}
	s.result.Recovery.Entries = s.result.Recovery.Entries[:offset]
	s.result.Bytes = s.bytes
	s.result.LastLog = model.Position{Index: s.point.Index, Term: s.point.Term}
	if offset > 0 {
		last := s.result.Recovery.Entries[offset-1]
		s.result.LastLog = model.Position{Index: last.Index, Term: last.Term}
	}
	return nil
}

// Exact acknowledged Ready retries preserve the already committed suffix.
func verifyStrictCommittedEntry(old, entry raftpb.Entry) error {
	if old.Term != entry.Term || old.Type != entry.Type || !bytes.Equal(old.Data, entry.Data) {
		return unreadable("WAL rewrites a committed entry", nil)
	}
	return nil
}

func validateStrictHardState(old, next raftpb.HardState) error {
	if next.Term < old.Term || next.Commit < old.Commit || next.Term == old.Term && old.Vote != 0 &&
		next.Vote != old.Vote {
		return unreadable("WAL hard state regresses or changes a durable vote", nil)
	}
	return nil
}

func strictEntrySize(entry raftpb.Entry) (uint64, error) {
	size := entry.Size()
	if size < 0 {
		return 0, fault.ErrUnreadable
	}
	return uint64(size), nil
}
