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

type logReplay struct {
	metadata  []byte
	hardState raftpb.HardState
	entries   []raftpb.Entry
}

type replayInputs struct {
	files   []*os.File
	readers []fileutil.FileReader
}

type replayDecoder struct {
	decoder wal.Decoder
	point   walpb.Snapshot
	result  logReplay
	matched bool
}

// readLog uses the upstream decoder while retaining its descriptors through
// decoding, including partial acquisition and corruption returns.
func readLog(dir string, point walpb.Snapshot) (logReplay, error) {
	var inputs replayInputs
	defer closeLogFiles(&inputs.files)
	if err := inputs.open(dir, point.Index); err != nil {
		return logReplay{}, err
	}
	return decodeLog(wal.NewDecoder(inputs.readers...), point)
}

func replaySegments(dir string, index uint64) ([]segment, error) {
	segments, err := segmentNames(dir)
	if err != nil {
		return nil, err
	}
	start := -1
	for i, segment := range segments {
		if segment.index <= index {
			start = i
		}
	}
	if start < 0 {
		return nil, wal.ErrSnapshotNotFound
	}
	return segments[start:], nil
}

func (r *replayInputs) open(dir string, index uint64) error {
	segments, err := replaySegments(dir, index)
	if err != nil {
		return err
	}
	for i, segment := range segments {
		if i > 0 && segment.seq != segments[i-1].seq+1 {
			return errors.New("WAL segment sequence has a gap")
		}
		if err := r.openSegment(dir, segment); err != nil {
			return err
		}
	}
	return nil
}

func (r *replayInputs) openSegment(dir string, segment segment) error {
	f, err := os.Open(filepath.Join(dir, segment.name))
	if err != nil {
		return err
	}
	r.files = append(r.files, f)
	r.readers = append(r.readers, fileutil.NewFileReader(f))
	return nil
}

func decodeLog(decoder wal.Decoder, point walpb.Snapshot) (logReplay, error) {
	state := replayDecoder{decoder: decoder, point: point}
	if err := state.readRecords(); err != nil {
		return logReplay{hardState: state.result.hardState}, err
	}
	if !state.matched {
		return logReplay{hardState: state.result.hardState}, wal.ErrSnapshotNotFound
	}
	return state.result, nil
}

func (s *replayDecoder) readRecords() error {
	for {
		var record walpb.Record
		err := s.decoder.Decode(&record)
		if replayEnded(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.applyRecord(record); err != nil {
			return err
		}
	}
}

func (s *replayDecoder) applyRecord(record walpb.Record) error {
	switch record.Type {
	case wal.EntryType:
		return s.applyEntry(record.Data)
	case wal.StateType:
		return s.result.hardState.Unmarshal(record.Data)
	case wal.MetadataType:
		return s.applyMetadata(record.Data)
	case wal.CrcType:
		return s.applyCRC(record)
	case wal.SnapshotType:
		return s.applySnapshot(record.Data)
	default:
		return fmt.Errorf("unknown WAL record type %d", record.Type)
	}
}

func (s *replayDecoder) applyEntry(data []byte) error {
	var entry raftpb.Entry
	if err := entry.Unmarshal(data); err != nil {
		return err
	}
	if entry.Index <= s.point.Index {
		return nil
	}
	offset := entry.Index - s.point.Index - 1
	if offset > uint64(len(s.result.entries)) {
		return fmt.Errorf("WAL entry gap at %d", entry.Index)
	}
	s.result.entries = append(s.result.entries[:offset], entry)
	return nil
}

func (s *replayDecoder) applyMetadata(data []byte) error {
	if s.result.metadata != nil && !bytes.Equal(s.result.metadata, data) {
		return wal.ErrMetadataConflict
	}
	s.result.metadata = bytes.Clone(data)
	return nil
}

func (s *replayDecoder) applyCRC(record walpb.Record) error {
	if s.decoder.LastCRC() != 0 {
		if err := record.Validate(s.decoder.LastCRC()); err != nil {
			return err
		}
	}
	s.decoder.UpdateCRC(record.Crc)
	return nil
}

func (s *replayDecoder) applySnapshot(data []byte) error {
	var marker walpb.Snapshot
	if err := marker.Unmarshal(data); err != nil {
		return err
	}
	if marker.Index == s.point.Index {
		if marker.Term != s.point.Term {
			return wal.ErrSnapshotMismatch
		}
		s.matched = true
	}
	return nil
}

// closeLogFiles sees the complete acquisition list at return and preserves the
// former per-file defers' reverse close order, including partial acquisition.
func closeLogFiles(files *[]*os.File) {
	for i := len(*files) - 1; i >= 0; i-- {
		_ = (*files)[i].Close()
	}
}

func replayEnded(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
