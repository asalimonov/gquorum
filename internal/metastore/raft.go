// Package metastore persists synchronous meta consensus state and local identity records.
package metastore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (s *Store) SaveReady(entries []raftpb.Entry, hs raftpb.HardState) error {
	for i, e := range entries {
		if e.Index == 0 || e.Term == 0 || i > 0 && e.Index != entries[i-1].Index+1 {
			return fault.New(fault.Invalid, "entries must have positive contiguous indices and terms", nil)
		}
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		var oldHS raftpb.HardState
		if err := oldHS.Unmarshal(tx.Bucket(bucketHardState).Get(keyHardState)); err != nil {
			return err
		}
		if err := validateHardState(oldHS, hs); err != nil {
			return err
		}
		if len(entries) > 0 {
			eb := tx.Bucket(bucketEntries)
			if entries[0].Index <= oldHS.Commit {
				if entries[len(entries)-1].Index < oldHS.Commit {
					return fault.New(fault.Invalid, "cannot truncate committed entries", nil)
				}
				for _, e := range entries {
					if e.Index > oldHS.Commit {
						break
					}
					raw, err := e.Marshal()
					if err != nil {
						return err
					}
					if !bytes.Equal(eb.Get(itob(e.Index)), raw) {
						return fault.New(fault.Invalid, "cannot replace committed entry", nil)
					}
				}
			}
			from := itob(entries[0].Index)
			var stale [][]byte
			c := eb.Cursor()
			for k, _ := c.Seek(from); k != nil; k, _ = c.Next() {
				kc := make([]byte, len(k))
				copy(kc, k)
				stale = append(stale, kc)
			}
			for _, k := range stale {
				if err := eb.Delete(k); err != nil {
					return err
				}
			}
			for i := range entries {
				data, err := entries[i].Marshal()
				if err != nil {
					return fmt.Errorf("marshal entry %d: %w", entries[i].Index, err)
				}
				if err := eb.Put(itob(entries[i].Index), data); err != nil {
					return err
				}
			}
		}
		if !raft.IsEmptyHardState(hs) {
			data, err := hs.Marshal()
			if err != nil {
				return fmt.Errorf("marshal hardstate: %w", err)
			}
			if err := tx.Bucket(bucketHardState).Put(keyHardState, data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) SaveSnapshot(snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bucketSnapshot)
		if cur := sb.Get(keySnapshot); cur != nil {
			var existing raftpb.Snapshot
			if err := existing.Unmarshal(cur); err != nil {
				return fmt.Errorf("unmarshal stored snapshot: %w", err)
			}
			if snap.Metadata.Index < existing.Metadata.Index {
				return nil // stale, keep the newer durable snapshot
			}
		}
		data, err := snap.Marshal()
		if err != nil {
			return fmt.Errorf("marshal snapshot: %w", err)
		}
		return sb.Put(keySnapshot, data)
	})
}

func (s *Store) InstallSnapshot(snap raftpb.Snapshot, hs raftpb.HardState) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if btoi(tx.Bucket(bucketApplied).Get(keyApplied)) > snap.Metadata.Index {
			return nil
		}
		sb := tx.Bucket(bucketSnapshot)
		if cur := sb.Get(keySnapshot); cur != nil {
			var existing raftpb.Snapshot
			if err := existing.Unmarshal(cur); err != nil {
				return fmt.Errorf("unmarshal stored snapshot: %w", err)
			}
			if snap.Metadata.Index < existing.Metadata.Index {
				return nil // stale, keep the newer durable snapshot
			}
		}
		snapBytes, err := snap.Marshal()
		if err != nil {
			return fmt.Errorf("marshal snapshot: %w", err)
		}
		if err := sb.Put(keySnapshot, snapBytes); err != nil {
			return err
		}
		if len(snap.Data) > 0 {
			if err := tx.Bucket(bucketState).Put(keyState, snap.Data); err != nil {
				return err
			}
		}
		if err := recordMemberSeen(tx, snap.Metadata.ConfState); err != nil {
			return err
		}
		ab := tx.Bucket(bucketApplied)
		if v := ab.Get(keyApplied); len(v) != 8 || btoi(v) < snap.Metadata.Index {
			if err := ab.Put(keyApplied, itob(snap.Metadata.Index)); err != nil {
				return err
			}
			csBytes, err := snap.Metadata.ConfState.Marshal()
			if err != nil {
				return fmt.Errorf("marshal confstate: %w", err)
			}
			if err := ab.Put(keyConfState, csBytes); err != nil {
				return err
			}
		}
		hb := tx.Bucket(bucketHardState)
		effHS := hs
		if raft.IsEmptyHardState(effHS) {
			if cur := hb.Get(keyHardState); cur != nil {
				if err := effHS.Unmarshal(cur); err != nil {
					return fmt.Errorf("unmarshal stored hardstate: %w", err)
				}
			}
		}
		if effHS.Commit < snap.Metadata.Index {
			effHS.Commit = snap.Metadata.Index
		}
		var previousHS raftpb.HardState
		if err := previousHS.Unmarshal(hb.Get(keyHardState)); err != nil {
			return err
		}
		if err := validateHardState(previousHS, effHS); err != nil {
			return err
		}
		if !raft.IsEmptyHardState(effHS) {
			hd, err := effHS.Marshal()
			if err != nil {
				return fmt.Errorf("marshal hardstate: %w", err)
			}
			if err := hb.Put(keyHardState, hd); err != nil {
				return err
			}
		}
		eb := tx.Bucket(bucketEntries)
		var doomed [][]byte
		c := eb.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if btoi(k) > snap.Metadata.Index {
				break // keys are ordered; nothing beyond here to compact
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			doomed = append(doomed, kc)
		}
		for _, k := range doomed {
			if err := eb.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Snapshot() (raftpb.Snapshot, error) {
	var snap raftpb.Snapshot
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketSnapshot).Get(keySnapshot)
		if v == nil {
			return nil
		}
		return snap.Unmarshal(v)
	})
	return snap, err
}

func (s *Store) CompactEntries(upTo uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var snapshot raftpb.Snapshot
		if err := snapshot.Unmarshal(tx.Bucket(bucketSnapshot).Get(keySnapshot)); err != nil {
			return err
		}
		if upTo > snapshot.Metadata.Index {
			return fault.New(fault.Invalid, "compaction requires a durable covering snapshot", nil)
		}
		eb := tx.Bucket(bucketEntries)
		var doomed [][]byte
		c := eb.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if btoi(k) > upTo {
				break // keys are ordered; nothing beyond here to compact
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			doomed = append(doomed, kc)
		}
		for _, k := range doomed {
			if err := eb.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) AppliedIndex() (uint64, error) {
	var applied uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketApplied).Get(keyApplied)
		if len(v) == 8 {
			applied = btoi(v)
		}
		return nil
	})
	return applied, err
}

func (s *Store) SetApplied(idx uint64, cs raftpb.ConfState) error {
	csBytes, err := cs.Marshal()
	if err != nil {
		return fmt.Errorf("marshal confstate: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		ab := tx.Bucket(bucketApplied)
		if v := ab.Get(keyApplied); len(v) == 8 && btoi(v) >= idx {
			return nil
		}
		if err := ab.Put(keyApplied, itob(idx)); err != nil {
			return err
		}
		if err := ab.Put(keyConfState, csBytes); err != nil {
			return err
		}
		return recordMemberSeen(tx, cs)
	})
}

func (s *Store) AppliedConfState() (raftpb.ConfState, bool, error) {
	var (
		cs    raftpb.ConfState
		found bool
	)
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketApplied).Get(keyConfState)
		if v == nil {
			return nil
		}
		if err := cs.Unmarshal(v); err != nil {
			return fmt.Errorf("unmarshal confstate: %w", err)
		}
		found = true
		return nil
	})
	return cs, found, err
}

func (s *Store) SaveOverride(value int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bucketOverride)
		if value <= 0 {
			return ob.Delete(keyOverride)
		}
		return ob.Put(keyOverride, itob(uint64(value)))
	})
}

func (s *Store) LoadOverride() (int, error) {
	var v int
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOverride).Get(keyOverride)
		if len(b) == 8 {
			v = int(btoi(b)) //nolint:gosec // bit-pattern roundtrip with itob(uint64(value)) above; int is 64-bit on all supported platforms
		}
		return nil
	})
	return v, err
}

func (s *Store) SaveState(data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketState).Put(keyState, data)
	})
}

func (s *Store) LoadState() ([]byte, error) {
	var out []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketState).Get(keyState); v != nil {
			out = make([]byte, len(v))
			copy(out, v)
		}
		return nil
	})
	return out, err
}

func (s *Store) LoadAll() (raftpb.HardState, []raftpb.Entry, raftpb.Snapshot, error) {
	var (
		hs      raftpb.HardState
		snap    raftpb.Snapshot
		entries []raftpb.Entry
	)
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketHardState).Get(keyHardState); v != nil {
			if err := hs.Unmarshal(v); err != nil {
				return fmt.Errorf("unmarshal hardstate: %w", err)
			}
		}
		if v := tx.Bucket(bucketSnapshot).Get(keySnapshot); v != nil {
			if err := snap.Unmarshal(v); err != nil {
				return fmt.Errorf("unmarshal snapshot: %w", err)
			}
		}
		floor := snap.Metadata.Index
		return tx.Bucket(bucketEntries).ForEach(func(k, v []byte) error {
			if btoi(k) <= floor {
				return nil
			}
			var e raftpb.Entry
			if err := e.Unmarshal(v); err != nil {
				return fmt.Errorf("unmarshal entry %d: %w", btoi(k), err)
			}
			entries = append(entries, e)
			return nil
		})
	})
	return hs, entries, snap, err
}

func (s *Store) EnsureIdentity(nodeName, clusterID string, memberID uint64) error {
	if strings.TrimSpace(nodeName) == "" || strings.TrimSpace(clusterID) == "" || strings.ContainsRune(nodeName, '\x00') || strings.ContainsRune(clusterID, '\x00') || memberID == 0 || memberID == math.MaxUint64 {
		return fault.New(fault.Invalid, "meta identity must be complete", nil)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		ib := tx.Bucket(bucketIdentity)
		stored, pinned, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		if !pinned {
			if !storeIsFresh(tx) {
				return fault.New(fault.Unreadable, "unpinned retained meta state cannot be rebound", nil)
			}
			if err := ib.Put(keyNodeName, []byte(nodeName)); err != nil {
				return err
			}
			if err := ib.Put(keyClusterID, []byte(clusterID)); err != nil {
				return err
			}
			return ib.Put(keyMemberID, itob(memberID))
		}
		resolved := Identity{NodeName: nodeName, ClusterID: clusterID, MemberID: memberID}
		if stored != resolved {
			return &IdentityMismatchError{Stored: stored, Resolved: resolved}
		}
		return nil
	})
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
