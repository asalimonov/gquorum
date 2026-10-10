package metastore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	bucketFormat       = []byte("format")
	bucketHardState    = []byte("hardstate")
	bucketEntries      = []byte("entries")
	bucketSnapshot     = []byte("snapshot")
	bucketApplied      = []byte("applied")
	bucketState        = []byte("state")
	bucketIdentity     = []byte("identity")
	bucketOverride     = []byte("serve_override")
	bucketLoadReplicas = []byte("load_replicas")
	allBuckets         = [][]byte{bucketFormat, bucketHardState, bucketEntries, bucketSnapshot, bucketApplied, bucketState, bucketIdentity, bucketOverride, bucketLoadReplicas}
	keySchemaVersion   = []byte("schema_version")
	keyHardState       = []byte("current")
	keySnapshot        = []byte("current")
	keyApplied         = []byte("index")
	keyConfState       = []byte("confstate")
	keyState           = []byte("current")
	keyNodeName        = []byte("node_name")
	keyClusterID       = []byte("cluster_id")
	keyMemberID        = []byte("member_id")
	keyMemberSeen      = []byte("member_seen")
	keyOverride        = []byte("current")
)

type Identity struct {
	NodeName  string `json:"node_name"`
	ClusterID string `json:"cluster_id"`
	MemberID  uint64 `json:"member_id"`
}

type IdentityMismatchError struct{ Stored, Resolved Identity }

func (e *IdentityMismatchError) Error() string {
	return fmt.Sprintf("meta identity mismatch: stored %+v, resolved %+v", e.Stored, e.Resolved)
}
func (e *IdentityMismatchError) Unwrap() error { return fault.ErrIdentityMismatch }

type Store struct {
	db        *bolt.DB
	path      string
	closeOnce sync.Once
	closeErr  error
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fault.New(fault.Invalid, "empty meta database path", nil)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create meta parent: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open meta database: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFormat)
		if b != nil && string(b.Get(keySchemaVersion)) != "1" {
			return fault.New(fault.Unsupported, "unsupported meta storage schema", nil)
		}
		if b == nil && tx.Bucket(bucketHardState) != nil {
			return fault.New(fault.Unsupported, "unversioned meta storage schema", nil)
		}
		for _, name := range allBuckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketFormat).Put(keySchemaVersion, []byte("1"))
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.db.Close() })
	return s.closeErr
}

func withReadOnly(path string, fn func(*bolt.Tx) error) error {
	if strings.TrimSpace(path) == "" {
		return fault.New(fault.Invalid, "empty meta database path", nil)
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.View(fn)
}

func readIdentity(tx *bolt.Tx) (Identity, bool) {
	b := tx.Bucket(bucketIdentity)
	if b == nil {
		return Identity{}, false
	}
	n, c := b.Get(keyNodeName), b.Get(keyClusterID)
	return Identity{NodeName: string(n), ClusterID: string(c), MemberID: btoi(b.Get(keyMemberID))}, n != nil || c != nil
}

func LoadIdentity(path string) (Identity, bool, error) {
	var id Identity
	var found bool
	err := withReadOnly(path, func(tx *bolt.Tx) error { id, found = readIdentity(tx); return nil })
	return id, found, err
}

func StoreIsFresh(path string) (bool, error) {
	fresh := true
	err := withReadOnly(path, func(tx *bolt.Tx) error {
		if _, pinned := readIdentity(tx); pinned {
			fresh = false
		}
		for _, name := range [][]byte{bucketHardState, bucketEntries, bucketSnapshot, bucketState, bucketApplied} {
			if b := tx.Bucket(name); b != nil {
				if k, _ := b.Cursor().First(); k != nil {
					fresh = false
				}
			}
		}
		return nil
	})
	return fresh, err
}

func PeekLocalLineage(path string) (version, memberID uint64, err error) {
	err = withReadOnly(path, func(tx *bolt.Tx) error {
		id, _ := readIdentity(tx)
		memberID = id.MemberID
		if b := tx.Bucket(bucketState); b != nil && b.Get(keyState) != nil {
			var views struct {
				Latest struct {
					Version uint64 `json:"version"`
				} `json:"latest"`
				Target struct {
					Version uint64 `json:"version"`
				} `json:"target"`
				Active struct {
					Version uint64 `json:"version"`
				} `json:"active"`
			}
			if err := json.Unmarshal(b.Get(keyState), &views); err != nil {
				return fault.New(fault.Unreadable, "invalid meta materialized state", err)
			}
			version = max(views.Latest.Version, views.Target.Version, views.Active.Version)
		}
		return nil
	})
	return
}

// SaveApplication pairs materialized state with applied membership in one synchronous transaction.
func (s *Store) SaveApplication(index uint64, cs raftpb.ConfState, data []byte) error {
	encoded, err := cs.Marshal()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		applied := tx.Bucket(bucketApplied)
		if btoi(applied.Get(keyApplied)) >= index {
			return nil
		}
		var hs raftpb.HardState
		if err := hs.Unmarshal(tx.Bucket(bucketHardState).Get(keyHardState)); err != nil {
			return err
		}
		if index > hs.Commit {
			return fault.New(fault.Invalid, "application index exceeds durable commit", nil)
		}
		if err := tx.Bucket(bucketState).Put(keyState, data); err != nil {
			return err
		}
		if err := applied.Put(keyApplied, itob(index)); err != nil {
			return err
		}
		if err := applied.Put(keyConfState, encoded); err != nil {
			return err
		}
		return recordMemberSeen(tx, cs)
	})
}

func validateHardState(old, next raftpb.HardState) error {
	if raft.IsEmptyHardState(next) {
		return nil
	}
	if next.Term < old.Term || next.Commit < old.Commit || next.Term == old.Term && old.Vote != 0 && old.Vote != next.Vote {
		return fault.New(fault.Invalid, "hard state would regress or change vote in one term", nil)
	}
	return nil
}
