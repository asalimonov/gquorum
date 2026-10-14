package metastore

import (
	"encoding/json"
	"fmt"
	"math"
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

const (
	directoryMode   = 0o700
	fileMode        = 0o600
	uint64ByteCount = 8
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
	allBuckets         = [][]byte{
		bucketFormat,
		bucketHardState,
		bucketEntries,
		bucketSnapshot,
		bucketApplied,
		bucketState,
		bucketIdentity,
		bucketOverride,
		bucketLoadReplicas,
	}
	keySchemaVersion = []byte("schema_version")
	keyHardState     = []byte("current")
	keySnapshot      = []byte("current")
	keyApplied       = []byte("index")
	keyConfState     = []byte("confstate")
	keyState         = []byte("current")
	keyNodeName      = []byte("node_name")
	keyClusterID     = []byte("cluster_id")
	keyMemberID      = []byte("member_id")
	keyMemberSeen    = []byte("member_seen")
	keyOverride      = []byte("current")
)

// Identity is the durable node, cluster and meta-member pin, excluding process
// incarnation.
type Identity struct {
	NodeName  string `json:"node_name"`
	ClusterID string `json:"cluster_id"`
	MemberID  uint64 `json:"member_id"`
}

// IdentityMismatchError retains both the stored pin and requested identity.
type IdentityMismatchError struct{ Stored, Resolved Identity }

func (e *IdentityMismatchError) Error() string {
	return fmt.Sprintf("meta identity mismatch: stored %+v, resolved %+v", e.Stored, e.Resolved)
}

func (*IdentityMismatchError) Unwrap() error { return fault.ErrIdentityMismatch }

// Store owns the synchronous bbolt database for meta consensus, identity and
// local ledgers.
type Store struct {
	db        *bolt.DB
	path      string
	closeOnce sync.Once
	closeErr  error
}

// Open opens or creates a meta database and validates its disk schema.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fault.New(fault.Invalid, "empty meta database path", nil)
	}
	if err := os.MkdirAll(filepath.Dir(path), directoryMode); err != nil {
		return nil, fmt.Errorf("create meta parent: %w", err)
	}
	db, err := bolt.Open(path, fileMode, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open meta database: %w", err)
	}
	updateErr := db.Update(func(tx *bolt.Tx) error {
		if err := validateDiskSchema(tx); err != nil {
			return err
		}
		for _, name := range allBuckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketFormat).Put(keySchemaVersion, []byte("3"))
	})
	if updateErr != nil {
		_ = db.Close()
		return nil, updateErr
	}
	return &Store{db: db, path: path}, nil
}

// Close releases the database under serialized close ownership.
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
	db, err := bolt.Open(path, fileMode, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
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
	first, _ := b.Cursor().First()
	return Identity{NodeName: string(n), ClusterID: string(c), MemberID: btoi(b.Get(keyMemberID))}, first != nil
}

func readIdentityChecked(tx *bolt.Tx) (IdentityLookup, error) {
	id, found := readIdentity(tx)
	if !found {
		return IdentityLookup{Identity: id, Found: false}, nil
	}
	b := tx.Bucket(bucketIdentity)
	forEachErr := b.ForEach(func(key, value []byte) error {
		if value == nil || string(key) != string(keyNodeName) && string(key) != string(keyClusterID) &&
			string(key) != string(keyMemberID) && string(key) != string(keyMemberSeen) {
			return fault.New(fault.Unreadable, "unknown meta identity field", nil)
		}
		return nil
	})
	if forEachErr != nil {
		return IdentityLookup{Identity: id, Found: true}, forEachErr
	}
	if id.MemberID == 0 || id.MemberID == math.MaxUint64 || strings.TrimSpace(id.NodeName) == "" ||
		strings.TrimSpace(id.ClusterID) == "" || strings.ContainsRune(id.NodeName, '\x00') ||
		strings.ContainsRune(id.ClusterID, '\x00') || len(b.Get(keyMemberID)) != uint64ByteCount {
		return IdentityLookup{Identity: id, Found: true}, fault.New(fault.Unreadable,
			"incomplete or invalid meta identity pin", nil)
	}
	if raw := b.Get(keyMemberSeen); raw != nil && (len(raw) != 1 || raw[0] != 1) {
		return IdentityLookup{Identity: id, Found: true}, fault.New(fault.Unreadable,
			"invalid local membership marker", nil)
	}
	return IdentityLookup{Identity: id, Found: true}, nil
}

func validateDiskSchema(tx *bolt.Tx) error {
	b := tx.Bucket(bucketFormat)
	if b == nil {
		if first, _ := tx.Cursor().First(); first != nil {
			return fault.New(fault.Unsupported, "unversioned meta storage schema", nil)
		}
		return nil
	}
	if string(b.Get(keySchemaVersion)) != "3" {
		return fault.New(fault.Unsupported, "unsupported meta storage schema", nil)
	}
	nextForEachErr := b.ForEach(func(key, value []byte) error {
		if value == nil || string(key) != string(keySchemaVersion) {
			return fault.New(fault.Unreadable, "unknown meta format field", nil)
		}
		return nil
	})
	if nextForEachErr != nil {
		return nextForEachErr
	}
	for _, name := range allBuckets {
		if tx.Bucket(name) == nil {
			return fault.New(fault.Unreadable, "incomplete meta storage schema", nil)
		}
	}
	return nil
}

func diskIsFresh(tx *bolt.Tx) bool {
	fresh := true
	_ = tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		if string(name) == string(bucketFormat) {
			return nil
		}
		known := false
		for _, expected := range allBuckets {
			if string(name) == string(expected) {
				known = true
				break
			}
		}
		if first, _ := b.Cursor().First(); first != nil || !known {
			fresh = false
		}
		return nil
	})
	return fresh
}

// LoadIdentity reads and validates a retained meta pin without opening a
// writer.
func LoadIdentity(path string) (result IdentityLookup, err error) {
	err = withReadOnly(path, func(tx *bolt.Tx) error {
		if err := validateDiskSchema(tx); err != nil {
			return err
		}
		var readErr error
		result, readErr = readIdentityChecked(tx)
		return readErr
	})
	return result, err
}

// StoreIsFresh inspects retained database evidence without creating or
// repairing storage.
func StoreIsFresh(path string) (bool, error) {
	fresh := true
	err := withReadOnly(path, func(tx *bolt.Tx) error {
		if err := validateDiskSchema(tx); err != nil {
			fresh = false
			return err
		}
		fresh = diskIsFresh(tx)
		return nil
	})
	return fresh, err
}

// lineageVersion is an alias so JSON decoding retains the anonymous type's diagnostics.
type lineageVersion = struct {
	Version uint64 `json:"version"`
}

// PeekLocalLineage reads the highest retained topology version and pinned
// member ID.
func PeekLocalLineage(path string) (lineage LocalLineage, err error) {
	err = withReadOnly(path, func(tx *bolt.Tx) error {
		if err := validateDiskSchema(tx); err != nil {
			return err
		}
		id, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		lineage.MemberID = id.Identity.MemberID
		if b := tx.Bucket(bucketState); b != nil && b.Get(keyState) != nil {
			var views struct {
				Latest lineageVersion `json:"latest"`
				Target lineageVersion `json:"target"`
				Active lineageVersion `json:"active"`
			}
			if err := json.Unmarshal(b.Get(keyState), &views); err != nil {
				return fault.New(fault.Unreadable, "invalid meta materialized state", err)
			}
			lineage.Version = max(views.Latest.Version, views.Target.Version, views.Active.Version)
		}
		return nil
	})
	return lineage, err
}

// SaveApplication pairs materialized state with applied membership in one
// synchronous transaction.
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
	if next.Term < old.Term || next.Commit < old.Commit || next.Term == old.Term && old.Vote != 0 &&
		old.Vote != next.Vote {
		return fault.New(fault.Invalid, "hard state would regress or change vote in one term", nil)
	}
	return nil
}
