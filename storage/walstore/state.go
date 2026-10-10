package walstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	bucketMeta   = []byte("meta")
	keyApplied   = []byte("applied")
	keyConfState = []byte("confstate")
	keyFloor     = []byte("floor")
	keySnapmark  = []byte("snapmark")
)

type diskIdentity struct {
	SchemaVersion int `json:"schema_version"`
	storage.Identity
}

func openState(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, fileMode, &bolt.Options{Timeout: time.Second, NoFreelistSync: true, FreelistType: bolt.FreelistMapType})
	if err != nil {
		if !errors.Is(err, bolt.ErrTimeout) {
			return nil, unreadable("open WAL state", err)
		}
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if old := tx.Bucket(bucketMeta); old != nil && old.Get([]byte("schema_version")) == nil {
			return fault.New(fault.Unsupported, "unversioned WAL state", nil)
		}
		b, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if v := b.Get([]byte("schema_version")); v != nil && string(v) != "1" {
			return fault.New(fault.Unsupported, "unsupported WAL state schema", nil)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("settings")); err != nil {
			return err
		}
		return b.Put([]byte("schema_version"), []byte("1"))
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func putU64(b *bolt.Bucket, key []byte, n uint64) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, n)
	return b.Put(key, data)
}
func getU64(b *bolt.Bucket, key []byte) uint64 {
	if b == nil {
		return 0
	}
	raw := b.Get(key)
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func readIdentity(dir string) (storage.Identity, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, IdentityFile))
	if errors.Is(err, os.ErrNotExist) {
		return storage.Identity{}, false, nil
	}
	if err != nil {
		return storage.Identity{}, false, err
	}
	var pin diskIdentity
	if err := json.Unmarshal(data, &pin); err != nil {
		return pin.Identity, false, unreadable("invalid identity pin", err)
	}
	if pin.SchemaVersion != 1 {
		return pin.Identity, false, fault.New(fault.Unsupported, "unsupported WAL identity schema", nil)
	}
	id := pin.Identity
	if id.NodeName == "" || id.ClusterID == "" || id.GroupID == "" || id.ReplicaID == 0 {
		return id, false, unreadable("incomplete identity pin", nil)
	}
	return id, true, nil
}

func writeIdentity(dir string, id storage.Identity) error {
	data, err := json.Marshal(diskIdentity{SchemaVersion: 1, Identity: id})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".identity-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(fileMode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, IdentityFile)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func readApplied(db *bolt.DB) (uint64, raftpb.ConfState, error) {
	var index uint64
	var cs raftpb.ConfState
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if b == nil || string(b.Get([]byte("schema_version"))) != "1" {
			return fault.New(fault.Unsupported, "unsupported WAL state schema", nil)
		}
		if raw := b.Get(keyApplied); len(raw) != 0 && len(raw) != 8 {
			return unreadable("invalid applied marker", nil)
		}
		index = getU64(b, keyApplied)
		if index > 0 && b.Get(keyConfState) == nil {
			return unreadable("applied marker lacks ConfState", nil)
		}
		return cs.Unmarshal(b.Get(keyConfState))
	})
	return index, cs, err
}
