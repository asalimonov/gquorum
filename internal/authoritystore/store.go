// Package authoritystore retains node-local fencing evidence independently of
// the replaceable metadata database and application replica storage.
package authoritystore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

type identityPin struct {
	ClusterID string `json:"cluster_id"`
	NodeName  string `json:"node_name"`
	MemberID  uint64 `json:"member_id"`
}

func pin(id topology.Identity) identityPin {
	return identityPin{id.ClusterID, id.NodeName, id.MemberID}
}
func validIdentity(id topology.Identity) bool {
	return strings.TrimSpace(id.ClusterID) != "" && !strings.ContainsRune(id.ClusterID, 0) && strings.TrimSpace(id.NodeName) != "" && !strings.ContainsRune(id.NodeName, 0) && id.MemberID != 0 && strings.TrimSpace(id.InstanceID) != ""
}

type Store struct {
	mu       sync.RWMutex
	db       *bolt.DB
	identity topology.Identity
}

// Open never recreates absent retained evidence. The caller must establish fresh
// from all meta pins, load ledgers and retained resources before setting it true.
func Open(path string, identity topology.Identity, fresh bool) (*Store, error) {
	if strings.TrimSpace(path) == "" || !validIdentity(identity) {
		return nil, fault.ErrInvalid
	}
	created := false
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if !fresh {
			return nil, fault.New(fault.Unreadable, "retained authority database is missing", err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		created = true
		if err := f.Close(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fault.New(fault.Unreadable, "open authority database", err)
	}
	s := &Store{db: db, identity: identity}
	if created {
		err = db.Update(func(tx *bolt.Tx) error {
			for _, name := range []string{"format", "identity", "fences", "effects"} {
				if _, err := tx.CreateBucket([]byte(name)); err != nil {
					return err
				}
			}
			if err := tx.Bucket([]byte("format")).Put([]byte("schema_version"), []byte("1")); err != nil {
				return err
			}
			return put(tx.Bucket([]byte("identity")), []byte("current"), pin(identity))
		})
	}
	if err == nil {
		err = db.View(func(tx *bolt.Tx) error { return s.validate(tx) })
	}
	if err == nil && created {
		var dir *os.File
		dir, err = os.Open(filepath.Dir(path))
		if err == nil {
			err = dir.Sync()
			closeErr := dir.Close()
			if err == nil {
				err = closeErr
			}
		}
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}
func decode(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return fault.New(fault.Unreadable, "decode authority record", err)
	}
	if d.Decode(new(any)) != io.EOF {
		return fault.ErrUnreadable
	}
	return nil
}
func put(b *bolt.Bucket, key []byte, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}
func (s *Store) transaction(ctx context.Context, write bool, fn func(*bolt.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return fault.ErrStopped
	}
	call := func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(tx)
	}
	if write {
		return s.db.Update(call)
	}
	return s.db.View(call)
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	s.db = nil
	return nil
}

func (s *Store) validate(tx *bolt.Tx) error {
	for _, name := range []string{"format", "identity", "fences", "effects"} {
		if tx.Bucket([]byte(name)) == nil {
			return fault.ErrUnreadable
		}
	}
	if !bytes.Equal(tx.Bucket([]byte("format")).Get([]byte("schema_version")), []byte("1")) {
		return fault.ErrUnsupported
	}
	var id identityPin
	if err := decode(tx.Bucket([]byte("identity")).Get([]byte("current")), &id); err != nil {
		return err
	}
	if id != pin(s.identity) {
		return fault.ErrIdentityMismatch
	}
	if data := tx.Bucket([]byte("identity")).Get([]byte("metadata_acquired")); data != nil {
		var acquired identityPin
		if err := decode(data, &acquired); err != nil {
			return err
		}
		if acquired != id {
			return fault.ErrUnreadable
		}
	}
	if data := tx.Bucket([]byte("identity")).Get([]byte("readmission")); data != nil {
		var proof rebindRecord
		if err := decode(data, &proof); err != nil {
			return err
		}
		if proof.Granted != id || proof.Previous.ClusterID != id.ClusterID || proof.Previous.NodeName != id.NodeName || proof.Previous.MemberID == 0 || proof.Previous.MemberID >= id.MemberID || strings.TrimSpace(proof.AdmissionID) == "" {
			return fault.ErrUnreadable
		}
	}
	if err := tx.Bucket([]byte("fences")).ForEach(func(k, v []byte) error {
		var r FenceRecord
		if err := decode(v, &r); err != nil {
			return err
		}
		if r.Grant.Key.GroupID != string(k) || r.Sequence == 0 || s.hostGrant(r.Grant, false) != nil {
			return fault.ErrUnreadable
		}
		return nil
	}); err != nil {
		return err
	}
	return tx.Bucket([]byte("effects")).ForEach(func(k, v []byte) error {
		var r EffectRecord
		if err := decode(v, &r); err != nil {
			return err
		}
		if !bytes.Equal(k, effectKey(r.Grant.Key, r.Target)) || s.validateEffect(r) != nil {
			return fault.ErrUnreadable
		}
		floor, ok, err := current(tx, r.Grant.Key.GroupID)
		if err != nil {
			return err
		}
		if !ok || floor.Grant.Key.Revision < r.Grant.Key.Revision || floor.Sequence < r.Sequence ||
			floor.Grant.Key == r.Grant.Key && (!floor.Grant.Equal(r.Grant) || floor.Sequence != r.Sequence || !floor.Complete) {
			return fault.ErrUnreadable
		}
		if r.State == "fenced" && floor.Grant.Key.Revision <= r.Grant.Key.Revision ||
			r.State == "running" && floor.Complete && floor.Grant.Key.Revision > r.Grant.Key.Revision {
			return fault.ErrUnreadable
		}
		return nil
	})
}
