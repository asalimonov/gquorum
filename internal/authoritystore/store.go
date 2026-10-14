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

const (
	directoryMode = 0o700
	fileMode      = 0o600
)

type identityPin struct {
	ClusterID string `json:"cluster_id"`
	NodeName  string `json:"node_name"`
	MemberID  uint64 `json:"member_id"`
}

// Freshness records whether the caller authorized creating new retained evidence.
type Freshness uint8

// RetainedEvidence requires existing evidence; FreshEvidence permits initial creation.
const (
	RetainedEvidence Freshness = iota
	FreshEvidence
)

func pin(id topology.Identity) identityPin {
	return identityPin{id.ClusterID, id.NodeName, id.MemberID}
}
func validIdentity(id topology.Identity) bool {
	return id.MemberID != 0 && strings.TrimSpace(id.ClusterID) != "" && !strings.ContainsRune(id.ClusterID, 0) &&
		strings.TrimSpace(id.NodeName) != "" && !strings.ContainsRune(id.NodeName, 0) &&
		strings.TrimSpace(id.InstanceID) != ""
}

// Store owns the node-local durable identity, fence and host-effect evidence
// database.
type Store struct {
	mu       sync.RWMutex
	db       *bolt.DB
	identity topology.Identity
}

// Open never recreates absent retained evidence. FreshEvidence requires the
// caller to establish freshness from meta pins, load ledgers and retained state.
func Open(path string, identity topology.Identity, freshness Freshness) (*Store, error) {
	if freshness > FreshEvidence || strings.TrimSpace(path) == "" || !validIdentity(identity) {
		return nil, fault.ErrInvalid
	}
	status, err := prepareAuthorityFile(path, freshness)
	if err != nil {
		return nil, err
	}
	return openAuthorityDatabase(path, identity, status)
}

type databaseStatus uint8

const (
	retainedDatabase databaseStatus = iota
	createdDatabase
)

func openAuthorityDatabase(path string, identity topology.Identity, status databaseStatus) (*Store, error) {
	db, err := bolt.Open(path, fileMode, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fault.New(fault.Unreadable, "open authority database", err)
	}
	s := &Store{db: db, identity: identity}
	if status == createdDatabase {
		err = initializeAuthorityDatabase(db, identity)
	}
	if err == nil {
		err = db.View(func(tx *bolt.Tx) error { return s.validate(tx) })
	}
	if err == nil && status == createdDatabase {
		err = syncAuthorityDirectory(path)
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func initializeAuthorityDatabase(db *bolt.DB, identity topology.Identity) error {
	return db.Update(func(tx *bolt.Tx) error {
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

func prepareAuthorityFile(path string, freshness Freshness) (databaseStatus, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if freshness != FreshEvidence {
			return retainedDatabase, fault.New(fault.Unreadable, "retained authority database is missing", err)
		}
		if err := os.MkdirAll(filepath.Dir(path), directoryMode); err != nil {
			return retainedDatabase, err
		}
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, fileMode)
		if err != nil {
			return retainedDatabase, err
		}
		if err := f.Close(); err != nil {
			return retainedDatabase, err
		}
		return createdDatabase, nil
	} else if err != nil {
		return retainedDatabase, err
	}
	return retainedDatabase, nil
}

func syncAuthorityDirectory(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
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

func (s *Store) transaction(ctx context.Context, run func(*bolt.DB, func(*bolt.Tx) error) error,
	fn func(*bolt.Tx) error) error {
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
	return run(s.db, call)
}

func (s *Store) readTransaction(ctx context.Context, fn func(*bolt.Tx) error) error {
	return s.transaction(ctx, func(db *bolt.DB, call func(*bolt.Tx) error) error {
		return db.View(call)
	}, fn)
}

func (s *Store) writeTransaction(ctx context.Context, fn func(*bolt.Tx) error) error {
	return s.transaction(ctx, func(db *bolt.DB, call func(*bolt.Tx) error) error {
		return db.Update(call)
	}, fn)
}

// Close releases the authority database under serialized store ownership.
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
	if err := validateBuckets(tx); err != nil {
		return err
	}
	if err := s.validateIdentity(tx); err != nil {
		return err
	}
	if err := s.validateFences(tx); err != nil {
		return err
	}
	return s.validateEffects(tx)
}

func validateBuckets(tx *bolt.Tx) error {
	for _, name := range []string{"format", "identity", "fences", "effects"} {
		if tx.Bucket([]byte(name)) == nil {
			return fault.ErrUnreadable
		}
	}
	if !bytes.Equal(tx.Bucket([]byte("format")).Get([]byte("schema_version")), []byte("1")) {
		return fault.ErrUnsupported
	}
	return nil
}

func (s *Store) validateIdentity(tx *bolt.Tx) error {
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
		if proof.Granted != id || proof.Previous.ClusterID != id.ClusterID || proof.Previous.NodeName != id.NodeName ||
			proof.Previous.MemberID == 0 || proof.Previous.MemberID >= id.MemberID ||
			strings.TrimSpace(proof.AdmissionID) == "" {
			return fault.ErrUnreadable
		}
	}
	return nil
}

func (s *Store) validateFences(tx *bolt.Tx) error {
	forEachErr := tx.Bucket([]byte("fences")).ForEach(func(k, v []byte) error {
		var r FenceRecord
		if err := decode(v, &r); err != nil {
			return err
		}
		if r.Sequence == 0 || r.Grant.Key.GroupID != string(k) || s.hostGrant(r.Grant) != nil {
			return fault.ErrUnreadable
		}
		return nil
	})
	if forEachErr != nil {
		return forEachErr
	}
	return nil
}

func (s *Store) validateEffects(tx *bolt.Tx) error {
	return tx.Bucket([]byte("effects")).ForEach(func(k, v []byte) error {
		return s.validateStoredEffect(tx, k, v)
	})
}

func (s *Store) validateStoredEffect(tx *bolt.Tx, k, v []byte) error {
	var r EffectRecord
	if err := decode(v, &r); err != nil {
		return err
	}
	key, err := effectKey(r.Grant.Key, r.Target)
	if err != nil {
		return err
	}
	if !bytes.Equal(k, key) || s.validateEffect(r) != nil {
		return fault.ErrUnreadable
	}
	floor, err := current(tx, r.Grant.Key.GroupID)
	if err != nil {
		return err
	}
	if !floor.Found || floor.Record.Grant.Key.Revision < r.Grant.Key.Revision ||
		floor.Record.Sequence < r.Sequence ||
		floor.Record.Grant.Key == r.Grant.Key && (floor.Record.Sequence != r.Sequence || !floor.Record.Complete ||
			!floor.Record.Grant.Equal(r.Grant)) {
		return fault.ErrUnreadable
	}
	if r.State == effectFenced && floor.Record.Grant.Key.Revision <= r.Grant.Key.Revision || r.State == effectRunning &&
		floor.Record.Complete && floor.Record.Grant.Key.Revision > r.Grant.Key.Revision {
		return fault.ErrUnreadable
	}
	return nil
}
