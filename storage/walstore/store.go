// Package walstore implements node-local etcd WAL persistence with complete snapshots.
package walstore

import "github.com/asalimonov/gquorum/internal/walbackend"

// Options configures node ownership, the explicit storage root and WAL writes.
type Options = walbackend.Options

// Store owns node-local replica WALs, snapshots and applied-state databases.
type Store = walbackend.Store

// IdentityMismatchError preserves the stored and requested exact identities.
type IdentityMismatchError = walbackend.IdentityMismatchError

// Replica storage filenames retain their existing encodings and locations.
const (
	IdentityFile = walbackend.IdentityFile
	StateFile    = walbackend.StateFile
	WALDir       = walbackend.WALDir
	SnapDir      = walbackend.SnapDir
)

// Open validates options and acquires a node-local WAL store.
func Open(opts Options) (*Store, error) { return walbackend.Open(opts) }
