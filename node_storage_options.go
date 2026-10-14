package gquorum

import (
	"github.com/asalimonov/gquorum/storage"
)

// LoadStorageOptions selects Node-owned WAL storage or borrowed storage and
// retirement interfaces.
type LoadStorageOptions struct {
	// Select owned WAL storage, or a borrowed Store with explicit Retirement.
	WAL        *WALOptions
	Store      storage.Store
	Retirement storage.RetirementStore
}

// WALOptions configures the caller-selected load storage root and WAL writes.
type WALOptions struct {
	RootDir      string
	SegmentBytes int64
	Prewrite     *bool
	// DisableFSync skips fsync for WAL and applied-state writes. Writes still
	// reach the OS, but host crashes can lose acknowledged data or corrupt
	// state. Snapshot files, identity pins and directory synchronization are
	// unchanged.
	DisableFSync bool
}
