package load

import "github.com/asalimonov/gquorum/storage"

// RecoveryInfo scopes acceptance of a retained source snapshot during recovery.
// The application must emit a portable destination snapshot after reconstruction.
type RecoveryInfo struct {
	RecoveryID    string
	Source        storage.Identity
	SnapshotIndex uint64
	CommitIndex   uint64
	CommitTerm    uint64
}
