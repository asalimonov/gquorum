// Package util provides application helpers for constructing gquorum nodes.
//
//revive:disable-next-line:package-naming Preserve the established public helper package.
package util

import (
	"crypto/rand"
	"encoding/hex"
)

// NewInstanceID identifies one process incarnation. Call it once per process
// start, including retained restarts, and supply it as Identity.InstanceID.
// It is not a durable member/replica ID, request ID or authority revision.
func NewInstanceID() string {
	var id [16]byte
	// crypto/rand.Read fills the buffer or terminates the process on failure.
	_, _ = rand.Read(id[:])
	return hex.EncodeToString(id[:])
}
