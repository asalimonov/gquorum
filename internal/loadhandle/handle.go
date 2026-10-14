// Package loadhandle holds the opaque representation shared by the public load
// facade and private owners. Defining load.Group over Handle does not inherit
// this package's privileged method set.
package loadhandle

import (
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
)

// Handle binds a stable public replica view to one exact engine and storage
// identity.
type Handle struct {
	core     *raftengine.Group
	identity storage.Identity
}

// New constructs a handle for one exact engine and replica identity.
func New(core *raftengine.Group, identity storage.Identity) *Handle {
	return &Handle{core: core, identity: identity}
}

// Core returns the bound engine, or nil for an absent handle.
func (h *Handle) Core() *raftengine.Group {
	if h == nil {
		return nil
	}
	return h.core
}

// Identity returns the bound storage identity, or zero for an absent handle.
func (h *Handle) Identity() storage.Identity {
	if h == nil {
		return storage.Identity{}
	}
	return h.identity
}
