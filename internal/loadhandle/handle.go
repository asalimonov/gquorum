// Package loadhandle holds the opaque representation shared by the public load
// facade and private owners. Defining load.Group over Handle does not inherit
// this package's privileged method set.
package loadhandle

import (
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
)

type Handle struct {
	core     *raftengine.Group
	identity storage.Identity
}

func New(core *raftengine.Group, identity storage.Identity) *Handle {
	return &Handle{core: core, identity: identity}
}
func (h *Handle) Core() *raftengine.Group {
	if h == nil {
		return nil
	}
	return h.core
}
func (h *Handle) Identity() storage.Identity {
	if h == nil {
		return storage.Identity{}
	}
	return h.identity
}
