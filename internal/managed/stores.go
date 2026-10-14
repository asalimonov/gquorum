package managed

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// storeFor returns the store that holds groups of an approved storage class.
func (h *Host) storeFor(c topology.StorageClass) (storage.Store, error) {
	switch c {
	case topology.StorageDefault:
		return h.cfg.Store, nil
	case topology.StorageMemory:
		if h.cfg.MemoryStore == nil {
			return nil, fault.New(fault.Unsupported, "in-memory load storage is not configured", nil)
		}
		return h.cfg.MemoryStore, nil
	}
	return nil, topology.ValidateStorageClass(c)
}

type locatedStore struct {
	Store storage.Store
	Probe storage.Probe
}

// locate finds the store holding a group's local state, for paths that may run
// after the group left approved topology. Without any state it reports the
// default store. Both stores holding the group is an identity conflict.
func (h *Host) locate(ctx context.Context, groupID string) (locatedStore, error) {
	durable, err := h.cfg.Store.Probe(ctx, groupID)
	if err != nil || h.cfg.MemoryStore == nil {
		return locatedStore{Store: h.cfg.Store, Probe: durable}, err
	}
	memory, err := h.cfg.MemoryStore.Probe(ctx, groupID)
	switch {
	case err != nil:
		return locatedStore{Store: h.cfg.MemoryStore, Probe: memory}, err
	case durable.Exists && memory.Exists:
		return locatedStore{}, fault.New(fault.IdentityMismatch, "both load stores hold the group", nil)
	case memory.Exists:
		return locatedStore{Store: h.cfg.MemoryStore, Probe: memory}, nil
	}
	return locatedStore{Store: h.cfg.Store, Probe: durable}, nil
}

// retirementStores lists every store whose quarantined replicas this host
// collects.
func (h *Host) retirementStores() []storage.RetirementStore {
	if h.cfg.MemoryRetirement == nil {
		return []storage.RetirementStore{h.cfg.Retirement}
	}
	return []storage.RetirementStore{h.cfg.Retirement, h.cfg.MemoryRetirement}
}
