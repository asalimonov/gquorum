package raftengine

import (
	"slices"
	"sync"

	"github.com/asalimonov/gquorum/fault"
)

type Registry struct {
	mu     sync.RWMutex
	groups map[string]*Group
}

func NewRegistry() *Registry { return &Registry{groups: make(map[string]*Group)} }
func (r *Registry) Add(g *Group) error {
	if g == nil {
		return fault.ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.groups == nil {
		r.groups = make(map[string]*Group)
	}
	if _, exists := r.groups[g.opts.GroupID]; exists {
		return fault.New(fault.IdentityMismatch, "group already registered", nil)
	}
	r.groups[g.opts.GroupID] = g
	return nil
}
func (r *Registry) Get(id string) (*Group, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[id]
	return g, ok
}

// Remove forgets exactly the stopped replica, refusing stale callbacks.
func (r *Registry) Remove(id string, expectedReplicaID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[id]
	if !ok {
		return nil
	}
	if g.opts.ReplicaID != expectedReplicaID {
		return fault.ErrIdentityMismatch
	}
	select {
	case <-g.done:
	default:
		return fault.New(fault.Unavailable, "stop group before removing it", nil)
	}
	delete(r.groups, id)
	return nil
}
func (r *Registry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, 0, len(r.groups))
	for id := range r.groups {
		result = append(result, id)
	}
	slices.Sort(result)
	return result
}
