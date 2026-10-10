package gquorum

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type ParkReason string

const (
	ParkStoreMissing     ParkReason = "store_missing"
	ParkStoreUnreadable  ParkReason = "store_unreadable"
	ParkIdentityMismatch ParkReason = "identity_mismatch"
)

// ParkedLoad describes a refused local boot, not a replicated cluster event.
// Entries clear only after a successful open of the approved identity.
type ParkedLoad struct {
	Identity storage.Identity `json:"identity"`
	Reason   ParkReason       `json:"reason"`
	Since    time.Time        `json:"since"`
}

type LoadParkedError struct {
	ParkedLoad
	Cause error
}

func (e *LoadParkedError) Error() string {
	return fmt.Sprintf("load group %s replica %d parked: %s", e.Identity.GroupID, e.Identity.ReplicaID, e.Reason)
}
func (e *LoadParkedError) Unwrap() error     { return e.Cause }
func (e *LoadParkedError) Is(err error) bool { return errors.Is(fault.ErrReplicaNotReady, err) }

func (r *Runtime) ParkedLoads() map[string]ParkedLoad {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Clone(r.parked)
}
func loadIdentity(opts load.Options) storage.Identity {
	return storage.Identity{NodeName: opts.NodeName, ClusterID: opts.ClusterID, GroupID: opts.GroupID, ReplicaID: opts.ReplicaID}
}
func (r *Runtime) parkLoad(opts load.Options, reason ParkReason, cause error) error {
	id := loadIdentity(opts)
	r.mu.Lock()
	park, exists := r.parked[id.GroupID]
	changed := !exists || park.Identity != id || park.Reason != reason
	if changed {
		park = ParkedLoad{Identity: id, Reason: reason, Since: time.Now().UTC()}
		r.parked[id.GroupID] = park
	}
	r.mu.Unlock()
	if changed && opts.Events != nil {
		opts.Events.Publish(events.Event{Type: events.RuntimeError, NodeName: id.NodeName, ClusterID: id.ClusterID, InstanceID: opts.InstanceID, GroupID: id.GroupID, ReplicaID: id.ReplicaID, Code: string(reason), State: "parked"})
	}
	return &LoadParkedError{ParkedLoad: park, Cause: cause}
}

type bootResult struct {
	origin   string
	previous uint64
}

// decideLoadBoot never mutates storage or the ledger. A prepared learner may
// legitimately have an empty log; an established replica may not refound one.
func decideLoadBoot(want storage.Identity, record meta.ReplicaRecord, recorded bool, probe storage.Probe, scope meta.LoadReplicaScope) (bootResult, ParkReason) {
	result := bootResult{origin: "fresh"}
	if recorded && record.ReplicaID != want.ReplicaID {
		result.previous = record.ReplicaID
		result.origin = "reseat"
	}
	if probe.Exists && probe.Identity != want {
		return result, ParkIdentityMismatch
	}
	if !probe.Exists {
		if recorded && record.ReplicaID == want.ReplicaID || scope.Started {
			return result, ParkStoreMissing
		}
		return result, ""
	}
	if !probe.HasLog && scope.Established {
		return result, ParkStoreMissing
	}
	if recorded && record.ReplicaID == want.ReplicaID {
		result.origin = "resume"
	}
	if !recorded {
		result.origin = "adopt"
	}
	return result, ""
}

func (r *Runtime) preflightLoad(m *meta.Group, opts load.Options) (bootResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	want := loadIdentity(opts)
	var scope meta.LoadReplicaScope
	var record meta.ReplicaRecord
	var recorded bool
	var err error
	if m != nil {
		if opts.InstanceID != m.Identity().InstanceID {
			return bootResult{}, fault.ErrIdentityMismatch
		}
		scope, err = m.LoadReplicaScope(ctx, want)
		if err != nil {
			return bootResult{}, err
		}
		record, recorded, err = m.LookupLoadReplica(ctx, opts.GroupID)
		if err != nil {
			return bootResult{}, r.parkLoad(opts, parkReason(err), err)
		}
	}
	probe, err := opts.Store.Probe(ctx, opts.GroupID)
	if err != nil {
		return bootResult{}, r.parkLoad(opts, parkReason(err), err)
	}
	result, reason := decideLoadBoot(want, record, recorded, probe, scope)
	if reason != "" {
		return bootResult{}, r.parkLoad(opts, reason, nil)
	}
	if m != nil && !probe.HasLog {
		if scope.Founding {
			if opts.Mode != load.Founding {
				return bootResult{}, fault.New(fault.Invalid, "new founding group requires founding mode", nil)
			}
			voters := map[uint64]bool{}
			for _, replica := range scope.Group.Replicas {
				if replica.Role == topology.ReplicaVoter {
					voters[replica.ReplicaID] = true
				}
			}
			if len(opts.Peers) != len(voters) {
				return bootResult{}, fault.New(fault.IdentityMismatch, "founding peers differ from approved voters", nil)
			}
			for _, peer := range opts.Peers {
				if !voters[peer.ID] {
					return bootResult{}, fault.New(fault.IdentityMismatch, "founding peers differ from approved voters", nil)
				}
			}
		} else if opts.Mode != load.Joining {
			return bootResult{}, fault.New(fault.Invalid, "new prepared replica requires joining mode", nil)
		}
	}
	return result, nil
}

func parkReason(err error) ParkReason {
	if errors.Is(err, fault.ErrIdentityMismatch) {
		return ParkIdentityMismatch
	}
	return ParkStoreUnreadable
}
