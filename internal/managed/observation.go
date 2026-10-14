package managed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ObserveGroup serializes bounded native membership observations for an exact
// group catalog.
func (h *Host) ObserveGroup(parent context.Context, group string) (topology.GroupStatus, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctxHandle, err := h.beginWork(ctx)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return topology.GroupStatus{}, err
	}
	defer done()
	release, err := h.serializeObservation(ctx, group)
	if err != nil {
		return topology.GroupStatus{}, err
	}
	defer release()
	ids, err := h.cfg.Catalog(ctx, group)
	if err != nil {
		return topology.GroupStatus{}, err
	}
	result := topology.GroupStatus{GroupID: group}
	if len(ids) == 0 {
		return result, nil
	}
	reads := h.nativeObservationReads(group, ids)
	probes := h.collectNativeProbes(ctx, reads)
	if err := ctx.Err(); err != nil {
		return result, err
	}
	round, err := aggregateNativeObservation(result, probes)
	result = round.status
	if err != nil {
		return result, err
	}
	if result.LeaderID != 0 {
		result.HealthKnown, result.LiveVoters = true, round.live
		return result, nil
	}
	active, err := h.cfg.Active(ctx)
	if err != nil {
		return result, err
	}
	if round.observed == 0 && active.Group(group) == nil {
		return result, nil
	}
	result.Found = true
	return result, fault.ErrUnavailable
}

// Concurrent status users must not mistake another observer's in-flight read
// for a missing peer. Serialize rounds per group without holding the consensus
// effect gate: a membership effect itself calls ObserveGroup while owning it.
func (h *Host) serializeObservation(ctx context.Context, group string) (func(), error) {
	h.mu.Lock()
	gate := h.observationGroups[group]
	if gate == nil {
		gate = &groupOperation{token: make(chan struct{}, 1)}
		h.observationGroups[group] = gate
	}
	gate.users++
	h.mu.Unlock()
	acquired := false
	release := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if acquired {
			<-gate.token
		}
		gate.users--
		if gate.users == 0 {
			delete(h.observationGroups, group)
		}
	}
	select {
	case gate.token <- struct{}{}:
		acquired = true
		return release, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}

func (h *Host) readNativeObservation(parent context.Context, group string, id storage.Identity,
	attempt *observe.ReadAttempt) (observe.Probe, error) {
	ctxHandle, err := h.beginWork(parent)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return observe.Probe{}, err
	}
	defer done()
	peer, err := h.peerFor(ctx, id)
	if err != nil {
		return observe.Probe{}, err
	}
	query := load.ReplicaQuery{From: h.cfg.Identity(), To: peer}
	attempt.Dispatch()
	var reply load.ReplicaStatus
	if peer.Node == query.From {
		reply, err = h.ReadReplica(ctx, query)
	} else {
		reply, err = h.cfg.Transport.ReadReplica(ctx, query)
		if err != nil {
			// A remote refusal is not an affirmative identity attestation. A
			// restarted peer may legitimately reject our stale process pin.
			return observe.Probe{}, nil
		}
	}
	if err != nil {
		return observe.Probe{}, err
	}
	if reply.Peer != peer || reply.Found != reply.Status.Found || reply.Found && (reply.Status.GroupID != group ||
		reply.Status.HostNode != peer.Node.NodeName) {
		return observe.Probe{}, fault.ErrIdentityMismatch
	}
	return observe.Probe{Status: reply.Status.Clone()}, nil
}

type observedGroup struct {
	status   topology.GroupStatus
	live     map[uint64]bool
	observed int
}

func (h *Host) nativeObservationReads(group string, ids []storage.Identity) []observe.ReplicaRead {
	seen := make(map[storage.Identity]bool)
	reads := make([]observe.ReplicaRead, 0, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		reads = append(reads, observe.ReplicaRead{
			// Serialize reads of this durable identity even while its process
			// pin is being resolved. A stuck predecessor keeps its own bounded
			// permit.
			Identity: metrics.ReplicaIdentity{
				ClusterID: id.ClusterID,
				GroupID:   id.GroupID,
				NodeName:  id.NodeName,
				ReplicaID: id.ReplicaID,
			},
			Read: func(work context.Context, attempt *observe.ReadAttempt) (observe.Probe, error) {
				return h.readNativeObservation(work, group, id, attempt)
			},
		})
	}
	return reads
}

func aggregateNativeObservation(initial topology.GroupStatus, probes []observe.Probe) (observedGroup, error) {
	result := initial
	live := make(map[uint64]bool)
	observed := 0
	for _, probe := range probes {
		if errors.Is(probe.Error, fault.ErrIdentityMismatch) || errors.Is(probe.Error, fault.ErrUnreadable) {
			err := fmt.Errorf("load observation from %s replica %d: %w", probe.Identity.NodeName,
				probe.Identity.ReplicaID, probe.Error)
			return observedGroup{status: result}, err
		}
		if probe.Error != nil || !probe.Status.Found {
			continue
		}
		observed++
		live[probe.Identity.ReplicaID] = true
		if probe.Status.LeaderID != probe.Identity.ReplicaID {
			continue
		}
		if result.LeaderID != 0 && result.Term == probe.Status.Term && result.LeaderID != probe.Identity.ReplicaID {
			return observedGroup{status: result}, fault.ErrUnavailable
		}
		if result.LeaderID == 0 || probe.Status.Term > result.Term {
			result = probe.Status.Clone()
		}
	}
	return observedGroup{status: result, live: live, observed: observed}, nil
}

func (h *Host) collectNativeProbes(ctx context.Context, reads []observe.ReplicaRead) []observe.Probe {
	h.mu.Lock()
	start := h.observationCursor % len(reads)
	h.observationCursor = (start + 1) % len(reads)
	h.mu.Unlock()
	// An observation must leave time for the effect/planner that requested it.
	// Only the read sub-budget may expire with usable partial results; caller
	// or
	// owner cancellation still invalidates the entire observation.
	deadline, _ := ctx.Deadline()
	budget := min(h.cfg.Metrics.CollectTimeout, time.Until(deadline)/2)
	readCtx, stopReads := context.WithTimeout(ctx, budget)
	options := observe.ReadOptions{Start: start, Parallel: h.cfg.Metrics.MaxParallelReads,
		MaxAge: h.cfg.Metrics.MaxSampleAge}
	probes, _ := observe.CollectReads(readCtx, h.observationPool, reads, options)
	stopReads()
	return probes
}
