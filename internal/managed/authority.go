package managed

import (
	"context"
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type fenceWork struct {
	done     chan struct{}
	revision uint64 // latest requested revision; protected by Host.mu
	evidence topology.FenceEvidence
	err      error
}

// InstallFence validates an authenticated request and installs its exact group
// or host fence.
func (h *Host) InstallFence(parent context.Context, req load.FenceRequest) (load.FenceResult, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctxHandle, err := h.beginAuthorityWork(ctx)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.FenceResult{}, err
	}
	defer release()
	req.Grant = req.Grant.Clone()
	recipientMatches := completeIdentity(req.From) && req.To == h.cfg.Identity()
	if !recipientMatches || req.From.ClusterID != req.To.ClusterID ||
		req.Grant.Key.ClusterID != req.To.ClusterID {
		return load.FenceResult{}, fault.ErrIdentityMismatch
	}
	if req.Resource.GroupID != req.Grant.Key.GroupID || !slices.Contains(req.Grant.Resources, req.Resource) {
		return load.FenceResult{}, fault.ErrOperationAuthorization
	}
	if err := h.cfg.ValidateCourier(ctx, req.Grant, req.From); err != nil {
		return load.FenceResult{}, err
	}
	var evidence topology.FenceEvidence
	switch req.Resource.Kind {
	case topology.AuthorityHost:
		if req.ReplicaID != 0 || req.Resource.NodeName != req.To.NodeName {
			return load.FenceResult{}, fault.ErrIdentityMismatch
		}
		evidence, err = h.InstallHostFence(ctx, req.Grant)
	case topology.AuthorityGroup:
		if req.ReplicaID == 0 {
			return load.FenceResult{}, fault.ErrIdentityMismatch
		}
		evidence, err = h.installGroupFence(ctx, req)
	default:
		return load.FenceResult{}, fault.ErrInvalid
	}
	if err != nil {
		return load.FenceResult{}, err
	}
	return load.FenceResult{From: req.To, Evidence: evidence}, nil
}

func (h *Host) authorityStore() (*authoritystore.Store, error) {
	s := h.cfg.Authority()
	if s == nil {
		return nil, fault.ErrReplicaNotReady
	}
	return s, nil
}

// InstallHostFence persists admission blocking before it joins older owners.
// Cancellation only ends the caller's wait; completion belongs to the Host.
func (h *Host) InstallHostFence(parent context.Context,
	grant topology.AuthorityGrant) (topology.FenceEvidence, error) {
	ctxHandle, err := h.beginAuthorityWork(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	defer release()
	if err := topology.ValidateLoadID(grant.Key.GroupID); err != nil {
		return topology.FenceEvidence{}, err
	}
	cloned := grant.Clone()
	if err := h.cfg.ValidateGrant(ctx, cloned); err != nil {
		return topology.FenceEvidence{}, err
	}
	s, err := h.authorityStore()
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	floor, err := s.Fence(ctx, cloned)
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	if floor.Record.Complete {
		return s.CompleteFence(ctx, floor.Record.Grant.Key, floor.Record.Sequence)
	}
	h.mu.Lock()
	if h.authorityClosed || h.shutdownRequested || h.authorityContext.Err() != nil {
		h.mu.Unlock()
		return topology.FenceEvidence{}, fault.ErrStopped
	}
	if h.fenceWorks == nil {
		h.fenceWorks = make(map[string]*fenceWork)
	}
	work := h.fenceWorks[cloned.Key.GroupID]
	if work == nil {
		work = &fenceWork{done: make(chan struct{}), revision: cloned.Key.Revision}
		h.fenceWorks[cloned.Key.GroupID] = work
		// The courier already owns authority work, so authorityDone is open.
		h.authorityCount++
		go h.completeHostFence(h.authorityContext, s, cloned.Key.GroupID, work)
	} else {
		work.revision = max(work.revision, cloned.Key.Revision)
	}
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		return topology.FenceEvidence{}, ctx.Err()
	case <-work.done:
		if work.err != nil {
			return topology.FenceEvidence{}, work.err
		}
		if work.evidence.Key != cloned.Key {
			return topology.FenceEvidence{}, fault.ErrAuthorityFenced
		}
		return work.evidence, nil
	}
}

func (h *Host) completeHostFence(ctx context.Context, s *authoritystore.Store, group string, work *fenceWork) {
	for {
		floor, err := s.Current(ctx, group)
		if err == nil && !floor.Found {
			err = fault.ErrUnreadable
		}
		var evidence topology.FenceEvidence
		if err == nil {
			evidence, err = h.joinHostFence(ctx, s, floor.Record)
		}
		h.mu.Lock()
		active := ctx.Err() == nil
		fenced := active && errors.Is(err, fault.ErrAuthorityFenced)
		if active && (fenced || err == nil && work.revision > evidence.Key.Revision) {
			h.mu.Unlock()
			continue // a later request raised the floor while older work joined
		}
		work.evidence, work.err = evidence, err
		delete(h.fenceWorks, group)
		close(work.done)
		h.releaseWorkLocked(authorityWork)
		h.mu.Unlock()
		return
	}
}

func (h *Host) joinHostFence(ctx context.Context, s *authoritystore.Store,
	floor authoritystore.FenceRecord) (topology.FenceEvidence, error) {
	group := floor.Grant.Key.GroupID
	// Cancel a startup before waiting on the action gate: its caller may still
	// own that gate while waiting for a factory which honors this cancellation.
	h.mu.Lock()
	var attempt *releaseAttempt
	if slot := h.slots[group]; slot != nil && !slot.released &&
		slot.authorityKey.Revision < floor.Grant.Key.Revision &&
		slot.state != "ready" && slot.state != recoveryReady {
		attempt = h.beginStopLocked(slot)
	}
	h.mu.Unlock()
	ownedHandle, err := h.beginAuthorityOperation(ctx, group)
	owned, release := ownedHandle.Context, ownedHandle.Release
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	defer release()
	// A stop could have acquired the action gate after the first inspection and
	// left a still-owned Close behind when its caller timed out. Inspect again
	// under the gate before treating the group as joined.
	h.mu.Lock()
	if slot := h.slots[group]; slot != nil && !slot.released &&
		slot.authorityKey.Revision < floor.Grant.Key.Revision &&
		slot.state != "ready" && slot.state != recoveryReady {
		attempt = h.beginStopLocked(slot)
	}
	h.mu.Unlock()
	if attempt != nil {
		if err := waitRelease(owned, attempt); err != nil {
			return topology.FenceEvidence{}, err
		}
	}
	// The action gate and startup join prove that no older callback can produce
	// a later effect. Keep uncertain outcomes as fenced records, without
	// inventing
	// success receipts or deleting any partial storage/application evidence.
	running, err := s.Running(owned, group)
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	for _, record := range running {
		if record.Grant.Key.Revision >= floor.Grant.Key.Revision {
			return topology.FenceEvidence{}, fault.ErrAuthorityPending
		}
		if err := s.FenceEffect(owned, record, floor.Grant.Key); err != nil {
			return topology.FenceEvidence{}, err
		}
	}
	return s.CompleteFence(owned, floor.Grant.Key, floor.Sequence)
}

// Called while holding Host.mu so floor inspection and startup reservation are
// ordered against the fence worker's join of earlier reservations.
func (h *Host) startAuthority(ctx context.Context, id storage.Identity,
	node topology.Identity) (*topology.AuthorityGrant, error) {
	s, err := h.authorityStore()
	if err != nil {
		return nil, err
	}
	if s.Identity() != node {
		return nil, fault.ErrIdentityMismatch
	}
	floor, err := s.Current(ctx, id.GroupID)
	if err != nil {
		return nil, err
	}
	if !floor.Found || !floor.Record.Complete {
		return nil, fault.ErrAuthorityPending
	}
	g := floor.Record.Grant
	if !slices.Contains(g.Targets, topology.AuthorityTarget{NodeName: id.NodeName, MemberID: node.MemberID,
		ReplicaID: id.ReplicaID}) {
		return nil, fault.ErrAuthorityFenced
	}
	return &g, nil
}

// Recovery reads current metadata floors before acquiring the group gate: a
// fence may need that gate to join an earlier reservation. Healthy/accepted
// local
// owners continue independently of metadata quorum. Fresh creation is checked
// separately against the exact effect grant during storage preflight.
func (h *Host) reconcileStartupAuthority(ctx context.Context, ref load.ReplicaRef) error {
	h.mu.Lock()
	slot := h.slots[ref.GroupID]
	accepted := slot != nil && !slot.released && slot.identity.ReplicaID == ref.ReplicaID &&
		(slot.state == "starting" || slot.state == "ready" || slot.state == recoveryReady)
	h.mu.Unlock()
	if accepted {
		return nil
	}
	grants, err := h.cfg.AuthorityFloors(ctx)
	if err != nil {
		return err
	}
	for _, grant := range grants {
		if grant.Key.GroupID == ref.GroupID {
			_, err := h.InstallHostFence(ctx, grant)
			return err
		}
	}
	return fault.ErrAuthorityPending
}

func creationGrant(slot *replica) bool {
	g := slot.startGrant
	return g != nil && g.Action != nil && (g.Action.Kind == topology.StepCreateFoundingGroup ||
		g.Action.Kind == topology.StepPrepareReplica && g.Action.NodeName == slot.identity.NodeName &&
			g.Action.ReplicaID == slot.identity.ReplicaID)
}

func (h *Host) checkStartupFloor(slot *replica) error {
	s, err := h.authorityStore()
	if err != nil {
		return err
	}
	floor, err := s.Current(slot.ctx, slot.identity.GroupID)
	if err != nil {
		return err
	}
	if !floor.Found || !floor.Record.Complete || floor.Record.Grant.Key != slot.authorityKey ||
		slot.startGrant == nil ||
		!floor.Record.Grant.Equal(*slot.startGrant) {
		return fault.ErrAuthorityFenced
	}
	return nil
}

func (h *Host) installGroupFence(ctx context.Context, req load.FenceRequest) (topology.FenceEvidence, error) {
	// Native group work belongs to the participation lifetime. Only host
	// fences may complete after the replica has been fenced and joined.
	participatingHandle, workErr := h.beginWork(ctx)
	participating, done := participatingHandle.Context, participatingHandle.Release
	if workErr != nil {
		return topology.FenceEvidence{}, workErr
	}
	defer done()
	core, coreErr := h.actionCore(load.ActionRequest{
		To: load.Peer{Node: req.To, Replica: load.ReplicaRef{GroupID: req.Resource.GroupID,
			ReplicaID: req.ReplicaID}},
	})
	if coreErr != nil {
		return topology.FenceEvidence{}, coreErr
	}
	return core.InstallAuthority(participating, req.Grant)
}
