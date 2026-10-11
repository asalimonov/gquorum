package managed

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (h *Host) Retire(parent context.Context, request load.RetirementRequest) (load.RetirementResult, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctx, done, err := h.beginOperation(ctx, request.Identity.GroupID)
	if err != nil {
		return load.RetirementResult{}, err
	}
	defer done()
	return h.retire(ctx, request)
}

func (h *Host) retirementAuthority(ctx context.Context, req load.RetirementRequest) error {
	if req.Reason != load.RetirementTopology {
		return fault.ErrUnsupported
	} // Readmission uses its separate authenticated grant path.
	if req.Authorization == nil || req.AdmissionID != "" || req.PreviousMemberID != 0 || req.GrantedMemberID != 0 {
		return fault.ErrOperatorAuthorization
	}
	a := *req.Authorization
	if a.Kind != topology.StepCleanupReplicaState && a.Kind != topology.StepDeleteGroupState {
		return fault.ErrOperatorAuthorization
	}
	if err := h.cfg.Authorize(ctx, a); err != nil {
		return err
	}
	own := h.cfg.Identity()
	if req.Identity.NodeName != own.NodeName || req.Identity.ClusterID != own.ClusterID || req.Identity.GroupID != a.GroupID || req.Identity.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	targets, err := h.cfg.ActionTargets(ctx, a)
	if err != nil {
		return err
	}
	if !slices.Contains(targets, req.Identity) {
		return fault.ErrOperatorAuthorization
	}
	return ctx.Err()
}

func (h *Host) otherIdentityAllowed(ctx context.Context, a topology.AuthorizedStep, wanted, other storage.Identity) error {
	if wanted.NodeName != other.NodeName || wanted.ClusterID != other.ClusterID || wanted.GroupID != other.GroupID || other.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	if a.Kind == topology.StepDeleteGroupState {
		targets, err := h.cfg.ActionTargets(ctx, a)
		if err != nil {
			return err
		}
		if !slices.Contains(targets, other) {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	_, err := h.cfg.Scope(ctx, other)
	return err
}

func (h *Host) stopRetiringIdentity(ctx context.Context, id storage.Identity, a topology.AuthorizedStep) error {
	h.mu.Lock()
	slot := h.slots[id.GroupID]
	released := slot != nil && slot.released
	h.mu.Unlock()
	if slot == nil || released {
		return nil
	}
	if slot.identity != id {
		return h.otherIdentityAllowed(ctx, a, id, slot.identity)
	}
	return h.stopReplica(ctx, load.ReplicaRef{GroupID: id.GroupID, ReplicaID: id.ReplicaID}, StopOperator)
}

func (h *Host) checkedRetirementProbe(ctx context.Context, req load.RetirementRequest) (storage.Probe, error) {
	probe, err := h.cfg.Store.Probe(ctx, req.Identity.GroupID)
	if err != nil {
		return probe, err
	}
	if probe.Exists && probe.Identity != req.Identity {
		err = h.otherIdentityAllowed(ctx, *req.Authorization, req.Identity, probe.Identity)
	}
	return probe, err
}

func (h *Host) retire(ctx context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
	// Own the proof pointer before handing a distinct copy to application code.
	req = cloneRetirement(req)
	if err := h.retirementAuthority(ctx, req); err != nil {
		return load.RetirementResult{}, err
	}
	if err := h.stopRetiringIdentity(ctx, req.Identity, *req.Authorization); err != nil {
		return load.RetirementResult{}, err
	}
	if err := h.retirementAuthority(ctx, req); err != nil {
		return load.RetirementResult{}, err
	}
	if _, err := h.checkedRetirementProbe(ctx, req); err != nil {
		return load.RetirementResult{}, err
	}
	// A ledger is evidence, never permission to clean a different local identity.
	if err := h.checkRetirementLedger(ctx, req, false); err != nil {
		return load.RetirementResult{}, err
	}
	result := retirementProof(req)
	if h.cfg.RetireApplication != nil {
		call, cancel := context.WithTimeout(ctx, h.cfg.CallbackTimeout)
		var err error
		result, err = h.cfg.RetireApplication(call, cloneRetirement(req))
		if err == nil {
			err = call.Err()
		}
		cancel()
		if err != nil {
			return load.RetirementResult{}, err
		}
	}
	if !matchingRetirement(req, result) {
		return load.RetirementResult{}, fault.ErrOperatorAuthorization
	}
	if err := h.retirementAuthority(ctx, req); err != nil {
		return load.RetirementResult{}, err
	}
	probe, err := h.checkedRetirementProbe(ctx, req)
	if err != nil {
		return load.RetirementResult{}, err
	}
	if probe.Exists && probe.Identity == req.Identity {
		if _, err := h.cfg.Store.Quarantine(ctx, req.Identity.GroupID, req.Identity.ReplicaID); err != nil {
			return load.RetirementResult{}, err
		}
	}
	if err := h.checkRetirementLedger(ctx, req, true); err != nil {
		return load.RetirementResult{}, err
	}
	h.mu.Lock()
	if parked, found := h.parked[req.Identity.GroupID]; found && parked.Identity == req.Identity {
		delete(h.parked, req.Identity.GroupID)
	}
	h.mu.Unlock()
	return retirementProof(req), nil
}

func (h *Host) checkRetirementLedger(ctx context.Context, req load.RetirementRequest, forget bool) error {
	record, found, err := h.cfg.LookupRecord(ctx, req.Identity.GroupID)
	if err != nil || !found {
		return err
	}
	id := storage.Identity{NodeName: record.NodeName, ClusterID: record.ClusterID, GroupID: req.Identity.GroupID, ReplicaID: record.ReplicaID}
	if id != req.Identity {
		return h.otherIdentityAllowed(ctx, *req.Authorization, req.Identity, id)
	}
	if forget {
		return h.cfg.ForgetReplica(ctx, *req.Authorization, id.ReplicaID)
	}
	return nil
}

func cloneRetirement(req load.RetirementRequest) load.RetirementRequest {
	if req.Authorization != nil {
		proof := *req.Authorization
		req.Authorization = &proof
	}
	return req
}
func retirementProof(req load.RetirementRequest) load.RetirementResult {
	owned := cloneRetirement(req)
	return load.RetirementResult{Identity: owned.Identity, Reason: owned.Reason, Authorization: owned.Authorization, AdmissionID: owned.AdmissionID, PreviousMemberID: owned.PreviousMemberID, GrantedMemberID: owned.GrantedMemberID, Verified: true}
}
func matchingRetirement(req load.RetirementRequest, result load.RetirementResult) bool {
	if !result.Verified || result.Identity != req.Identity || result.Reason != req.Reason || result.AdmissionID != req.AdmissionID || result.PreviousMemberID != req.PreviousMemberID || result.GrantedMemberID != req.GrantedMemberID {
		return false
	}
	if req.Authorization == nil || result.Authorization == nil {
		return req.Authorization == nil && result.Authorization == nil
	}
	return *req.Authorization == *result.Authorization
}

func (h *Host) collectLoop(ctx context.Context) {
	tick := time.NewTicker(h.cfg.RetirementInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := h.CollectRetired(ctx); err != nil && ctx.Err() == nil {
			h.cfg.Logger.Warn("retired replica collection failed", "error", err)
		}
	}
}

func (h *Host) CollectRetired(ctx context.Context) error {
	ctx, done, err := h.beginWork(ctx)
	if err != nil {
		return err
	}
	defer done()
	probe, cancel := context.WithTimeout(ctx, h.cfg.EffectTimeout)
	records, err := h.cfg.Retirement.Retired(probe)
	cancel()
	if err != nil {
		return err
	}
	var failures []error
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		own := h.cfg.Identity()
		if record.Identity.NodeName != own.NodeName || record.Identity.ClusterID != own.ClusterID {
			continue
		}
		if err := h.collectOne(ctx, record); err != nil {
			failures = append(failures, err)
			if h.cfg.Events != nil {
				h.cfg.Events.Publish(events.Event{Type: events.RuntimeError, NodeName: own.NodeName, ClusterID: own.ClusterID, InstanceID: own.InstanceID, GroupID: record.Identity.GroupID, ReplicaID: record.Identity.ReplicaID, Code: "retention_failed"})
			}
		}
	}
	return errors.Join(failures...)
}

func (h *Host) collectOne(parent context.Context, record storage.RetiredReplica) error {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctx, done, err := h.beginOperation(ctx, record.Identity.GroupID)
	if err != nil {
		return err
	}
	defer done()
	select {
	case h.retentionGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	call, stop := context.WithTimeout(ctx, h.cfg.CallbackTimeout)
	decision, err := h.cfg.Retention(call, policy.RetentionInput{Now: time.Now().UTC(), Retired: record})
	if err == nil {
		err = call.Err()
	}
	stop()
	<-h.retentionGate
	if err != nil {
		return err
	}
	if !decision.Collect || time.Now().Before(decision.NotBefore) {
		return nil
	}
	h.mu.Lock()
	slot := h.slots[record.Identity.GroupID]
	live := slot != nil && slot.identity == record.Identity && !slot.released
	h.mu.Unlock()
	if live {
		return fault.ErrReplicaNotReady
	}
	ledger, found, err := h.cfg.LookupRecord(ctx, record.Identity.GroupID)
	if err != nil {
		return err
	}
	if found && ledger.ReplicaID == record.Identity.ReplicaID {
		return fault.ErrReplicaNotReady
	}
	probe, err := h.cfg.Store.Probe(ctx, record.Identity.GroupID)
	if err != nil {
		return err
	}
	if probe.Exists && probe.Identity == record.Identity {
		return fault.ErrReplicaNotReady
	}
	return h.cfg.Retirement.Collect(ctx, record)
}
