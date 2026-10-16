package managed

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (h *Host) retirementAuthority(ctx context.Context, req load.RetirementRequest) error {
	a, err := retirementAction(req)
	if err != nil {
		return err
	}
	if err := h.checkRetirementAuthority(ctx, req); err != nil {
		return err
	}
	own := h.cfg.Identity()
	if req.Identity.NodeName != own.NodeName || req.Identity.ClusterID != own.ClusterID ||
		req.Identity.GroupID != a.GroupID || req.Identity.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	return retirementTarget(ctx, a, req.Identity, req.Grant)
}

func retirementAction(req load.RetirementRequest) (topology.AuthorizedStep, error) {
	if req.Reason != load.RetirementTopology {
		return topology.AuthorizedStep{}, fault.ErrUnsupported
	} // Readmission uses its separate authenticated grant path.
	if req.Authorization == nil || req.AdmissionID != "" || req.PreviousMemberID != 0 || req.GrantedMemberID != 0 {
		return topology.AuthorizedStep{}, fault.ErrOperatorAuthorization
	}
	a := *req.Authorization
	if a.Kind != topology.StepCleanupReplicaState && a.Kind != topology.StepDeleteGroupState {
		return topology.AuthorizedStep{}, fault.ErrOperatorAuthorization
	}
	return a, nil
}

func retirementTarget(ctx context.Context, a topology.AuthorizedStep, identity storage.Identity,
	grant topology.AuthorityGrant) error {
	targets, err := actionTargets(ctx, a, grant)
	if err != nil {
		return err
	}
	if !slices.Contains(targets, identity) {
		return fault.ErrOperatorAuthorization
	}
	return ctx.Err()
}

type retirementIdentities struct {
	wanted, other storage.Identity
}

func (h *Host) otherIdentityAllowed(ctx context.Context, a topology.AuthorizedStep, identities retirementIdentities,
	grant topology.AuthorityGrant) error {
	wanted, other := identities.wanted, identities.other
	if wanted.NodeName != other.NodeName || wanted.ClusterID != other.ClusterID || wanted.GroupID != other.GroupID ||
		other.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	if a.Kind == topology.StepDeleteGroupState {
		return deleteIdentityAllowed(ctx, a, other, grant)
	}
	_, err := h.cfg.Scope(ctx, other)
	return err
}

func deleteIdentityAllowed(ctx context.Context, a topology.AuthorizedStep, other storage.Identity,
	grant topology.AuthorityGrant) error {
	targets, err := actionTargets(ctx, a, grant)
	if err != nil {
		return err
	}
	if !slices.Contains(targets, other) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func (h *Host) stopRetiringIdentity(ctx context.Context, id storage.Identity, a topology.AuthorizedStep,
	grant topology.AuthorityGrant) error {
	h.mu.Lock()
	slot := h.slots[id.GroupID]
	released := slot != nil && slot.released
	h.mu.Unlock()
	if slot == nil || released {
		return nil
	}
	if slot.identity != id {
		return h.otherIdentityAllowed(ctx, a, retirementIdentities{id, slot.identity}, grant)
	}
	return h.stopOwnedReplica(ctx, load.ReplicaRef{GroupID: id.GroupID, ReplicaID: id.ReplicaID}, StopOperator)
}

type retirementProbe struct {
	store storage.Store
	probe storage.Probe
}

func (h *Host) checkedRetirementProbe(ctx context.Context, req load.RetirementRequest) (retirementProbe, error) {
	located, err := h.locate(ctx, req.Identity.GroupID)
	store, probe := located.Store, located.Probe
	if err != nil {
		return retirementProbe{store: store, probe: probe}, err
	}
	if probe.Exists && probe.Identity != req.Identity {
		identities := retirementIdentities{req.Identity, probe.Identity}
		err = h.otherIdentityAllowed(ctx, *req.Authorization, identities, req.Grant)
	}
	return retirementProbe{store: store, probe: probe}, err
}

func (h *Host) retire(ctx context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
	// Own the proof pointer before handing a distinct copy to application code.
	cloned := cloneRetirement(req)
	if err := h.prepareRetirement(ctx, cloned); err != nil {
		return load.RetirementResult{}, err
	}
	if err := h.checkRetirementResources(ctx, cloned); err != nil {
		return load.RetirementResult{}, err
	}
	if err := h.retireApplicationVerified(ctx, cloned); err != nil {
		return load.RetirementResult{}, err
	}
	if err := h.finishRetirement(ctx, cloned); err != nil {
		return load.RetirementResult{}, err
	}
	return retirementProof(cloned), nil
}

func (h *Host) prepareRetirement(ctx context.Context, req load.RetirementRequest) error {
	if err := h.retirementAuthority(ctx, req); err != nil {
		return err
	}
	if err := h.stopRetiringIdentity(ctx, req.Identity, *req.Authorization, req.Grant); err != nil {
		return err
	}
	return h.retirementAuthority(ctx, req)
}

func (h *Host) checkRetirementResources(ctx context.Context, req load.RetirementRequest) error {
	if _, err := h.checkedRetirementProbe(ctx, req); err != nil {
		return err
	}
	// A ledger is evidence, never permission to clean a different local identity.
	return h.checkRetirementLedger(ctx, req)
}

func (h *Host) retireApplicationVerified(ctx context.Context, req load.RetirementRequest) error {
	result, err := h.callRetireApplication(ctx, req)
	if err != nil {
		return err
	}
	if !matchingRetirement(req, result) {
		return fault.ErrOperatorAuthorization
	}
	return nil
}

func (h *Host) finishRetirement(ctx context.Context, req load.RetirementRequest) error {
	if err := h.retirementAuthority(ctx, req); err != nil {
		return err
	}
	if err := h.quarantineRetirement(ctx, req); err != nil {
		return err
	}
	if err := h.forgetRetirementLedger(ctx, req); err != nil {
		return err
	}
	h.clearRetirementPark(req.Identity)
	return nil
}

func (h *Host) quarantineRetirement(ctx context.Context, req load.RetirementRequest) error {
	located, err := h.checkedRetirementProbe(ctx, req)
	if err != nil {
		return err
	}
	if located.probe.Exists && located.probe.Identity == req.Identity {
		_, err = located.store.Quarantine(ctx, req.Identity.GroupID, req.Identity.ReplicaID)
		return err
	}
	return nil
}

func (h *Host) clearRetirementPark(identity storage.Identity) {
	h.mu.Lock()
	if parked, found := h.parked[identity.GroupID]; found && parked.Identity == identity {
		delete(h.parked, identity.GroupID)
	}
	h.mu.Unlock()
}

func (h *Host) callRetireApplication(ctx context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
	if h.cfg.RetireApplication == nil {
		return retirementProof(req), ctx.Err()
	}
	call, cancel := context.WithTimeout(ctx, h.cfg.CallbackTimeout)
	defer cancel()
	result, err := h.cfg.RetireApplication(call, cloneRetirement(req))
	if err == nil {
		err = call.Err()
	}
	return result, err
}

func (h *Host) checkRetirementLedger(ctx context.Context, req load.RetirementRequest) error {
	_, err := h.retirementLedgerMatches(ctx, req)
	return err
}

func (h *Host) forgetRetirementLedger(ctx context.Context, req load.RetirementRequest) error {
	matches, err := h.retirementLedgerMatches(ctx, req)
	if err != nil || !matches {
		return err
	}
	return h.cfg.ForgetGrantedReplica(ctx, req.Grant, req.Identity.ReplicaID)
}

func (h *Host) retirementLedgerMatches(ctx context.Context, req load.RetirementRequest) (bool, error) {
	record, err := h.cfg.LookupRecord(ctx, req.Identity.GroupID)
	if err != nil || !record.Found {
		return false, err
	}
	id := storage.Identity{
		NodeName: record.Record.NodeName, ClusterID: record.Record.ClusterID,
		GroupID: req.Identity.GroupID, ReplicaID: record.Record.ReplicaID,
	}
	if id != req.Identity {
		return false, h.otherIdentityAllowed(ctx, *req.Authorization, retirementIdentities{req.Identity, id}, req.Grant)
	}
	return true, nil
}

func cloneRetirement(req load.RetirementRequest) load.RetirementRequest {
	req.Grant = req.Grant.Clone()
	if req.Authorization != nil {
		proof := *req.Authorization
		req.Authorization = &proof
	}
	return req
}

func retirementProof(req load.RetirementRequest) load.RetirementResult {
	return req.Result()
}

func matchingRetirement(req load.RetirementRequest, result load.RetirementResult) bool {
	grantMatches := result.Grant.Equal(req.Grant)
	if !grantMatches || !result.Verified || result.Identity != req.Identity ||
		result.Reason != req.Reason || result.AdmissionID != req.AdmissionID ||
		result.PreviousMemberID != req.PreviousMemberID || result.GrantedMemberID != req.GrantedMemberID {
		return false
	}
	return matchingRetirementAuthorization(req.Authorization, result.Authorization)
}

func matchingRetirementAuthorization(requested, returned *topology.AuthorizedStep) bool {
	if requested == nil || returned == nil {
		return requested == nil && returned == nil
	}
	return *requested == *returned
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

// CollectRetired checks local retirement records and collects those permitted
// by verified retention decisions.
func (h *Host) CollectRetired(parent context.Context) error {
	ctxHandle, err := h.beginAuthorityWork(parent)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer done()
	resumed := h.resumeCollections(ctx)
	collected := h.collectRetiredStores(ctx)
	return errors.Join(resumed, collected, h.reportCollections(ctx))
}

func (h *Host) collectRetiredStores(ctx context.Context) error {
	var failures []error
	// Each record is collected through the store that enumerated it.
	for _, retirement := range h.retirementStores() {
		records, err := h.retiredRecords(ctx, retirement.store)
		if err != nil {
			return err
		}
		if err := h.collectRetiredRecords(ctx, retirement, records, &failures); err != nil {
			return err
		}
	}
	return errors.Join(failures...)
}

func (h *Host) retiredRecords(ctx context.Context,
	retirement storage.RetirementStore) ([]storage.RetiredReplica, error) {
	probe, cancel := context.WithTimeout(ctx, h.cfg.EffectTimeout)
	records, err := retirement.Retired(probe)
	cancel()
	return records, err
}

func (h *Host) collectRetiredRecords(ctx context.Context, retirement retirementBackend,
	records []storage.RetiredReplica, failures *[]error) error {
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		own := h.cfg.Identity()
		if record.Identity.NodeName != own.NodeName || record.Identity.ClusterID != own.ClusterID {
			continue
		}
		if err := h.collectOne(ctx, retirement, record); err != nil {
			*failures = append(*failures, err)
			h.publishRetentionFailure(own, record)
		}
	}
	return nil
}

func (h *Host) publishRetentionFailure(own topology.Identity, record storage.RetiredReplica) {
	if h.cfg.Events != nil {
		h.cfg.Events.Publish(events.Event{
			Type: events.RuntimeError, NodeName: own.NodeName, ClusterID: own.ClusterID,
			InstanceID: own.InstanceID, GroupID: record.Identity.GroupID, ReplicaID: record.Identity.ReplicaID,
			Code: "retention_failed",
		})
	}
}

func (h *Host) collectOne(parent context.Context, retirement retirementBackend,
	record storage.RetiredReplica) error {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctxHandle, err := h.beginAuthorityOperation(ctx, record.Identity.GroupID)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer done()
	if retirement.class == topology.StorageMemory {
		return h.collectMemoryReplica(ctx, retirement.store, record)
	}
	return h.collectRetiredReplica(ctx, retirement.store, record)
}

func (h *Host) retentionDecision(ctx context.Context,
	record storage.RetiredReplica) (lifecycle.RetentionDecision, error) {
	select {
	case h.retentionGate <- struct{}{}:
	case <-ctx.Done():
		return lifecycle.RetentionDecision{}, ctx.Err()
	}
	call, stop := context.WithTimeout(ctx, h.cfg.CallbackTimeout)
	decision, err := h.cfg.Retention(call, lifecycle.RetentionInput{Now: time.Now().UTC(), Retired: record})
	if err == nil {
		err = call.Err()
	}
	stop()
	<-h.retentionGate
	return decision, err
}

func (h *Host) checkRetiredCollection(ctx context.Context, identity storage.Identity) error {
	if h.retiredReplicaLive(identity) {
		return fault.ErrReplicaNotReady
	}
	if err := h.checkCollectionLedger(ctx, identity); err != nil {
		return err
	}
	return h.checkCollectionStorage(ctx, identity)
}

func (h *Host) retiredReplicaLive(identity storage.Identity) bool {
	h.mu.Lock()
	slot := h.slots[identity.GroupID]
	live := slot != nil && slot.identity == identity && !slot.released
	h.mu.Unlock()
	return live
}

func (h *Host) checkCollectionLedger(ctx context.Context, identity storage.Identity) error {
	ledger, err := h.cfg.LookupRecord(ctx, identity.GroupID)
	if err != nil {
		return err
	}
	if ledger.Found && ledger.Record.ReplicaID == identity.ReplicaID {
		return fault.ErrReplicaNotReady
	}
	return nil
}

func (h *Host) checkCollectionStorage(ctx context.Context, identity storage.Identity) error {
	located, err := h.locate(ctx, identity.GroupID)
	probe := located.Probe
	if err != nil {
		return err
	}
	if probe.Exists && probe.Identity == identity {
		return fault.ErrReplicaNotReady
	}
	return nil
}
