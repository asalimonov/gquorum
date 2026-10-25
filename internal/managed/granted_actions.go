package managed

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type actionWork struct {
	request load.ActionRequest
	done    chan struct{}
	result  load.ActionResult
	err     error
}

type replicaStorageState uint8

const (
	freshReplicaStorage replicaStorageState = iota
	retainedReplicaStorage
)

func (h *Host) beginStartupEffect(slot *replica, storageState replicaStorageState) error {
	if !creationGrant(slot) {
		return nil
	}
	s, err := h.authorityStore()
	if err != nil {
		return err
	}
	target := topology.AuthorityTarget{NodeName: slot.node.NodeName, MemberID: slot.node.MemberID,
		ReplicaID: slot.identity.ReplicaID}
	receipt, err := s.BeginEffect(slot.ctx, *slot.startGrant, target)
	if err == nil && receipt != nil {
		if storageState == freshReplicaStorage {
			return h.park(slot, ParkStoreMissing, fault.ErrUnreadable)
		}
		return nil // ordinary retained recovery, not repeated creation
	}
	if err != nil && !errors.Is(err, fault.ErrAuthorityPending) {
		return err
	}
	records, err := s.Running(slot.ctx, slot.identity.GroupID)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Target == target && record.Grant.Equal(*slot.startGrant) {
			slot.startSequence = record.Sequence
			return nil
		}
	}
	return fault.ErrAuthorityPending
}

// Called by the unique startup owner immediately before publication. A caller's
// canceled wait cannot lose the creation receipt after readiness succeeds.
func (h *Host) finishStartupEffect(slot *replica) error {
	if slot.startSequence == 0 {
		return nil
	}
	s, err := h.authorityStore()
	if err != nil {
		return err
	}
	return s.FinishEffect(h.operationContext, topology.EffectReceipt{Key: slot.authorityKey,
		Resource: topology.AuthorityResource{
			Kind:     topology.AuthorityHost,
			GroupID:  slot.identity.GroupID,
			NodeName: slot.node.NodeName,
		},
		Action: slot.startGrant.Action, Target: topology.AuthorityTarget{NodeName: slot.node.NodeName,
			MemberID:  slot.node.MemberID,
			ReplicaID: slot.identity.ReplicaID}, Outcome: topology.EffectApplied,
		LocalSequence: slot.startSequence})
}

func actionTargets(ctx context.Context, a topology.AuthorizedStep,
	grant topology.AuthorityGrant) ([]storage.Identity, error) {
	if grant.Action == nil || *grant.Action != a || grant.Validate() != nil {
		return nil, fault.ErrOperationAuthorization
	}
	var targets []storage.Identity
	for _, t := range grant.Targets {
		targets = append(targets,
			storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID, NodeName: t.NodeName,
				ReplicaID: t.ReplicaID})
	}
	return targets, ctx.Err()
}

func (h *Host) validateGrantedAction(ctx context.Context, req load.ActionRequest) error {
	if req.Grant.Action == nil || *req.Grant.Action != req.Authorization || !completeIdentity(req.From) ||
		!completePeer(req.To) || req.Grant.Validate() != nil {
		return fault.ErrOperationAuthorization
	}
	identity := h.cfg.Identity()
	if req.To.Node != identity || req.From.ClusterID != req.To.Node.ClusterID ||
		req.To.Replica.GroupID != req.Grant.Key.GroupID || req.To.Node.ClusterID != req.Grant.Key.ClusterID {
		return fault.ErrIdentityMismatch
	}
	if err := h.cfg.ValidateCourier(ctx, req.Grant, req.From); err != nil {
		return err
	}
	if localAction(req.Authorization.Kind) && !foundingConfirmation(req) {
		want := storage.Identity{
			NodeName:  req.To.Node.NodeName,
			ClusterID: req.To.Node.ClusterID,
			GroupID:   req.To.Replica.GroupID,
			ReplicaID: req.To.Replica.ReplicaID,
		}
		if req.LocalTarget != want || !slices.Contains(req.Grant.Targets, actionTarget(req)) {
			return fault.ErrOperationAuthorization
		}
		return nil
	}
	if req.LocalTarget != (storage.Identity{}) {
		return fault.ErrOperationAuthorization
	}
	return h.cfg.ValidatePeer(ctx, req.To)
}

func actionTarget(req load.ActionRequest) topology.AuthorityTarget {
	return topology.AuthorityTarget{
		NodeName:  req.LocalTarget.NodeName,
		MemberID:  req.To.Node.MemberID,
		ReplicaID: req.LocalTarget.ReplicaID,
	}
}

func (h *Host) checkInstalledGrant(ctx context.Context, grant topology.AuthorityGrant) error {
	s, err := h.authorityStore()
	if err != nil {
		return err
	}
	floor, err := s.Current(ctx, grant.Key.GroupID)
	if err != nil {
		return err
	}
	if !floor.Found || !floor.Record.Grant.Equal(grant) {
		return fault.ErrAuthorityFenced
	}
	if !floor.Record.Complete {
		return fault.ErrAuthorityPending
	}
	return nil
}

// An admitted retirement may finish while a newer pending floor joins its group
// gate. A later request must first pass checkInstalledGrant and BeginEffect.
func (h *Host) checkRetirementAuthority(ctx context.Context, req load.RetirementRequest) error {
	if req.Grant.Action == nil || *req.Grant.Action != *req.Authorization {
		return fault.ErrOperationAuthorization
	}
	if err := h.cfg.ValidateGrant(ctx, req.Grant); err != nil {
		return err
	}
	s, err := h.authorityStore()
	if err != nil {
		return err
	}
	floor, err := s.Current(ctx, req.Identity.GroupID)
	if err != nil {
		return err
	}
	if !floor.Found || floor.Record.Grant.Key.Revision < req.Grant.Key.Revision ||
		floor.Record.Grant.Key != req.Grant.Key &&
			floor.Record.Complete {
		return fault.ErrAuthorityFenced
	}
	if floor.Record.Grant.Key == req.Grant.Key && (!floor.Record.Complete || !floor.Record.Grant.Equal(req.Grant)) {
		return fault.ErrAuthorityPending
	}
	records, err := s.Running(ctx, req.Identity.GroupID)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Grant.Equal(req.Grant) &&
			record.Target == (topology.AuthorityTarget{NodeName: req.Identity.NodeName,
				MemberID:  h.cfg.Identity().MemberID,
				ReplicaID: req.Identity.ReplicaID}) {
			return nil
		}
	}
	return fault.ErrAuthorityFenced
}

func (h *Host) applyGrantedAction(parent context.Context, req load.ActionRequest) (load.ActionResult, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctxHandle, err := h.beginAuthorityWork(ctx)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.ActionResult{}, err
	}
	defer release()
	req.Grant = req.Grant.Clone()
	if err := h.validateGrantedAction(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	if !localAction(req.Authorization.Kind) || foundingConfirmation(req) {
		return h.applyGrantedConsensus(ctx, req)
	}
	if err := h.checkInstalledGrant(ctx, req.Grant); err != nil {
		return load.ActionResult{}, err
	}
	h.mu.Lock()
	if (h.quiescing || h.stopped) && hostActionLifetime(req.Authorization.Kind) != authorityWork {
		h.mu.Unlock()
		return load.ActionResult{}, fault.ErrStopped
	}
	if h.actionWorks == nil {
		h.actionWorks = make(map[string]*actionWork)
	}
	work := h.actionWorks[req.Grant.Key.GroupID]
	if work == nil {
		if _, err := h.admitWorkLocked(authorityWork); err != nil {
			h.mu.Unlock()
			return load.ActionResult{}, err
		}
		work = &actionWork{request: req, done: make(chan struct{})}
		h.actionWorks[req.Grant.Key.GroupID] = work
		go h.runGrantedAction(work)
	} else if work.request.To != req.To || work.request.LocalTarget != req.LocalTarget ||
		!work.request.Grant.Equal(req.Grant) {
		h.mu.Unlock()
		return load.ActionResult{}, fault.ErrAuthorityPending
	}
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		return load.ActionResult{}, ctx.Err()
	case <-work.done:
		result := cloneActionResult(work.result)
		return result, work.err
	}
}

func (h *Host) runGrantedAction(work *actionWork) {
	ctx, cancel := context.WithTimeout(h.authorityContext, h.cfg.EffectTimeout)
	h.mu.Lock()
	recovery := h.recoveryManifest != nil && !h.recoveryReleased &&
		work.request.Authorization.Kind == topology.StepCreateFoundingGroup
	h.mu.Unlock()
	if recovery {
		cancel()
		ctx, cancel = context.WithCancel(h.authorityContext)
	}
	result, err := h.executeGrantedLocal(ctx, work.request)
	cancel()
	h.mu.Lock()
	work.result, work.err = result, err
	delete(h.actionWorks, work.request.Grant.Key.GroupID)
	close(work.done)
	h.releaseWorkLocked(authorityWork)
	h.mu.Unlock()
}

func (h *Host) executeGrantedLocal(parent context.Context, req load.ActionRequest) (load.ActionResult, error) {
	ctxHandle, err := h.beginOwnedOperation(parent, req.Grant.Key.GroupID,
		hostActionLifetime(req.Authorization.Kind))
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.ActionResult{}, err
	}
	defer release()
	if err := h.validateGrantedAction(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	if err := h.checkInstalledGrant(ctx, req.Grant); err != nil {
		return load.ActionResult{}, err
	}
	s, err := h.authorityStore()
	if err != nil {
		return load.ActionResult{}, err
	}
	intent, err := h.beginLocalEffect(ctx, req, s)
	if err != nil {
		return load.ActionResult{}, err
	}
	if intent.receipt != nil {
		return grantedResult(req, *intent.receipt), nil
	}
	if err := h.runLocalAction(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	if err := h.verifyLocalEffect(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	proof := topology.EffectReceipt{
		Key: req.Grant.Key,
		Resource: topology.AuthorityResource{
			Kind:     topology.AuthorityHost,
			GroupID:  req.Grant.Key.GroupID,
			NodeName: req.To.Node.NodeName,
		},
		Action:        req.Grant.Action,
		Target:        actionTarget(req),
		Outcome:       topology.EffectApplied,
		LocalSequence: intent.sequence,
	}
	if err := s.FinishEffect(h.authorityContext, proof); err != nil {
		return load.ActionResult{}, err
	}
	return grantedResult(req, proof), nil
}

func hostActionLifetime(kind topology.StepKind) workLifetime {
	switch kind {
	case topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepDeleteGroupState:
		return authorityWork
	default:
		return ordinaryWork
	}
}

func grantedResult(req load.ActionRequest, receipt topology.EffectReceipt) load.ActionResult {
	return load.ActionResult{
		From:          req.To,
		Authorization: req.Authorization,
		LocalTarget:   req.LocalTarget,
		Receipts:      []topology.EffectReceipt{receipt.Clone()},
	}
}

// Founding has two distinct resources: host creation names LocalTarget, while
// an empty LocalTarget asks the exact running group to persist its native
// proof.
func foundingConfirmation(req load.ActionRequest) bool {
	return req.Authorization.Kind == topology.StepCreateFoundingGroup && req.LocalTarget == (storage.Identity{})
}

func (h *Host) applyGrantedConsensus(parent context.Context, req load.ActionRequest) (load.ActionResult, error) {
	ctxHandle, err := h.beginOperation(parent, req.Grant.Key.GroupID)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.ActionResult{}, err
	}
	defer release()
	if err := h.validateGrantedAction(ctx, req); err != nil {
		return load.ActionResult{}, err
	}
	core, err := h.actionCore(req)
	if err != nil {
		return load.ActionResult{}, err
	}
	receipt, err := performConsensusAction(ctx, req, core)
	if err != nil {
		return load.ActionResult{}, err
	}
	return grantedResult(req, receipt), nil
}

func cloneActionResult(result load.ActionResult) load.ActionResult {
	result.Receipts = slices.Clone(result.Receipts)
	for i := range result.Receipts {
		result.Receipts[i] = result.Receipts[i].Clone()
	}
	return result
}

type localEffectIntent struct {
	receipt  *topology.EffectReceipt
	sequence uint64
}

func (h *Host) beginLocalEffect(ctx context.Context, req load.ActionRequest,
	s *authoritystore.Store) (localEffectIntent, error) {
	receipt, err := s.BeginEffect(ctx, req.Grant, actionTarget(req))
	if err == nil && receipt != nil {
		return localEffectIntent{receipt: receipt}, nil
	}
	if err != nil {
		if !errors.Is(err, fault.ErrAuthorityPending) {
			return localEffectIntent{}, err
		}
		// An earlier waiter may have left while its startup/Close was still
		// owned. Reconcile the same identity through its reservation and the
		// documented idempotent retirement callback, never a new identity.
		if err := h.checkInstalledGrant(ctx, req.Grant); err != nil {
			return localEffectIntent{}, err
		}
	}
	floor, err := s.Current(ctx, req.Grant.Key.GroupID)
	if err != nil {
		return localEffectIntent{}, err
	}
	// The sequence belongs to the original running intent, even when a new
	// pending fence arrives during the callback.
	records, err := s.Running(ctx, req.Grant.Key.GroupID)
	if err != nil {
		return localEffectIntent{}, err
	}
	sequence := uint64(0)
	for _, record := range records {
		if record.Grant.Equal(req.Grant) && record.Target == actionTarget(req) {
			sequence = record.Sequence
		}
	}
	if sequence == 0 || floor.Record.Grant.Key.Revision < req.Grant.Key.Revision {
		return localEffectIntent{}, fault.ErrUnreadable
	}
	return localEffectIntent{sequence: sequence}, nil
}

func (h *Host) runLocalAction(ctx context.Context, req load.ActionRequest) error {
	var err error
	switch req.Authorization.Kind {
	case topology.StepCreateFoundingGroup, topology.StepPrepareReplica:
		var slot *replica
		slot, err = h.acceptGuardedStart(ctx, req.To.Replica, StartOperator, func(ctx context.Context) error {
			return h.validateGrantedAction(ctx, req)
		})
		if err == nil {
			err = h.waitPrivateStarted(ctx, slot)
		}
	case topology.StepStopReplica:
		err = h.stopRetiringIdentity(ctx, req.LocalTarget, req.Authorization, req.Grant)
	case topology.StepCleanupReplicaState, topology.StepDeleteGroupState:
		a := req.Authorization
		_, err = h.retire(ctx, load.RetirementRequest{
			Grant:         req.Grant,
			Identity:      req.LocalTarget,
			Reason:        load.RetirementTopology,
			Authorization: &a,
		})
	default: // Other actions require only the effect verification below.
	}
	if err != nil {
		return err
	}
	return nil
}

func performConsensusAction(ctx context.Context, req load.ActionRequest,
	core *raftengine.Group) (topology.EffectReceipt, error) {
	var err error
	var receipt topology.EffectReceipt
	a := req.Authorization
	if a.Kind == topology.StepCreateFoundingGroup {
		receipt, err = core.ObserveAuthorizedEffect(ctx, req.Grant)
		if err != nil {
			return topology.EffectReceipt{}, err
		}
	} else if a.Kind == topology.StepTransferLeader {
		if err := core.TransferAuthorized(ctx, req.Grant); err != nil {
			return topology.EffectReceipt{}, err
		}
		return waitTransferReceipt(ctx, req, core)
	} else {
		var change raftpb.ConfChangeV2
		switch a.Kind {
		case topology.StepAddLearner:
			change = membership.AddLearner(a.ReplicaID)
		case topology.StepPromoteVoter:
			change = membership.PromoteLearner(a.ReplicaID)
		case topology.StepDemoteVoter:
			change = membership.DemoteVoter(a.ReplicaID)
		case topology.StepRemoveReplica:
			change = membership.RemoveReplica(a.ReplicaID)
		default:
			return topology.EffectReceipt{}, fault.ErrUnsupported
		}
		receipt, err = core.ProposeAuthorizedConfChange(ctx, req.Grant, change, core.Status().ConfState)
		if err != nil {
			return topology.EffectReceipt{}, err
		}
	}
	return receipt, nil
}

func waitTransferReceipt(ctx context.Context, req load.ActionRequest,
	core *raftengine.Group) (topology.EffectReceipt, error) {
	a := req.Authorization
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		lookup, err := core.AuthorityReceipt(ctx, req.Grant.Key)
		if err != nil {
			return topology.EffectReceipt{}, err
		}
		if lookup.Found && consensusEffectPresent(core.Status(), a) {
			return lookup.Receipt, nil
		}
		select {
		case <-ctx.Done():
			return topology.EffectReceipt{}, ctx.Err()
		case <-tick.C:
		}
	}
}
