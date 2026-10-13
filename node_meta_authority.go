package gquorum

import (
	"context"
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// One owned metadata-host operation bounds the queue and serializes its durable
// floor/effect records. Courier cancellation only ends the caller's wait.
type metaAuthorityWork struct {
	grant   topology.AuthorityGrant
	action  bool
	done    chan struct{}
	fence   topology.FenceEvidence
	receipt topology.EffectReceipt
	err     error
}

func (n *node) installAuthorityFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if n.needsReadmissionFence(request) {
		return n.installReadmissionFence(ctx, request)
	}
	if request.Resource.GroupID != topology.MetaGroupID {
		return n.InstallLoadFence(ctx, request)
	}
	if request.Resource != (topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: request.To.NodeName}) || request.ReplicaID != 0 {
		return load.FenceResult{}, fault.ErrIdentityMismatch
	}
	work, err := n.runMetaAuthority(ctx, request.Grant, request.To, false)
	if err != nil {
		return load.FenceResult{}, err
	}
	return load.FenceResult{From: request.To, Evidence: work.fence}, nil
}
func (n *node) applyMetaHostAction(ctx context.Context, request load.ActionRequest) (load.ActionResult, error) {
	id := request.To.Node
	if request.Grant.Action == nil || *request.Grant.Action != request.Authorization || request.To.Replica != (load.ReplicaRef{GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}) || request.LocalTarget != (storage.Identity{ClusterID: id.ClusterID, NodeName: id.NodeName, GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}) {
		return load.ActionResult{}, fault.ErrIdentityMismatch
	}
	if request.Authorization.Kind != topology.StepPrepareReplica && request.Authorization.Kind != topology.StepStopReplica {
		return load.ActionResult{}, fault.ErrOperatorAuthorization
	}
	work, err := n.runMetaAuthority(ctx, request.Grant, id, true)
	if err != nil {
		return load.ActionResult{}, err
	}
	return load.ActionResult{From: request.To, Authorization: request.Authorization, LocalTarget: request.LocalTarget, Receipts: []topology.EffectReceipt{work.receipt.Clone()}}, nil
}
func (n *node) runMetaAuthority(ctx context.Context, grant topology.AuthorityGrant, id topology.Identity, action bool) (*metaAuthorityWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if grant.Validate() != nil || grant.Key.GroupID != topology.MetaGroupID || grant.Key.ClusterID != id.ClusterID || !slices.Contains(grant.Targets, topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: id.MemberID}) {
		return nil, fault.ErrOperatorAuthorization
	}
	n.mu.Lock()
	if n.stopping || n.ctx == nil || n.ctx.Err() != nil {
		n.mu.Unlock()
		return nil, fault.ErrStopped
	}
	if n.pending != nil {
		n.mu.Unlock()
		return nil, fault.ErrAuthorityPending
	}
	m, s := n.meta, n.authority
	if m == nil || s == nil {
		n.mu.Unlock()
		return nil, fault.ErrReplicaNotReady
	}
	if m.Identity() != id || s.Identity() != id {
		n.mu.Unlock()
		return nil, fault.ErrIdentityMismatch
	}
	work := n.metaAuthorityWork
	if work != nil {
		select {
		case <-work.done:
			work = nil
		default:
		}
	}
	if work != nil && (work.action != action || !work.grant.Equal(grant)) {
		n.mu.Unlock()
		return nil, fault.ErrAuthorityPending
	}
	if work == nil {
		work = &metaAuthorityWork{grant: grant.Clone(), action: action, done: make(chan struct{})}
		n.metaAuthorityWork = work
		n.wg.Add(1)
		go func() { defer n.wg.Done(); defer close(work.done); n.executeMetaAuthority(m, s, id, work) }()
	}
	n.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-work.done:
		return work, work.err
	}
}
func (n *node) executeMetaAuthority(m *metaservice.Group, s *authoritystore.Store, id topology.Identity, work *metaAuthorityWork) {
	ctx := n.ctx
	if !work.action {
		floor, _, err := s.Fence(ctx, work.grant)
		if err != nil {
			work.err = err
			return
		}
		running, err := s.Running(ctx, topology.MetaGroupID)
		if err != nil {
			work.err = err
			return
		}
		for _, old := range running {
			if old.Grant.Key.Revision >= work.grant.Key.Revision {
				continue
			}
			if old.Grant.Action != nil && old.Grant.Action.Kind == topology.StepStopReplica {
				if err = n.joinMetaHost(ctx, m); err != nil {
					work.err = err
					return
				}
			}
			// Preparation only verifies an acquired engine; no callback owner can
			// survive this serialized worker (or acquisition in a fresh process).
			if err = s.FenceEffect(ctx, old, work.grant.Key); err != nil {
				work.err = err
				return
			}
		}
		work.fence, work.err = s.CompleteFence(ctx, work.grant.Key, floor.Sequence)
		return
	}
	target := topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: id.MemberID}
	receipt, err := s.BeginEffect(ctx, work.grant, target)
	if receipt != nil && err == nil {
		work.receipt = receipt.Clone()
		return
	}
	if err != nil && !errors.Is(err, fault.ErrAuthorityPending) {
		work.err = err
		return
	}
	// A prior process or failed attempt may have persisted intent before finishing.
	running, err := s.Running(ctx, topology.MetaGroupID)
	if err != nil {
		work.err = err
		return
	}
	sequence := uint64(0)
	for _, record := range running {
		if record.Target == target && record.Grant.Equal(work.grant) {
			sequence = record.Sequence
		}
	}
	if sequence == 0 {
		work.err = fault.ErrAuthorityPending
		return
	}
	switch work.grant.Action.Kind {
	case topology.StepPrepareReplica:
		attempt, cancel := context.WithTimeout(ctx, n.opts.Policies.CallbackTimeout)
		err = m.VerifyPreparedMeta(attempt, id)
		cancel()
	case topology.StepStopReplica:
		err = n.joinMetaHost(ctx, m)
	default:
		err = fault.ErrOperatorAuthorization
	}
	if err != nil {
		work.err = err
		return
	}
	work.receipt = topology.EffectReceipt{Key: work.grant.Key, Resource: topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: id.NodeName}, Action: work.grant.Action, Target: target, Outcome: topology.EffectApplied, LocalSequence: sequence}
	work.err = s.FinishEffect(ctx, work.receipt)
}
func (n *node) joinMetaHost(ctx context.Context, m *metaservice.Group) error {
	if err := m.FenceParticipation(ctx); err != nil {
		return err
	}
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h != nil {
		return h.FenceParticipation(ctx)
	}
	return nil
}
func (n *node) confirmMetaStop(ctx context.Context, m *metaservice.Group) error {
	s := n.localAuthority()
	if s == nil {
		return fault.ErrReplicaNotReady
	}
	completed, found, err := s.CompletedMetaStop(ctx)
	if err != nil {
		return err
	}
	if !found || completed.Receipt == nil {
		return fault.ErrAuthorityPending
	}
	id := m.Identity()
	if s.Identity() != id || completed.Target != (topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: id.MemberID}) {
		return fault.ErrIdentityMismatch
	}
	return m.ConfirmHostRelease(ctx, completed.Grant, *completed.Receipt)
}
