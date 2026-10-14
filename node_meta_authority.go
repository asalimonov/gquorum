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
	mode    metaAuthorityMode
	done    chan struct{}
	fence   topology.FenceEvidence
	receipt topology.EffectReceipt
	err     error
}

type metaAuthorityMode uint8

const (
	prepareMetaAuthority metaAuthorityMode = iota
	applyMetaAuthority
)

func (n *node) installAuthorityFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if n.needsReadmissionFence(request) {
		return n.installReadmissionFence(ctx, request)
	}
	if request.Resource.GroupID != topology.MetaGroupID {
		return n.InstallLoadFence(ctx, request)
	}
	if request.Resource != (topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID,
		NodeName: request.To.NodeName}) ||
		request.ReplicaID != 0 {
		return load.FenceResult{}, fault.ErrIdentityMismatch
	}
	work, err := n.runMetaAuthority(ctx, request.Grant, request.To, prepareMetaAuthority)
	if err != nil {
		return load.FenceResult{}, err
	}
	return load.FenceResult{From: request.To, Evidence: work.fence}, nil
}

func (n *node) applyMetaHostAction(ctx context.Context, request load.ActionRequest) (load.ActionResult, error) {
	id := request.To.Node
	if request.Grant.Action == nil || *request.Grant.Action != request.Authorization ||
		request.To.Replica != (load.ReplicaRef{GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}) ||
		request.LocalTarget != (storage.Identity{ClusterID: id.ClusterID, NodeName: id.NodeName,
			GroupID:   topology.MetaGroupID,
			ReplicaID: id.MemberID}) {
		return load.ActionResult{}, fault.ErrIdentityMismatch
	}
	if request.Authorization.Kind != topology.StepPrepareReplica &&
		request.Authorization.Kind != topology.StepStopReplica {
		return load.ActionResult{}, fault.ErrOperatorAuthorization
	}
	work, err := n.runMetaAuthority(ctx, request.Grant, id, applyMetaAuthority)
	if err != nil {
		return load.ActionResult{}, err
	}
	return load.ActionResult{
		From:          request.To,
		Authorization: request.Authorization,
		LocalTarget:   request.LocalTarget,
		Receipts:      []topology.EffectReceipt{work.receipt.Clone()},
	}, nil
}

func (n *node) runMetaAuthority(ctx context.Context, grant topology.AuthorityGrant, id topology.Identity,
	mode metaAuthorityMode) (*metaAuthorityWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if grant.Key.GroupID != topology.MetaGroupID || grant.Key.ClusterID != id.ClusterID || grant.Validate() != nil ||
		!slices.Contains(grant.Targets, topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID,
			ReplicaID: id.MemberID}) {
		return nil, fault.ErrOperatorAuthorization
	}
	work, err := n.reserveMetaAuthority(grant, id, mode)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-work.done:
		return work, work.err
	}
}

func (n *node) reserveMetaAuthority(grant topology.AuthorityGrant, id topology.Identity,
	mode metaAuthorityMode) (*metaAuthorityWork, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping || n.ctx == nil || n.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if n.pending != nil {
		return nil, fault.ErrAuthorityPending
	}
	m, s := n.meta, n.authority
	if m == nil || s == nil {
		return nil, fault.ErrReplicaNotReady
	}
	if m.Identity() != id || s.Identity() != id {
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
	if work != nil && (work.mode != mode || !work.grant.Equal(grant)) {
		return nil, fault.ErrAuthorityPending
	}
	if work == nil {
		work = &metaAuthorityWork{grant: grant.Clone(), mode: mode, done: make(chan struct{})}
		n.metaAuthorityWork = work
		n.wg.Add(1)
		go func() { defer n.wg.Done(); defer close(work.done); n.executeMetaAuthority(m, s, id, work) }()
	}
	return work, nil
}

func (n *node) executeMetaAuthority(m *metaservice.Group, s *authoritystore.Store, id topology.Identity,
	work *metaAuthorityWork) {
	ctx := n.ctx
	if work.mode == prepareMetaAuthority {
		work.fence, work.err = n.prepareMetaHostAuthority(m, s, work)
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
	// A prior process or failed attempt may have persisted intent before
	// finishing.
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
		attempt, cancel := context.WithTimeout(ctx, n.opts.CallbackTimeout)
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
	work.receipt = topology.EffectReceipt{
		Key: work.grant.Key,
		Resource: topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID,
			NodeName: id.NodeName},
		Action:        work.grant.Action,
		Target:        target,
		Outcome:       topology.EffectApplied,
		LocalSequence: sequence,
	}
	work.err = s.FinishEffect(ctx, work.receipt)
}
func (n *node) prepareMetaHostAuthority(m *metaservice.Group, s *authoritystore.Store,
	work *metaAuthorityWork) (topology.FenceEvidence, error) {
	ctx := n.ctx
	floor, err := s.Fence(ctx, work.grant)
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	running, err := s.Running(ctx, topology.MetaGroupID)
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	for _, old := range running {
		if old.Grant.Key.Revision >= work.grant.Key.Revision {
			continue
		}
		if old.Grant.Action != nil && old.Grant.Action.Kind == topology.StepStopReplica {
			if err = n.joinMetaHost(ctx, m); err != nil {
				return topology.FenceEvidence{}, err
			}
		}
		// Preparation only verifies an acquired engine; no callback owner
		// can survive this serialized worker (or acquisition in a fresh
		// process).
		if err = s.FenceEffect(ctx, old, work.grant.Key); err != nil {
			return topology.FenceEvidence{}, err
		}
	}
	return s.CompleteFence(ctx, work.grant.Key, floor.Record.Sequence)
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
	completed, err := s.CompletedMetaStop(ctx)
	if err != nil {
		return err
	}
	if !completed.Found || completed.Record.Receipt == nil {
		return fault.ErrAuthorityPending
	}
	id := m.Identity()
	if completed.Record.Target != (topology.AuthorityTarget{NodeName: id.NodeName, MemberID: id.MemberID,
		ReplicaID: id.MemberID}) || s.Identity() != id {
		return fault.ErrIdentityMismatch
	}
	return m.ConfirmHostRelease(ctx, completed.Record.Grant, *completed.Record.Receipt)
}
