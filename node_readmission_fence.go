package gquorum

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

type readmissionWork struct {
	cancel   context.CancelFunc
	done     chan struct{}
	revision uint64
}

type readmissionFenceWork struct {
	grant    topology.AuthorityGrant
	done     chan struct{}
	evidence topology.FenceEvidence
	err      error
}

func workPending(done <-chan struct{}) bool {
	select {
	case <-done:
		return false
	default:
		return true
	}
}

// Readmission owns retirement outside the Host's closed administration
// lifetime. Missing metadata also needs this path before Join can obtain
// retirement grants.
func (n *node) needsReadmissionFence(request load.FenceRequest) bool {
	if request.Resource.Kind != topology.AuthorityHost || request.Grant.Purpose != topology.AuthorityFence {
		return false
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.pending != nil || n.meta != nil && n.authority != nil && n.meta.Identity().MemberID == 0
}

// One owned worker bounds all host-fence requests during readmission. A courier
// may stop waiting, but the worker retains the callback join and durable
// receipt.
func (n *node) installReadmissionFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if err := ctx.Err(); err != nil {
		return load.FenceResult{}, err
	}
	grant := request.Grant.Clone()
	if grant.Purpose != topology.AuthorityFence || request.ReplicaID != 0 ||
		request.Resource != (topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: grant.Key.GroupID,
			NodeName: request.To.NodeName}) || grant.Validate() != nil ||
		!slices.Contains(grant.Resources, request.Resource) {
		return load.FenceResult{}, fault.ErrOperatorAuthorization
	}
	n.mu.RLock()
	m := n.meta
	n.mu.RUnlock()
	if m == nil {
		return load.FenceResult{}, fault.ErrReplicaNotReady
	}
	if err := m.ValidateAuthorityCourier(ctx, grant, request.From); err != nil {
		return load.FenceResult{}, err
	}
	work, err := n.reserveReadmissionFence(m, request.To, grant)
	if err != nil {
		return load.FenceResult{}, err
	}
	select {
	case <-ctx.Done():
		return load.FenceResult{}, ctx.Err()
	case <-work.done:
		if work.err != nil {
			return load.FenceResult{}, work.err
		}
		return load.FenceResult{From: request.To, Evidence: work.evidence}, nil
	}
}

func (n *node) reserveReadmissionFence(m *metaservice.Group, to topology.Identity,
	grant topology.AuthorityGrant) (*readmissionFenceWork, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping || n.ctx == nil || n.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	s, h := n.authority, n.host
	if n.meta != m || s == nil || grant.Key.ClusterID != to.ClusterID || s.Identity() != to {
		return nil, fault.ErrIdentityMismatch
	}
	if n.pending == nil && m.Identity().MemberID != 0 || n.metaAuthorityWork != nil &&
		workPending(n.metaAuthorityWork.done) {
		return nil, fault.ErrAuthorityPending
	}
	retirement := n.readmissionWork
	if retirement != nil && retirement.revision >= grant.Key.Revision {
		return nil, fault.ErrAuthorityFenced
	}
	work := n.readmissionFenceWork
	if work != nil && !workPending(work.done) {
		work = nil
	}
	if work != nil && !work.grant.Equal(grant) {
		return nil, fault.ErrAuthorityPending
	}
	if work == nil {
		work = &readmissionFenceWork{grant: grant, done: make(chan struct{})}
		n.readmissionFenceWork = work
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			defer close(work.done)
			work.evidence, work.err = n.executeReadmissionFence(s, h, retirement, work.grant)
		}()
	}
	return work, nil
}

func (n *node) executeReadmissionFence(s *authoritystore.Store, h *managed.Host, retirement *readmissionWork,
	grant topology.AuthorityGrant) (topology.FenceEvidence, error) {
	ctx := n.ctx
	floor, err := s.Fence(ctx, grant)
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	if retirement != nil {
		retirement.cancel()
		select {
		case <-ctx.Done():
			return topology.FenceEvidence{}, ctx.Err()
		case <-retirement.done:
		}
	}
	if h != nil {
		if err := h.FenceReadmission(ctx); err != nil {
			return topology.FenceEvidence{}, err
		}
	}
	running, err := s.Running(ctx, grant.Key.GroupID)
	if err != nil {
		return topology.FenceEvidence{}, err
	}
	for _, record := range running {
		if record.Grant.Key.Revision >= grant.Key.Revision {
			return topology.FenceEvidence{}, fault.ErrAuthorityPending
		}
		if err := s.FenceEffect(ctx, record, grant.Key); err != nil {
			return topology.FenceEvidence{}, err
		}
	}
	return s.CompleteFence(ctx, grant.Key, floor.Record.Sequence)
}
