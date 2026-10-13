package gquorum

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/metastore"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (n *node) newHost(m *metaservice.Group) (*managed.Host, error) {
	n.mu.RLock()
	store, retirement, runner := n.store, n.retirement, n.policies
	n.mu.RUnlock()
	return managed.New(managed.Config{
		Authority: n.localAuthority, ValidateGrant: m.ValidateGrant, ValidateCourier: m.ValidateAuthorityCourier,
		ResolveAuthorityPeer: m.ResolveAuthorityPeer, RecordFence: m.RecordFence,
		ForgetGrantedReplica: m.ForgetGrantedLoadReplica,
		AuthorityFloors: func(ctx context.Context) ([]topology.AuthorityGrant, error) {
			return m.ReadAuthorityFloors(ctx, m.Identity().NodeName)
		},
		Identity: m.Identity, MetaDBPath: n.opts.metaDBPath(),
		AuthorizeReadmission: func(ctx context.Context, grant managed.Readmission) error {
			return m.AuthorizeReadmission(ctx, metaservice.Readmission(grant))
		},
		Scope: m.LoadReplicaScope, LookupRecord: m.LookupLoadReplica, RecordReplica: m.RecordLoadReplica,
		Catalog: m.LoadCatalog, Active: m.Active,
		RetireApplication: n.opts.Loads.Retire,
		Retirement:        retirement, Retention: runner.Retention, RetirementInterval: n.opts.Loads.RetirementInterval,
		Assignments: m.LoadAssignments, Factory: n.opts.Loads.Factory, Store: store, Transport: n.opts.Loads.Transport,
		ResolvePeer: m.ResolveLoadPeer, ValidatePeer: m.ValidateLoadPeer, ValidateNode: m.ValidateLoadNode,
		Engine: n.opts.Loads.Engine, EffectTimeout: n.opts.Loads.EffectTimeout, CallbackTimeout: n.opts.Policies.CallbackTimeout,
		Metrics:          internalmetrics.Settings(n.opts.Metrics),
		RecoveryInterval: n.opts.Loads.RecoveryInterval, Events: n.opts.Events, Logger: n.opts.Logger,
	})
}

func (n *node) heldLoads() []storage.Identity {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return nil
	}
	return h.Held()
}

func (n *node) StartLoadGroup(ctx context.Context, ref load.ReplicaRef) (*load.Group, error) {
	h, err := n.loadHost(ctx)
	if err != nil {
		return nil, err
	}
	handle, err := h.StartReplica(ctx, ref, managed.StartExplicit)
	var parked *managed.ParkedError
	if errors.As(err, &parked) {
		err = &LoadParkedError{ParkedLoad: ParkedLoad{Identity: parked.Identity, Reason: ParkReason(parked.Reason), Since: parked.Since}, Cause: parked.Cause}
	}
	return (*load.Group)(handle), err
}
func (n *node) LoadGroup(id string) (*load.Group, bool) {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return nil, false
	}
	handle, found := h.Lookup(id)
	return (*load.Group)(handle), found
}
func (n *node) LoadGroupIDs() []string {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return nil
	}
	return h.IDs()
}
func (n *node) StopLoadGroup(ctx context.Context, ref load.ReplicaRef) error {
	h, err := n.loadHost(ctx)
	if err != nil {
		return err
	}
	return h.StopReplica(ctx, ref, managed.StopExplicit)
}
func (n *node) ParkedLoads() map[string]ParkedLoad {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	result := make(map[string]ParkedLoad)
	if h != nil {
		for id, park := range h.Parked() {
			result[id] = ParkedLoad{Identity: park.Identity, Reason: ParkReason(park.Reason), Since: park.Since}
		}
	}
	return result
}

func (n *node) readmit(ctx context.Context, grant metaservice.Readmission) error {
	n.mu.RLock()
	m := n.meta
	stopped := n.stopping
	n.mu.RUnlock()
	if stopped {
		return fault.ErrStopped
	}
	if err := m.AuthorizeReadmission(ctx, grant); err != nil {
		return err
	}
	n.mu.Lock()
	if n.stopping || n.meta != m {
		n.mu.Unlock()
		return fault.ErrStopped
	}
	if n.readmissionWork != nil && workPending(n.readmissionWork.done) || n.readmissionFenceWork != nil && workPending(n.readmissionFenceWork.done) || n.metaAuthorityWork != nil && workPending(n.metaAuthorityWork.done) {
		n.mu.Unlock()
		return fault.ErrAuthorityPending
	}
	if n.authority == nil || len(grant.Grants) == 0 {
		n.mu.Unlock()
		return fault.ErrOperatorAuthorization
	}
	ctx, cancel := context.WithCancel(ctx)
	retirement := &readmissionWork{cancel: cancel, done: make(chan struct{}), revision: grant.Grants[0].Key.Revision}
	n.readmissionWork = retirement
	defer func() { cancel(); close(retirement.done) }()
	granted := grant.Granted
	n.pending = &granted
	n.ready = false
	authority, h := n.authority, n.host
	n.mu.Unlock()
	// A successful retirement/rebind can precede a failed metadata acquisition.
	// Resume only that exact durable proof; never retire the successor on retry.
	if authority.Identity() == grant.Granted {
		if err := n.readmissionReceipts(ctx, grant); err != nil {
			return err
		}
		return authority.Rebind(ctx, grant.Previous, grant.Granted, grant.AdmissionID)
	}
	var err error
	if h != nil {
		err = h.Readmit(ctx, managed.Readmission(grant))
	} else {
		err = managed.ReadmitMeta(ctx, m.Identity(), n.opts.metaDBPath(), managed.Readmission(grant), authority, func(ctx context.Context, g managed.Readmission) error {
			return m.AuthorizeReadmission(ctx, metaservice.Readmission(g))
		})
	}
	if err == nil {
		err = n.readmissionReceipts(ctx, grant)
	}
	if err == nil {
		err = authority.Rebind(ctx, grant.Previous, grant.Granted, grant.AdmissionID)
	}
	if err != nil {
		n.recordError(err)
	}
	n.signal()
	return err
}

func (n *node) readmissionReceipts(ctx context.Context, grant metaservice.Readmission) error {
	if fresh, err := metastore.StoreIsFresh(grant.MetaDBPath); err != nil {
		return err
	} else if !fresh {
		return fault.ErrIdentityMismatch
	}
	s := n.localAuthority()
	if s == nil || len(grant.Grants) == 0 {
		return fault.ErrOperatorAuthorization
	}
	for _, g := range grant.Grants {
		for _, target := range g.Targets {
			if _, found, err := s.Receipt(ctx, g, target); err != nil {
				return err
			} else if !found {
				return fault.ErrAuthorityPending
			}
		}
	}
	return nil
}

func (n *node) replaceReadmittedHost(m *metaservice.Group) error {
	n.mu.RLock()
	pending := n.pending
	stopped := n.stopping || n.readmissionFenceWork != nil && workPending(n.readmissionFenceWork.done)
	n.mu.RUnlock()
	if pending == nil || stopped || m.Identity() != *pending {
		return nil
	}
	// The service installed the exact granted pin only after successful Readmit.
	// The previous host is permanently stopped; its lifetime cannot be restarted.
	var h *managed.Host
	if n.opts.Loads.Factory != nil {
		var err error
		h, err = n.newHost(m)
		if err != nil {
			return err
		}
		if err = h.Start(n.ctx); err != nil {
			return err
		}
	}
	n.mu.Lock()
	n.host, n.pending, n.lastError = h, nil, nil
	n.mu.Unlock()
	return nil
}

// nodeEffects resolves the current managed lifetime after readmission; the
// service never keeps an adapter bound to a retired host.
type nodeEffects struct{ node *node }

func (e nodeEffects) ReadNative(ctx context.Context, id string, candidates ...storage.Identity) (topology.NativeState, error) {
	h, err := e.node.loadHost(ctx)
	if err != nil {
		return topology.NativeState{}, err
	}
	return h.ReadNative(ctx, id, candidates...)
}

func (e nodeEffects) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	h, err := e.node.loadHost(ctx)
	if err != nil {
		return topology.GroupStatus{}, err
	}
	return h.ObserveGroup(ctx, id)
}
func (e nodeEffects) dispatch(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	h, err := e.node.loadHost(ctx)
	if err != nil {
		return nil, err
	}
	return h.DispatchGrant(ctx, a)
}
func (e nodeEffects) CreateGroup(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.dispatch(ctx, a)
}
func (e nodeEffects) PrepareReplica(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.dispatch(ctx, a)
}
func (e nodeEffects) ApplyMembership(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.dispatch(ctx, a)
}
func (e nodeEffects) TransferLeader(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.dispatch(ctx, a)
}
func (e nodeEffects) StopReplica(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.dispatch(ctx, a)
}
func (e nodeEffects) CleanupReplicaState(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.dispatch(ctx, a)
}
func (e nodeEffects) DeleteGroupState(ctx context.Context, a topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return e.dispatch(ctx, a)
}

func (n *node) readmissionIdentity() topology.Identity {
	if s := n.localAuthority(); s != nil {
		return s.Identity()
	}
	id := n.opts.Identity
	id.MemberID = 0
	return id
}

func (n *node) collectReadmissionReceipts(ctx context.Context, grant metaservice.Readmission) ([]topology.EffectReceipt, error) {
	s := n.localAuthority()
	if s == nil || s.Identity() != grant.Granted {
		return nil, fault.ErrIdentityMismatch
	}
	if err := s.VerifyRebind(ctx, grant.Previous, grant.Granted, grant.AdmissionID); err != nil {
		return nil, err
	}
	var receipts []topology.EffectReceipt
	for _, g := range grant.Grants {
		for _, target := range g.Targets {
			receipt, found, err := s.Receipt(ctx, g, target)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fault.ErrAuthorityPending
			}
			receipts = append(receipts, receipt)
		}
	}
	return receipts, nil
}
