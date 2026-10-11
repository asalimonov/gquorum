package gquorum

import (
	"context"
	"log/slog"
	"sync"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/raftengine"
)

type Options struct {
	Logger *slog.Logger
	Events *events.Bus
}

// Runtime owns the handles it opens. Constructors do not start workers;
// callers start each handle with their own context. Injected dependencies,
// including shared load stores, machines and the event bus, remain caller-owned.
type Runtime struct {
	opts            Options
	mu              sync.RWMutex
	meta            *metaservice.Group
	loads           *raftengine.Registry
	parked          map[string]ParkedLoad
	node, cluster   string
	gate            chan struct{}
	closing, closed chan struct{}
	closeOnce       sync.Once
	closeErr        error
}

// New creates an empty runtime without opening stores or starting workers.
func New(opts Options) (*Runtime, error) {
	return &Runtime{opts: opts, loads: raftengine.NewRegistry(), parked: make(map[string]ParkedLoad), gate: make(chan struct{}, 1), closing: make(chan struct{}), closed: make(chan struct{})}, nil
}
func (r *Runtime) takeGate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-r.closing:
		return fault.ErrStopped
	default:
	}
	select {
	case r.gate <- struct{}{}:
		select {
		case <-r.closing:
			<-r.gate
			return fault.ErrStopped
		default:
			return nil
		}
	case <-r.closing:
		return fault.ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *Runtime) releaseGate() { <-r.gate }
func (r *Runtime) checkOwner(node, cluster string) error {
	if r.node != "" && (r.node != node || r.cluster != cluster) {
		return fault.New(fault.IdentityMismatch, "runtime handles belong to one node and cluster", nil)
	}
	return nil
}

func (r *Runtime) OpenMeta(opts metaservice.Options) (*metaservice.Group, error) {
	if err := r.takeGate(context.Background()); err != nil {
		return nil, err
	}
	defer r.releaseGate()
	if _, exists := r.Meta(); exists {
		return nil, fault.New(fault.IdentityMismatch, "meta handle already opened", nil)
	}
	if len(r.loads.IDs()) != 0 {
		return nil, fault.New(fault.Invalid, "open meta before load handles", nil)
	}
	if err := r.checkOwner(opts.Identity.NodeName, opts.Identity.ClusterID); err != nil {
		return nil, err
	}
	if opts.Logger == nil {
		opts.Logger = r.opts.Logger
	}
	if opts.Events == nil {
		opts.Events = r.opts.Events
	}
	g, err := metaservice.New(opts)
	if err != nil {
		return nil, err
	}
	select {
	case <-r.closing:
		_ = g.Stop(context.Background())
		return nil, fault.ErrStopped
	default:
	}
	r.mu.Lock()
	r.meta = g
	r.node, r.cluster = opts.Identity.NodeName, opts.Identity.ClusterID
	r.mu.Unlock()
	return g, nil
}

// OpenLoad constructs a replica. With meta attached it verifies approved local
// identity and the local ledger before opening storage. Effect receivers must
// still call metaservice.Authorize for the full execution/attempt/step scope.
func (r *Runtime) OpenLoad(opts raftengine.Options) (*raftengine.Group, error) {
	if err := r.takeGate(context.Background()); err != nil {
		return nil, err
	}
	defer r.releaseGate()
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := r.checkOwner(opts.NodeName, opts.ClusterID); err != nil {
		return nil, err
	}
	if old, exists := r.loads.Get(opts.GroupID); exists {
		if err := r.loads.Remove(opts.GroupID, old.ReplicaID()); err != nil {
			return nil, fault.New(fault.IdentityMismatch, "load handle is still owned; stop and join it before reopening", err)
		}
	}
	if opts.Logger == nil {
		opts.Logger = r.opts.Logger
	}
	if opts.Events == nil {
		opts.Events = r.opts.Events
	}
	m, hasMeta := r.Meta()
	if hasMeta && opts.InstanceID == "" {
		opts.InstanceID = m.Identity().InstanceID
	}
	boot, err := r.preflightLoad(m, opts)
	if err != nil {
		return nil, err
	}
	g, err := raftengine.New(opts)
	if err != nil {
		return nil, r.parkLoad(opts, parkReason(err), err)
	}
	if hasMeta {
		// raftengine.New has durably opened/restored the store before recording its
		// identity. A crash cannot create a ledger record without a store first.
		err = m.RecordLoadReplica(context.Background(), opts.GroupID, metaservice.ReplicaRecord{ReplicaID: opts.ReplicaID, Origin: boot.origin}, boot.previous)
		if err != nil {
			_ = g.Stop(context.Background())
			return nil, err
		}
	}
	select {
	case <-r.closing:
		_ = g.Stop(context.Background())
		return nil, fault.ErrStopped
	default:
	}
	if err = r.loads.Add(g); err != nil {
		_ = g.Stop(context.Background())
		return nil, err
	}
	r.mu.Lock()
	r.node, r.cluster = opts.NodeName, opts.ClusterID
	delete(r.parked, opts.GroupID)
	r.mu.Unlock()
	return g, nil
}

func (r *Runtime) Meta() (*metaservice.Group, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.meta, r.meta != nil
}
func (r *Runtime) Load(id string) (*raftengine.Group, bool) { return r.loads.Get(id) }
func (r *Runtime) LoadIDs() []string                        { return r.loads.IDs() }

// StopLoad joins exactly the expected local handle and unregisters it. It does not delete durable
// state, forget a voted identity, or delete the consensus group.
func (r *Runtime) StopLoad(ctx context.Context, id string, expectedReplicaID uint64) error {
	if expectedReplicaID == 0 {
		return fault.ErrInvalid
	}
	if err := r.takeGate(ctx); err != nil {
		return err
	}
	defer r.releaseGate()
	g, exists := r.loads.Get(id)
	if !exists {
		return nil
	}
	if g.ReplicaID() != expectedReplicaID {
		return fault.ErrIdentityMismatch
	}
	if err := g.Stop(ctx); err != nil {
		return err
	}
	return r.loads.Remove(id, g.ReplicaID())
}

// RequestClose schedules teardown without waiting. Owned callbacks may call
// this and return; they must not wait for a Close that must join them itself.
func (r *Runtime) RequestClose() {
	r.closeOnce.Do(func() { close(r.closing); go r.finishClose() })
}
func (r *Runtime) finishClose() {
	// All admitted constructors/StopLoad calls finish before taking a stable
	// snapshot. Accessors stay available while a slow constructor is in flight.
	r.gate <- struct{}{}
	m, _ := r.Meta()
	ids := r.loads.IDs()
	r.releaseGate()
	ctx := context.Background()
	if m != nil {
		r.closeErr = m.Quiesce(ctx)
	}
	for _, id := range ids {
		g, exists := r.loads.Get(id)
		if !exists {
			continue
		}
		if err := g.Stop(ctx); err != nil && r.closeErr == nil {
			r.closeErr = err
		}
		if err := r.loads.Remove(id, g.ReplicaID()); err != nil && r.closeErr == nil {
			r.closeErr = err
		}
	}
	if m != nil {
		if err := m.Stop(ctx); err != nil && r.closeErr == nil {
			r.closeErr = err
		}
	}
	close(r.closed)
}

// Close first quiesces meta effects, then joins all load handles and finally
// closes meta storage. Timeout retains ownership while the same teardown runs.
func (r *Runtime) Close(ctx context.Context) error {
	r.RequestClose()
	select {
	case <-r.closed:
		return r.closeErr
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "runtime handles have not closed", ctx.Err())
	}
}
