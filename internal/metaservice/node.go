package metaservice

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LifecycleObservation reports local readiness and applied self-removal evidence.
type LifecycleObservation struct {
	Ready   bool
	Removal *topology.SelfRemovalStatus
}

// Lifecycle is a local copied observation for Node's owner. It performs no I/O.
func (g *Group) Lifecycle() (LifecycleObservation, error) {
	var ready bool
	var removal *topology.SelfRemovalStatus
	var terminal error
	g.mu.RLock()
	defer g.mu.RUnlock()
	select {
	case <-g.ready:
		ready = true
	default:
	}
	terminal = g.terminal
	if core := g.engine.Load(); terminal == nil && core != nil && !g.quiescing {
		// Readmission deliberately joins the former engine before replacing its
		// pointer. Normal stop is not an asynchronous failure of the new
		// lifetime.
		if err := core.Error(); !errors.Is(err, fault.ErrStopped) {
			terminal = err
		}
	}
	if g.selfRemoval != nil {
		removalCopy := *g.selfRemoval
		removal = &removalCopy
	}
	return LifecycleObservation{Ready: ready && terminal == nil && removal == nil && !g.quiescing,
		Removal: removal}, terminal
}

// Readiness assembles owned local admission and founder observations for
// lifecycle hooks.
func (g *Group) Readiness(started time.Time) lifecycle.ReadinessInput {
	observed, _ := g.Lifecycle()
	in := lifecycle.ReadinessInput{
		Now:            time.Now().UTC(),
		StartedAt:      started,
		Identity:       g.Identity(),
		FirstBootstrap: g.firstBootstrap,
		Admitted:       observed.Ready,
		Recovered:      observed.Ready,
		SessionLive:    g.OwnSessionLive(),
	}
	if !in.FirstBootstrap {
		return in
	}
	active := g.state().Active()
	observation := g.HealthObservation()
	fresh := active.Version != 0 && observation.Version == active.Version && !observation.At.IsZero() &&
		in.Now.Sub(observation.At) <= g.opts.SessionTTL
	for _, founder := range g.opts.Founders {
		if !fresh || observation.Nodes[founder.Node.Name] != topology.HealthAlive ||
			active.MemberID(founder.Node.Name) != founder.MemberID {
			in.MissingFounders = append(in.MissingFounders,
				topology.Identity{ClusterID: in.Identity.ClusterID, NodeName: founder.Node.Name,
					MemberID: founder.MemberID})
		}
	}
	return in
}

// LoadAssignments returns approved local load assignments for the exact
// admitted member.
func (g *Group) LoadAssignments(ctx context.Context) ([]load.ReplicaRef, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return nil, err
	}
	own := g.Identity()
	target := g.state().Target()
	if own.MemberID == 0 || target.MemberID(own.NodeName) != own.MemberID {
		return nil, fault.ErrReplicaNotReady
	}
	var refs []load.ReplicaRef
	for _, group := range target.LoadGroups() {
		for _, r := range group.Replicas {
			if r.NodeName == own.NodeName {
				refs = append(refs, load.ReplicaRef{GroupID: group.ID, ReplicaID: r.ReplicaID})
			}
		}
	}
	return refs, nil
}

// ValidateLoadNode verifies an exact current node incarnation before load
// dispatch.
func (g *Group) ValidateLoadNode(ctx context.Context, id topology.Identity) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	identity := g.Identity()
	if id == identity && id.MemberID != 0 {
		return nil
	}
	current, found := g.state().ProcessIdentity(id.NodeName)
	if !found {
		return fault.ErrReplicaNotReady
	}
	if id != current {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// ValidateLoadPeer verifies current process identity and an exact admitted load
// catalog entry.
func (g *Group) ValidateLoadPeer(ctx context.Context, peer load.Peer) error {
	if err := g.ValidateLoadNode(ctx, peer.Node); err != nil {
		return err
	}
	catalog, err := g.LoadCatalog(ctx, peer.Replica.GroupID)
	if err != nil {
		return err
	}
	id := storage.Identity{
		ClusterID: peer.Node.ClusterID,
		NodeName:  peer.Node.NodeName,
		GroupID:   peer.Replica.GroupID,
		ReplicaID: peer.Replica.ReplicaID,
	}
	if !slices.Contains(catalog, id) {
		// A current sender can observe an approval before this receiver applies
		// it. Refuse the unrecognized replica without turning observation lag
		// into a terminal Operator failure. No Raft input is admitted here.
		return fault.New(fault.ReplicaNotReady, "load replica is not locally approved", nil)
	}
	return nil
}

// ResolveLoadPeer can route old approved cleanup identities. Incoming Raft uses
// the stricter current catalog above; cleanup still requires an exact approved
// step.
func (g *Group) ResolveLoadPeer(ctx context.Context, ref load.ReplicaRef) (load.Peer, error) {
	catalog, err := g.LoadCatalog(ctx, ref.GroupID)
	if err != nil {
		return load.Peer{}, err
	}
	name := ""
	for _, id := range catalog {
		if id.ReplicaID == ref.ReplicaID {
			name = id.NodeName
			break
		}
	}
	if name == "" {
		groups, err := g.state().RecoveryCatalog(ctx)
		if err != nil {
			return load.Peer{}, err
		}
		name = recoveryLoadPeerName(groups, ref)
	}
	if name == "" || ref.ReplicaID == 0 {
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	id := g.Identity()
	if name != id.NodeName {
		var found bool
		id, found = g.state().ProcessIdentity(name)
		if !found {
			return load.Peer{}, fault.ErrReplicaNotReady
		}
	}
	return load.Peer{Node: id, Replica: ref}, nil
}

func recoveryLoadPeerName(groups []topology.Group, ref load.ReplicaRef) string {
	name := ""
	for _, group := range groups {
		if group.ID == ref.GroupID {
			for _, replica := range group.Replicas {
				if replica.ReplicaID == ref.ReplicaID {
					name = replica.NodeName
				}
			}
		}
	}
	return name
}
