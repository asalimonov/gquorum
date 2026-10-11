package metaservice

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) run() {
	defer func() {
		g.wg.Done()
		if g.autoStopNeeded() {
			g.RequestStop()
		}
	}()
	ticker := time.NewTicker(g.opts.Timing.TickInterval)
	defer ticker.Stop()
	var wasLeader bool
	var nextExpiry time.Time
	for {
		select {
		case <-g.workCtx.Done():
			return
		case <-ticker.C:
		}

		if g.opts.Join && !g.joinDone {
			ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
			err := g.tryJoin(ctx)
			cancel()
			if err != nil {
				if g.workCtx.Err() != nil {
					return
				}
				var fatal *fatalAdmissionError
				if errors.As(err, &fatal) {
					g.fail(fatal.error)
					return
				}
				if errors.Is(err, fault.ErrReadmissionRequired) {
					g.mu.Lock()
					g.terminal = err
					g.lifecycle = "blocked"
					g.mu.Unlock()
					g.readyOnce.Do(func() { close(g.ready) })
					return
				}
				if g.workCtx.Err() != nil {
					return
				}
				g.mu.Lock()
				g.lifecycle = "parked"
				g.mu.Unlock()
				g.opts.Logger.Debug("meta join awaiting admission", "error", err)
				continue
			}
		}
		engine := g.engine.Load()
		if engine == nil {
			continue
		}
		if g.opts.Join && !g.joinReady {
			ctx, cancel := context.WithTimeout(g.workCtx, 2*time.Second)
			err := g.acknowledgeJoin(ctx)
			cancel()
			if err != nil {
				continue
			}
		}

		if err := engine.Error(); err != nil {
			if g.workCtx.Err() == nil {
				g.fail(err)
			}
			return
		}
		if err := g.transport.Error(); err != nil {
			g.fail(err)
			return
		}
		status := engine.Status()
		g.observeRemoval(g.currentIdentity(), g.state().Target(), status.ConfState, status.Applied)
		if g.workCtx.Err() != nil {
			return
		}
		leader := g.IsLeader()
		if leader && !wasLeader {
			g.seedLeases(time.Now(), true)
		}
		wasLeader = leader
		if leader {
			ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
			err := g.bootstrap(ctx)
			if err == nil {
				err = g.activateCompleted(ctx)
			}
			cancel()
			if err != nil && g.workCtx.Err() == nil {
				g.opts.Logger.Debug("meta bootstrap or activation retry", "error", err)
			}
		}
		if leader && !time.Now().Before(nextExpiry) {
			ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
			if err := g.expireSessions(ctx, time.Now()); err != nil && g.workCtx.Err() == nil {
				g.opts.Logger.Debug("meta lease expiry retry", "error", err)
			}
			cancel()
			nextExpiry = time.Now().Add(g.opts.SessionTTL / 3)
		}

		identity := g.currentIdentity()
		status = engine.Status()
		members := make(map[uint64]bool)
		voter := false
		for _, id := range status.ConfState.Voters {
			members[id] = true
			voter = voter || id == identity.MemberID
		}
		for _, id := range status.ConfState.Learners {
			members[id] = true
		}
		g.transport.Prune(members)
		if !voter || g.state().ActiveVersion() == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
		_, err := g.activate(ctx, true)
		cancel()
		if err != nil {
			continue
		}
		g.mu.Lock()
		if !g.quiescing && g.selfRemoval == nil && g.workCtx.Err() == nil {
			g.lifecycle = "ready"
		}
		g.mu.Unlock()
		g.readyOnce.Do(func() { close(g.ready) })
	}
}
func (g *Group) bootstrap(ctx context.Context) error {
	machine := g.state()
	identity := g.currentIdentity()
	if machine.Latest().Version == 0 {
		if len(g.opts.Founders) == 0 {
			return fault.ErrUnavailable
		}
		now := time.Now().UTC()
		doc := topology.Document{ClusterID: identity.ClusterID, Placement: g.opts.Placement, ChangedBy: identity.NodeName, Origin: topology.OriginFounding, CreatedAt: now, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 1, CreatedAt: now, UpdatedAt: now}}}
		for _, founder := range g.opts.Founders {
			node := founder.Node
			node.CreatedAt = now
			node.UpdatedAt = now
			doc.Nodes = append(doc.Nodes, node)
			if err := doc.SetMemberID(node.Name, founder.MemberID, now); err != nil {
				return err
			}
		}
		if _, err := g.propose(ctx, metaengine.ProposeDocument{Doc: doc, ExpectedVersion: 0}); err != nil {
			return err
		}
	}
	latest := machine.Latest()
	if latest.Origin != topology.OriginFounding || latest.Version != 1 || machine.Active().Version != 0 {
		return nil
	}
	_, ok := machine.CurrentOperator()
	if !ok {
		observed := map[string]topology.GroupStatus{topology.MetaGroupID: g.engine.Load().Status()}
		plan, err := topology.BuildPlan(machine.Active(), latest, observed, g.rules)
		if err != nil {
			return err
		}
		if len(plan.Steps) != 0 {
			return fault.New(fault.OperatorAuthorization, "founding membership differs from durable seed", nil)
		}
		_, err = g.propose(ctx, metaengine.BeginOperatorProposal{Version: latest.Version, Plan: plan, Actor: identity.NodeName, Origin: topology.OriginFounding, At: time.Now().UTC()})
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) Activate(ctx context.Context) (topology.Session, error) {
	return g.activate(ctx, false)
}

func (g *Group) activate(ctx context.Context, automatic bool) (topology.Session, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return topology.Session{}, err
	}
	if err := g.takeSessionToken(ctx); err != nil {
		return topology.Session{}, err
	}
	defer g.releaseSessionToken()
	g.sessionMu.Lock()
	sessionID, deadline := g.ownSession, g.ownDeadline
	paused, previous := g.sessionPaused, g.lastDeactivated
	g.sessionMu.Unlock()
	if automatic && paused {
		return topology.Session{}, fault.ErrUnavailable
	}
	identity := g.currentIdentity()
	machine := g.state()
	active := machine.Active()
	node := active.Node(identity.NodeName)
	if node == nil || !node.State.Places() || active.MemberID(identity.NodeName) != identity.MemberID {
		return topology.Session{}, fault.ErrUnavailable
	}
	if !machine.HasActivated(identity.NodeName) && active.Origin == topology.OriginFounding {
		operator, ok := machine.CurrentOperator()
		if !ok || operator.Status.State != topology.OperatorSucceeded {
			return topology.Session{}, fault.ErrUnavailable
		}
		if _, err := g.propose(ctx, metaengine.BindAdmissionProposal{Admission: metaengine.AdmissionRecord{NodeName: identity.NodeName, InstanceID: identity.InstanceID, MemberID: identity.MemberID, OperatorID: operator.Status.ID}, Founding: true, At: time.Now().UTC()}); err != nil {
			return topology.Session{}, err
		}
	}
	holder, held := machine.LockSession("node/" + identity.NodeName)
	if held && (holder.NodeName != identity.NodeName || holder.InstanceID != identity.InstanceID) {
		return topology.Session{}, fault.New(fault.IdentityMismatch, "node name is held by another process instance", nil)
	}
	if held {
		// A follower may still have the just-revoked session locally. Do not
		// regrant its identity while the revocation is catching up.
		if holder.SessionID == previous {
			return topology.Session{}, fault.ErrUnavailable
		}
		sessionID = holder.SessionID
	}
	if sessionID == "" {
		// Keep this identity across uncertain grant retries. A later activation
		// after successful deactivation needs a fresh ID to fence delayed revokes.
		var nonce [24]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return topology.Session{}, err
		}
		sessionID = "session-" + hex.EncodeToString(nonce[:])
	}
	g.sessionMu.Lock()
	if err := g.ensureRunning(ctx); err != nil {
		g.sessionMu.Unlock()
		return topology.Session{}, err
	}
	g.ownSession = sessionID
	g.sessionMu.Unlock()
	if _, exists := machine.Session(sessionID); !exists {
		if _, err := g.propose(ctx, metaengine.GrantSession{SessionID: sessionID, NodeName: identity.NodeName, InstanceID: identity.InstanceID, TTLSeconds: int(g.opts.SessionTTL / time.Second)}); err != nil {
			return topology.Session{}, err
		}
	}
	if !held {
		if _, err := g.propose(ctx, metaengine.AcquireLock{Key: "node/" + identity.NodeName, SessionID: sessionID}); err != nil {
			return topology.Session{}, err
		}
	}
	ttl := g.opts.SessionTTL
	if record, exists := machine.Session(sessionID); exists {
		session, err := sessionFromRecord(record)
		if err != nil {
			return topology.Session{}, err
		}
		ttl = session.TTL
	}
	if paused || time.Until(deadline) <= ttl-ttl/3 {
		deadline, err := g.renewSession(ctx, sessionID, identity.InstanceID, ttl)
		if err != nil {
			return topology.Session{}, err
		}
		g.sessionMu.Lock()
		if err := g.ensureRunning(ctx); err != nil {
			g.sessionMu.Unlock()
			return topology.Session{}, err
		}
		g.ownDeadline = deadline
		g.sessionPaused = false
		g.sessionMu.Unlock()
	}
	return topology.Session{ID: sessionID, NodeName: identity.NodeName, InstanceID: identity.InstanceID, TTL: ttl}, nil
}
