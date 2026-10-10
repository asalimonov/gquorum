package meta

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) run() {
	defer func() { g.wg.Done(); go g.Stop(context.Background()) }()
	ticker := time.NewTicker(g.opts.Timing.TickInterval)
	defer ticker.Stop()
	var wasLeader bool
	var nextExpiry time.Time
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
		}
		engine := g.engine.Load()
		if engine == nil {
			g.fail(fault.New(fault.Unsupported, "join orchestration is not yet wired", nil))
			return
		}
		if err := engine.Error(); err != nil {
			if g.ctx.Err() == nil {
				g.fail(err)
			}
			return
		}
		if err := g.transport.Error(); err != nil {
			g.fail(err)
			return
		}
		leader := g.IsLeader()
		if leader && !wasLeader {
			g.seedLeases(time.Now(), true)
		}
		wasLeader = leader
		if leader {
			ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
			err := g.bootstrap(ctx)
			if err == nil {
				err = g.activateCompleted(ctx)
			}
			cancel()
			if err != nil && g.ctx.Err() == nil {
				g.opts.Logger.Debug("meta bootstrap or activation retry", "error", err)
			}
		}
		if leader && !time.Now().Before(nextExpiry) {
			ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
			if err := g.expireSessions(ctx, time.Now()); err != nil && g.ctx.Err() == nil {
				g.opts.Logger.Debug("meta lease expiry retry", "error", err)
			}
			cancel()
			nextExpiry = time.Now().Add(g.opts.SessionTTL / 3)
		}

		identity := g.currentIdentity()
		status := engine.Status()
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
		if !voter || g.state().Active().Version == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
		_, err := g.Activate(ctx)
		cancel()
		if err != nil {
			continue
		}
		g.mu.Lock()
		g.lifecycle = "ready"
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
	_, ok := machine.CurrentExecution()
	if !ok {
		observed := map[string]topology.GroupStatus{topology.MetaGroupID: g.engine.Load().Status()}
		plan, err := topology.BuildPlan(machine.Active(), latest, observed, g.rules)
		if err != nil {
			return err
		}
		if len(plan.Steps) != 0 {
			return fault.New(fault.ExecutionAuthorization, "founding membership differs from durable seed", nil)
		}
		_, err = g.propose(ctx, metaengine.BeginExecutionProposal{Version: latest.Version, Plan: plan, Actor: identity.NodeName, Origin: topology.OriginFounding, At: time.Now().UTC()})
		if err != nil {
			return err
		}
	}
	return nil
}

type Session struct {
	ID, NodeName, InstanceID string
	TTL                      time.Duration
}

func (g *Group) Activate(ctx context.Context) (Session, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return Session{}, err
	}
	select {
	case g.sessionToken <- struct{}{}:
		defer func() { <-g.sessionToken }()
	case <-ctx.Done():
		return Session{}, ctx.Err()
	case <-g.ctx.Done():
		return Session{}, fault.ErrStopped
	}
	g.sessionMu.Lock()
	sessionID, deadline := g.ownSession, g.ownDeadline
	g.sessionMu.Unlock()
	identity := g.currentIdentity()
	machine := g.state()
	active := machine.Active()
	node := active.Node(identity.NodeName)
	if node == nil || !node.State.Places() || active.MemberID(identity.NodeName) != identity.MemberID {
		return Session{}, fault.ErrUnavailable
	}
	if !machine.HasActivated(identity.NodeName) && active.Origin == topology.OriginFounding {
		execution, ok := machine.CurrentExecution()
		if !ok || execution.Status.State != topology.ExecutionSucceeded {
			return Session{}, fault.ErrUnavailable
		}
		if _, err := g.propose(ctx, metaengine.BindAdmissionProposal{Admission: metaengine.AdmissionRecord{NodeName: identity.NodeName, InstanceID: identity.InstanceID, MemberID: identity.MemberID, ExecutionID: execution.Status.ID}, Founding: true, At: time.Now().UTC()}); err != nil {
			return Session{}, err
		}
	}
	holder, held := machine.LockSession("node/" + identity.NodeName)
	if held && (holder.NodeName != identity.NodeName || holder.InstanceID != identity.InstanceID) {
		return Session{}, fault.New(fault.IdentityMismatch, "node name is held by another process instance", nil)
	}
	if held {
		sessionID = holder.SessionID
	}
	if sessionID == "" {
		// The exact process tuple is stable across retries, including uncertain grant outcomes.
		data, _ := json.Marshal([3]string{identity.ClusterID, identity.NodeName, identity.InstanceID})
		hash := sha256.Sum256(data)
		sessionID = "session-" + hex.EncodeToString(hash[:])
	}
	g.sessionMu.Lock()
	g.ownSession = sessionID
	g.sessionMu.Unlock()
	if _, exists := machine.Session(sessionID); !exists {
		if _, err := g.propose(ctx, metaengine.GrantSession{SessionID: sessionID, NodeName: identity.NodeName, InstanceID: identity.InstanceID, TTLSeconds: int(g.opts.SessionTTL / time.Second)}); err != nil {
			return Session{}, err
		}
	}
	if !held {
		if _, err := g.propose(ctx, metaengine.AcquireLock{Key: "node/" + identity.NodeName, SessionID: sessionID}); err != nil {
			return Session{}, err
		}
	}
	if time.Until(deadline) <= g.opts.SessionTTL-g.opts.SessionTTL/3 {
		data, _ := json.Marshal(struct {
			SessionID  string `json:"session_id"`
			InstanceID string `json:"instance_id"`
		}{sessionID, identity.InstanceID})
		var replyData []byte
		if g.IsLeader() {
			reply := g.Call(ctx, "KeepAlive", g.transport.Header(), data)
			if err := metatransport.ReplyError(reply); err != nil {
				return Session{}, err
			}
			replyData = reply.Data
		} else {
			engine := g.engine.Load()
			if engine == nil || engine.RaftStatus().Lead == 0 {
				return Session{}, fault.ErrUnavailable
			}
			address, err := g.resolveMember(ctx, engine.RaftStatus().Lead)
			if err != nil {
				return Session{}, err
			}
			reply, err := g.transport.CallAddress(ctx, address, "KeepAlive", data)
			if err != nil {
				return Session{}, err
			}
			if err = metatransport.ReplyError(reply); err != nil {
				return Session{}, err
			}
			replyData = reply.Data
		}
		var response struct {
			ExpiresIn int `json:"expires_in"`
		}
		if err := json.Unmarshal(replyData, &response); err != nil || response.ExpiresIn <= 0 {
			return Session{}, fault.ErrUnavailable
		}
		g.sessionMu.Lock()
		g.ownDeadline = time.Now().Add(time.Duration(response.ExpiresIn) * time.Second)
		g.sessionMu.Unlock()
	}
	return Session{ID: sessionID, NodeName: identity.NodeName, InstanceID: identity.InstanceID, TTL: g.opts.SessionTTL}, nil
}
