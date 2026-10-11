package metaservice

import (
	"context"
	"encoding/json"
	"github.com/asalimonov/gquorum/topology"
	"math"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
)

func (g *Group) takeSessionToken(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case g.sessionToken <- struct{}{}:
		if err := ctx.Err(); err != nil {
			g.releaseSessionToken()
			return err
		}
		if err := g.ensureRunning(ctx); err != nil {
			g.releaseSessionToken()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-g.ctx.Done():
		return fault.ErrStopped
	}
}
func (g *Group) releaseSessionToken() { <-g.sessionToken }
func sessionFromRecord(s metaengine.SessionRecord) (topology.Session, error) {
	if s.SessionID == "" || s.NodeName == "" || s.InstanceID == "" || s.TTLSeconds <= 0 || int64(s.TTLSeconds) > math.MaxInt64/int64(time.Second) {
		return topology.Session{}, fault.New(fault.Unreadable, "invalid replicated session identity or TTL", nil)
	}
	return topology.Session{ID: s.SessionID, NodeName: s.NodeName, InstanceID: s.InstanceID, TTL: time.Duration(s.TTLSeconds) * time.Second}, nil
}
func (g *Group) ownedSession(id string) (topology.Session, error) {
	if id == "" {
		return topology.Session{}, fault.ErrInvalid
	}
	s, exists := g.state().Session(id)
	if !exists {
		return topology.Session{}, fault.ErrIdentityMismatch
	}
	identity := g.currentIdentity()
	if s.NodeName != identity.NodeName || s.InstanceID != identity.InstanceID {
		return topology.Session{}, fault.ErrIdentityMismatch
	}
	return sessionFromRecord(s)
}
func (g *Group) renewSession(ctx context.Context, id, instance string, ttl time.Duration) (time.Time, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return time.Time{}, err
	}
	data, err := json.Marshal(struct {
		SessionID  string `json:"session_id"`
		InstanceID string `json:"instance_id"`
	}{id, instance})
	if err != nil {
		return time.Time{}, err
	}
	var replyData []byte
	started := time.Now()
	if g.IsLeader() {
		reply := g.Call(ctx, "KeepAlive", g.transport.Header(), data)
		if err := metatransport.ReplyError(reply); err != nil {
			return time.Time{}, err
		}
		replyData = reply.Data
	} else {
		engine := g.engine.Load()
		if engine == nil || engine.RaftStatus().Lead == 0 {
			return time.Time{}, fault.ErrUnavailable
		}
		address, err := g.resolveMember(ctx, engine.RaftStatus().Lead)
		if err != nil {
			return time.Time{}, err
		}
		reply, err := g.transport.CallAddress(ctx, address, "KeepAlive", data)
		if err != nil {
			return time.Time{}, err
		}
		if err = metatransport.ReplyError(reply); err != nil {
			return time.Time{}, err
		}
		replyData = reply.Data
	}
	var response struct {
		ExpiresIn int `json:"expires_in"`
	}
	if err := json.Unmarshal(replyData, &response); err != nil || response.ExpiresIn <= 0 || int64(response.ExpiresIn) > int64(ttl/time.Second) {
		return time.Time{}, fault.ErrUnavailable
	}
	// Use the request start to avoid extending the acknowledged lease by the
	// response's travel time. A reply that took the entire lease is unusable.
	deadline := started.Add(time.Duration(response.ExpiresIn) * time.Second)
	if !deadline.After(time.Now()) {
		return time.Time{}, fault.ErrUnavailable
	}
	return deadline, nil
}

// KeepAlive renews this exact process's session at the current meta leader.
// It cannot renew another process or implicitly resume a deactivated session.
func (g *Group) KeepAlive(ctx context.Context, id string) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	if err := g.takeSessionToken(ctx); err != nil {
		return err
	}
	defer g.releaseSessionToken()
	s, err := g.ownedSession(id)
	if err != nil {
		return err
	}
	g.sessionMu.Lock()
	paused := g.sessionPaused && (id == g.ownSession || id == g.lastDeactivated)
	g.sessionMu.Unlock()
	if paused {
		return fault.ErrUnavailable
	}
	deadline, err := g.renewSession(ctx, id, s.InstanceID, s.TTL)
	if err != nil {
		return err
	}
	g.sessionMu.Lock()
	if id == g.ownSession && !g.sessionPaused {
		g.ownDeadline = deadline
	}
	g.sessionMu.Unlock()
	return nil
}

// Deactivate revokes this process's session and every lock it owns. Automatic
// activation remains paused until an explicit successful Activate. Retry the
// same ID when the proposal outcome is unknown; successful retries are safe.
func (g *Group) Deactivate(ctx context.Context, id string) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	if id == "" {
		return fault.ErrInvalid
	}
	if err := g.takeSessionToken(ctx); err != nil {
		return err
	}
	defer g.releaseSessionToken()
	g.sessionMu.Lock()
	own, previous := g.ownSession, g.lastDeactivated
	g.sessionMu.Unlock()
	if _, exists := g.state().Session(id); exists {
		if _, err := g.ownedSession(id); err != nil {
			return err
		}
	} else if id != own && id != previous {
		// An absent ID has no effect to revoke. Do not change the current
		// process's pause/fencing state for an old or unknown retry.
		return nil
	}
	if id == own {
		g.sessionMu.Lock()
		g.sessionPaused = true
		g.ownDeadline = time.Time{}
		// Fence this ID before submitting: a canceled proposal may still commit.
		g.ownSession = ""
		g.lastDeactivated = id
		g.sessionMu.Unlock()
	}
	if _, err := g.propose(ctx, metaengine.RevokeSession{SessionID: id}); err != nil {
		return err
	}
	return nil
}

// AcquireLock takes an application lock with this exact process's session.
// The node/ namespace is reserved for the matching node's activation lock.
func (g *Group) AcquireLock(ctx context.Context, key, id string) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	if key == "" {
		return fault.ErrInvalid
	}
	if err := g.takeSessionToken(ctx); err != nil {
		return err
	}
	defer g.releaseSessionToken()
	g.sessionMu.Lock()
	paused := g.sessionPaused && id == g.lastDeactivated
	g.sessionMu.Unlock()
	if paused {
		return fault.ErrUnavailable
	}
	if _, err := g.ownedSession(id); err != nil {
		return err
	}
	if strings.HasPrefix(key, "node/") && key != "node/"+g.currentIdentity().NodeName {
		return fault.ErrIdentityMismatch
	}
	_, err := g.propose(ctx, metaengine.AcquireLock{Key: key, SessionID: id})
	return err
}

// LockHolder returns the owned, locally applied session holding key.
// It is not a linearizable or liveness-confirming read.
func (g *Group) LockHolder(ctx context.Context, key string) (topology.Session, bool, error) {
	if err := ctx.Err(); err != nil {
		return topology.Session{}, false, err
	}
	if key == "" {
		return topology.Session{}, false, fault.ErrInvalid
	}
	s, exists := g.state().LockSession(key)
	if !exists {
		return topology.Session{}, false, nil
	}
	result, err := sessionFromRecord(s)
	return result, err == nil, err
}

// ServeOverride returns the persisted local operator input (zero means auto).
// The caller decides its serving semantics; it never changes Raft membership.
func (g *Group) ServeOverride() int { g.mu.RLock(); defer g.mu.RUnlock(); return g.serveOverride }

// SetServeOverride changes only this handle's pinned local store. It remains
// available without a Raft leader and takes effect after successful persistence.
func (g *Group) SetServeOverride(ctx context.Context, value int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value < 0 || value > g.rules.Limits.MaxNodes {
		return fault.ErrInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.store == nil || g.identity.MemberID == 0 {
		return fault.ErrUnavailable
	}
	if g.quiescing || g.lifecycle == "stopped" || g.lifecycle == "stopping" {
		return fault.ErrStopped
	}
	if err := g.store.SaveOverride(value); err != nil {
		return err
	}
	g.serveOverride = value
	return nil
}
