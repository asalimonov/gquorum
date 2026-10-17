package metaservice

import (
	"context"
	"encoding/json"
	"time"

	"github.com/asalimonov/gquorum/topology"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
)

type leaseSeedMode uint8

const (
	preserveLeases leaseSeedMode = iota
	resetLeases
)

func (g *Group) seedLeases(now time.Time, mode leaseSeedMode) {
	sessions := g.state().Sessions()
	g.leases.mu.Lock()
	defer g.leases.mu.Unlock()
	if mode == resetLeases {
		g.leases.deadlines = make(map[string]time.Time, len(sessions))
	}
	for _, session := range sessions {
		if _, exists := g.leases.deadlines[session.SessionID]; !exists {
			g.leases.deadlines[session.SessionID] = now.Add(time.Duration(session.TTLSeconds) * time.Second)
		}
	}
}
func (g *Group) expireSessions(ctx context.Context, now time.Time) error {
	g.seedLeases(now, preserveLeases)
	for _, id := range g.leases.expired(now) {
		if !g.IsLeader() {
			return nil
		}
		session, exists := g.state().Session(id)
		if !exists {
			g.leases.remove(id)
			continue
		}
		if g.probeSession(ctx, session) {
			g.leases.refreshTTL(id, time.Now(), time.Duration(session.TTLSeconds)*time.Second)
			continue
		}
		// Serialize the final expiry decision with keepalive acknowledgements.
		g.leaseGate.Lock()
		if !g.leases.isExpired(id, now) {
			g.leaseGate.Unlock()
			continue
		}
		_, err := g.propose(ctx, metaengine.RevokeSession{SessionID: id, Reason: "session TTL expired"})
		if err == nil {
			g.leases.remove(id)
		}
		g.leaseGate.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) probeSession(parent context.Context, session metaengine.SessionRecord) bool {
	identity := g.currentIdentity()
	if session.NodeName == identity.NodeName {
		return session.InstanceID == identity.InstanceID && identity.MemberID == g.eligibleMemberID(session.NodeName) &&
			g.ctx.Err() == nil
	}
	ctx, cancel := context.WithTimeout(parent, g.opts.Timing.DialTimeout)
	defer cancel()
	address, err := g.resolveNode(ctx, session.NodeName)
	if err != nil {
		return false
	}
	reply, err := g.transport.CallAddress(ctx, address, "IdentityPing", nil)
	if err != nil || metatransport.ReplyError(reply) != nil {
		return false
	}
	var observed topology.Identity
	if err = json.Unmarshal(reply.Data, &observed); err != nil {
		return false
	}
	return observed.ClusterID == identity.ClusterID && observed.NodeName == session.NodeName &&
		observed.InstanceID == session.InstanceID && observed.MemberID == g.eligibleMemberID(session.NodeName)
}
