package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// removalReason uses approved intent, not an unapproved staged edit. The local
// pin's sticky seen marker handles a crash after persistence before notification.
func removalReason(id Identity, target topology.Document, seen bool, cs raftpb.ConfState, index uint64) string {
	if id.MemberID == 0 || !seen || containsMember(cs, id.MemberID) || target.Version == 0 || index == 0 {
		return ""
	}
	node := target.Node(id.NodeName)
	if node == nil {
		return ""
	}
	switch {
	case node.State == topology.NodeExcluded:
		return fmt.Sprintf("node %q member %d removed from _meta at applied index %d by approved exclusion v%d", id.NodeName, id.MemberID, index, target.Version)
	case target.MemberID(id.NodeName) != 0 && target.MemberID(id.NodeName) != id.MemberID:
		return fmt.Sprintf("node %q member %d removed from _meta at applied index %d by approved replacement with member %d v%d", id.NodeName, id.MemberID, index, target.MemberID(id.NodeName), target.Version)
	default:
		return ""
	}
}
func (g *Group) observeRemoval(identity Identity, target topology.Document, cs raftpb.ConfState, index uint64) {
	g.mu.Lock()
	if g.identity != identity || g.selfRemoval != nil || g.quiescing {
		g.mu.Unlock()
		return
	}
	g.memberSeen = g.memberSeen || containsMember(cs, identity.MemberID)
	reason := removalReason(identity, target, g.memberSeen, cs, index)
	if reason == "" {
		g.mu.Unlock()
		return
	}
	g.selfRemoval = &topology.SelfRemovalStatus{Fenced: true, MemberID: identity.MemberID, Version: target.Version, AppliedIndex: index, Reason: reason}
	g.lifecycle = "removed"
	if g.workCancel != nil {
		g.workCancel()
	}
	g.mu.Unlock()
	if g.ready != nil {
		g.readyOnce.Do(func() { close(g.ready) })
	}
	g.sessionMu.Lock()
	g.ownDeadline = time.Time{}
	g.sessionPaused = true
	g.sessionMu.Unlock()
	if g.opts.Events != nil {
		e := g.observerEvent(events.SelfRemoved)
		e.GroupID = topology.MetaGroupID
		e.Version = target.Version
		e.State = "fenced"
		g.opts.Events.Publish(e)
	}
	if g.opts.OnSelfRemoved != nil {
		select {
		case g.removalSignal <- struct{}{}:
		default:
		}
	}
}
func (g *Group) notifyRemoval(ctx context.Context) {
	defer g.removalWG.Done()
	select {
	case <-ctx.Done():
		return
	case <-g.removalSignal:
	}
	if ctx.Err() != nil {
		return
	}
	g.mu.Lock()
	if g.selfRemoval == nil || g.selfRemoval.Notified {
		g.mu.Unlock()
		return
	}
	reason := g.selfRemoval.Reason
	g.selfRemoval.Notified = true
	g.mu.Unlock()
	// Callers schedule teardown with RequestStop and return. A callback must not
	// wait on the Stop/Quiesce that must join this callback itself.
	err := g.opts.OnSelfRemoved(ctx, reason)
	if err == nil || ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return
	}
	g.mu.Lock()
	g.selfRemoval.ErrorCode = "callback_failed"
	version := g.selfRemoval.Version
	g.mu.Unlock()
	if g.opts.Logger != nil {
		g.opts.Logger.Error("meta self-removal callback failed", "reason", reason, "error", err)
	}
	if g.opts.Events != nil {
		e := g.observerEvent(events.RuntimeError)
		e.GroupID = topology.MetaGroupID
		e.Version = version
		e.Code = "self_removal_callback_failed"
		g.opts.Events.Publish(e)
	}
}
