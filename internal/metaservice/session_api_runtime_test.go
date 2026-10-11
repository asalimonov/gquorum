package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"google.golang.org/protobuf/proto"
)

func TestActivateDeactivateReleasesLocksAndFencesDelayedRevoke(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var follower *Group
	for _, g := range groups {
		if g != leader {
			follower = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	s, err := follower.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = follower.KeepAlive(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	if err = leader.KeepAlive(ctx, s.ID); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("renewed another node", err)
	}
	if err = leader.Deactivate(ctx, s.ID); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("revoked another node", err)
	}
	if err = follower.AcquireLock(ctx, "node/"+leader.currentIdentity().NodeName, s.ID); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("took another node identity lock", err)
	}
	if err = follower.AcquireLock(ctx, "application", s.ID); err != nil {
		t.Fatal(err)
	}
	if err = follower.AcquireLock(ctx, "application", s.ID); err != nil {
		t.Fatal("idempotent lock retry", err)
	}
	waitPublic(t, func() bool {
		held, exists, e := leader.LockHolder(ctx, "application")
		return e == nil && exists && held.ID == s.ID
	})
	own, err := leader.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = leader.AcquireLock(ctx, "application", own.ID); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("stole live lock", err)
	}
	if err = follower.Deactivate(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	if follower.OwnSessionLive() {
		t.Fatal("deactivation kept serving lease")
	}
	waitPublic(t, func() bool {
		for _, g := range groups {
			if _, ok := g.state().Session(s.ID); ok {
				return false
			}
		}
		return true
	})
	time.Sleep(5 * follower.opts.Timing.TickInterval)
	if follower.OwnSessionLive() {
		t.Fatal("automatic activation ignored explicit deactivation")
	}
	if _, held, err := leader.LockHolder(ctx, "application"); err != nil || held {
		t.Fatal(held, err)
	}
	if err = follower.Deactivate(ctx, s.ID); err != nil {
		t.Fatal("revocation retry was not idempotent", err)
	}
	next, err := follower.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID == s.ID || !follower.OwnSessionLive() {
		t.Fatal("reused deactivated session", next)
	}
	if err = follower.Deactivate(ctx, s.ID); err != nil {
		t.Fatal("delayed revoke failed", err)
	}
	if _, ok := leader.state().Session(next.ID); !ok || !follower.OwnSessionLive() {
		t.Fatal("delayed revoke affected new session")
	}
	if err = leader.AcquireLock(ctx, "application", own.ID); err != nil {
		t.Fatal(err)
	}
	if err = follower.AcquireLock(ctx, "application", next.ID); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("stole reacquired lock", err)
	}
	if leader.state().Latest().Version != 1 {
		t.Fatal("session commands churned topology")
	}
}
func TestSessionRPCRejectsAnotherProcessInstanceAndReservedNodeLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	g := readyPublic(t, publicFoundingCluster(t, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := g.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	good := g.transport.Header()
	bad := proto.Clone(good).(*metapb.Header)
	bad.InstanceId = "other-process"
	for _, proposal := range []any{
		metaengine.GrantSession{SessionID: "foreign", NodeName: s.NodeName, InstanceID: s.InstanceID, TTLSeconds: 3},
		metaengine.AcquireLock{Key: "application", SessionID: s.ID},
		metaengine.RevokeSession{SessionID: s.ID},
	} {
		data, err := metaengine.Encode(proposal)
		if err != nil {
			t.Fatal(err)
		}
		if err = metatransport.ReplyError(g.Call(ctx, "ProposeMeta", bad, data)); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("accepted wrong instance", err)
		}
	}
	if err = metatransport.ReplyError(keepAliveCall(g, bad, s.ID, s.InstanceID)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	data, err := metaengine.Encode(metaengine.AcquireLock{Key: "node/other", SessionID: s.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err = metatransport.ReplyError(g.Call(ctx, "ProposeMeta", good, data)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("reserved node lock accepted", err)
	}
	if _, ok := g.state().Session(s.ID); !ok {
		t.Fatal("rejected RPC changed session")
	}
	if _, ok := g.state().Session("foreign"); ok {
		t.Fatal("rejected RPC granted session")
	}
}
