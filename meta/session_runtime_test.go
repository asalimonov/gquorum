package meta

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
)

func waitPublic(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("meta condition did not converge")
}
func readyPublic(t *testing.T, groups []*Group) *Group {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, g := range groups {
		if err := g.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range groups {
		if g.IsLeader() {
			return g
		}
	}
	t.Fatal("missing meta leader")
	return nil
}
func keepAliveCall(g *Group, header *metapb.Header, session, instance string) *metapb.Reply {
	data, _ := json.Marshal(map[string]string{"session_id": session, "instance_id": instance})
	return g.Call(context.Background(), "KeepAlive", header, data)
}

func TestMetaIdentityPingAndExactKeepAlive(t *testing.T) {
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	identity := leader.currentIdentity()
	header := leader.transport.Header()
	var observed Identity
	reply := leader.Call(context.Background(), "IdentityPing", header, nil)
	if err := json.Unmarshal(reply.Data, &observed); err != nil || observed != identity {
		t.Fatal("ping lost process identity", observed, err)
	}
	session, err := leader.Activate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = metatransport.ReplyError(keepAliveCall(leader, header, session.ID, identity.InstanceID)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct{ session, instance string }{{"unknown", identity.InstanceID}, {session.ID, "wrong-instance"}} {
		if err = metatransport.ReplyError(keepAliveCall(leader, header, bad.session, bad.instance)); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("accepted wrong session identity", err)
		}
	}
	for _, g := range groups {
		if g != leader {
			if err = metatransport.ReplyError(keepAliveCall(g, header, session.ID, identity.InstanceID)); !errors.Is(err, fault.ErrNotLeader) {
				t.Fatal("follower renewed lease", err)
			}
			break
		}
	}
}

func TestMetaExpiryPreservesLiveExactInstanceAndRevokesDeadLock(t *testing.T) {
	g := readyPublic(t, publicFoundingCluster(t, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := g.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Expiry probes the exact live process before deciding to revoke.
	expired := time.Now().Add(-time.Second)
	g.leases.refreshTTL(session.ID, expired, -time.Second)
	if err = g.expireSessions(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.state().Session(session.ID); !ok {
		t.Fatal("revoked live process")
	}
	if _, err = g.propose(ctx, metaengine.GrantSession{SessionID: "dead", NodeName: "dead-node", InstanceID: "dead-instance", TTLSeconds: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err = g.propose(ctx, metaengine.AcquireLock{SessionID: "dead", Key: "node/dead-node"}); err != nil {
		t.Fatal(err)
	}
	g.leases.refreshTTL("dead", expired, -time.Second)
	if err = g.expireSessions(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.state().Session("dead"); ok {
		t.Fatal("dead session survived")
	}
	if _, ok := g.state().LockSession("node/dead-node"); ok {
		t.Fatal("dead process name stayed locked")
	}
}

func TestMetaActivationUsesContextWhileAnotherActivationIsInFlight(t *testing.T) {
	g := readyPublic(t, publicFoundingCluster(t, 1))
	g.sessionToken <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := g.Activate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("activation ignored waiting caller deadline", err)
	}
	<-g.sessionToken
}

func TestMetaFollowerDocumentCASAndOwnedHistory(t *testing.T) {
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var follower *Group
	for _, g := range groups {
		if g != leader {
			follower = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc, _ := follower.Latest(ctx)
	doc.Nodes[0].Labels = map[string]string{"zone": "a"}
	staged, err := follower.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	if staged.Version != 2 {
		t.Fatal(staged)
	}
	_, err = follower.Stage(ctx, doc, 1)
	var conflict *fault.Error
	if !errors.As(err, &conflict) || conflict.Code != fault.VersionConflict || conflict.ExpectedVersion != 1 || conflict.CurrentVersion != 2 {
		t.Fatal("lost leader CAS version", err)
	}
	waitPublic(t, func() bool { return follower.state().Latest().Version == 2 })
	history, err := follower.History(ctx, Page{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Documents) != 1 || history.Documents[0].Version != 2 || history.OldestVersion != 1 {
		t.Fatal(history)
	}
	history.Documents[0].Nodes[0].Labels["zone"] = "mutated"
	retained, _ := follower.Get(ctx, 2)
	if retained.Nodes[0].Labels["zone"] != "a" {
		t.Fatal("history exposed mutable state")
	}
	active, _ := follower.Active(ctx)
	if active.Version != 1 {
		t.Fatal("staging approved topology")
	}
}
