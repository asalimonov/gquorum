//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
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

func TestMetaExpiryPreservesLiveExactInstanceAndRevokesDeadLock(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
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
	// The admitted node is alive, but this earlier process incarnation is not.
	if _, err = g.propose(ctx, metaengine.GrantSession{SessionID: "dead", NodeName: session.NodeName, InstanceID: "dead-instance", TTLSeconds: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err = g.propose(ctx, metaengine.AcquireLock{SessionID: "dead", Key: "job/abandoned"}); err != nil {
		t.Fatal(err)
	}
	g.leases.refreshTTL("dead", expired, -time.Second)
	if err = g.expireSessions(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.state().Session("dead"); ok {
		t.Fatal("dead session survived")
	}
	if _, ok := g.state().LockSession("job/abandoned"); ok {
		t.Fatal("dead process lock survived")
	}
}

func TestMetaActivationUsesContextWhileAnotherActivationIsInFlight(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	g.sessionToken <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := g.Activate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("activation ignored waiting caller deadline", err)
	}
	<-g.sessionToken
}
