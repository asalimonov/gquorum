//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestConsistentMetadataReadsAndStop(t *testing.T) {
	t.Parallel()
	g := singleMetaFixture(t)[0]
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := g.ReadTopology(ctx)
	if err != nil || view.AppliedIndex == 0 || view.Active.Version == 0 || view.Target.Version != view.Active.Version {
		t.Fatal(view, err)
	}
	if _, err := g.propose(ctx, metaengine.GrantSession{SessionID: "read-session", NodeName: "n1", InstanceID: "read-instance", TTLSeconds: 3}); err != nil {
		t.Fatal(err)
	}
	if err := g.AcquireLock(ctx, "read-key", "read-session"); err == nil {
		t.Fatal("foreign session unexpectedly accepted by public acquisition")
	}
	if _, err := g.propose(ctx, metaengine.AcquireLock{Key: "read-key", SessionID: "read-session"}); err != nil {
		t.Fatal(err)
	}
	session, err := g.ReadLockHolder(ctx, "read-key")
	if err != nil || !session.Held || session.Session.ID != "read-session" || session.Session.TTL != 3*time.Second {
		t.Fatal(session.Session, session.Held, err)
	}
	if err := g.ReadBarrier(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.propose(ctx, metaengine.RevokeSession{SessionID: "read-session"}); err != nil {
		t.Fatal(err)
	}
	if held, err := g.ReadLockHolder(ctx, "read-key"); err != nil || held.Held {
		t.Fatal(held.Held, err)
	}
	if err := g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.ReadTopology(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if err := g.ReadBarrier(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

func TestMetaReadOptionsReachEngine(t *testing.T) {
	t.Parallel()
	g := singleMetaFixtureWith(t, func(o *Options) { o.Reads = raftengine.ReadOptions{MaxResultBytes: 1} })[0]
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.ReadTopology(ctx); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	// Internal control reads retain bounded library defaults even when callers
	// deliberately restrict their public results to one byte.
	if _, err := g.ReadAuthorityFloors(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	key := topology.AuthorityKey{ClusterID: g.Identity().ClusterID, GroupID: topology.MetaGroupID, Revision: 999999}
	if found, err := g.ReadGrant(ctx, key); err != nil || found.Found {
		t.Fatal(found.Found, err)
	}
	if proof, err := g.readAuthorityProof(ctx, key, ""); err != nil || proof.Grant != nil {
		t.Fatal(proof, err)
	}
	if err := g.ReadBarrier(ctx); err != nil {
		t.Fatal(err)
	}
}
