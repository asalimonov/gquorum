//go:build functional

package metaservice

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
)

func TestLockHolderUsesOwnedLocallyAppliedSession(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.command(t, metaengine.GrantSession{SessionID: "one", NodeName: "n1", InstanceID: "process", TTLSeconds: 3})
	f.command(t, metaengine.AcquireLock{SessionID: "one", Key: "application"})
	got, held, err := f.g.LockHolder(context.Background(), "application")
	if err != nil || !held || got.ID != "one" || got.NodeName != "n1" || got.InstanceID != "process" || got.TTL != 3*time.Second {
		t.Fatal(got, held, err)
	}
	got.ID = "changed"
	if got, _, _ := f.g.LockHolder(context.Background(), "application"); got.ID != "one" {
		t.Fatal("aliased read")
	}
	if _, held, err = f.g.LockHolder(context.Background(), "missing"); err != nil || held {
		t.Fatal(held, err)
	}
	if _, _, err = f.g.LockHolder(context.Background(), ""); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = f.g.LockHolder(ctx, "application"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSessionOwnershipAndTTLValidation(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.identity.InstanceID = "process"
	for _, s := range []metaengine.GrantSession{{SessionID: "mine", NodeName: "n1", InstanceID: "process", TTLSeconds: 3}, {SessionID: "other-node", NodeName: "n2", InstanceID: "process", TTLSeconds: 3}, {SessionID: "other-instance", NodeName: "n1", InstanceID: "old", TTLSeconds: 3}} {
		f.command(t, s)
	}
	if _, err := f.g.ownedSession("mine"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"other-node", "other-instance", "missing"} {
		if _, err := f.g.ownedSession(id); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal(id, err)
		}
	}
	if _, err := f.g.ownedSession(""); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	for _, s := range []metaengine.SessionRecord{{}, {SessionID: "id", NodeName: "n", InstanceID: "i", TTLSeconds: 0}, {SessionID: "id", NodeName: "n", InstanceID: "i", TTLSeconds: math.MaxInt}} {
		if _, err := sessionFromRecord(s); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal(s, err)
		}
	}
}

func TestSessionMutableAPIsRefuseStoppedHandle(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	for _, call := range []func(context.Context) error{
		func(ctx context.Context) error { return f.g.KeepAlive(ctx, "one") },
		func(ctx context.Context) error { return f.g.Deactivate(ctx, "one") },
		func(ctx context.Context) error { return f.g.AcquireLock(ctx, "application", "one") },
	} {
		if err := call(context.Background()); !errors.Is(err, fault.ErrStopped) {
			t.Fatal(err)
		}
	}
}

func TestServeOverridePersistsClearsAndStaysOutsideConsensus(t *testing.T) {
	t.Parallel()
	o := optionsForTest(t)
	g, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(context.Background()) })
	if err = g.SetServeOverride(context.Background(), 1); err != nil || g.ServeOverride() != 1 {
		t.Fatal(err)
	}
	if g.state().Latest().Version != 0 || g.OwnSessionLive() {
		t.Fatal("local override changed consensus or manufactured session")
	}
	if err = g.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(restartOptions(t, g))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Stop(context.Background()) })
	if reopened.ServeOverride() != 1 {
		t.Fatal("override lost on restart")
	}
	if err = reopened.SetServeOverride(context.Background(), 0); err != nil || reopened.ServeOverride() != 0 {
		t.Fatal(err)
	}
	stored, err := reopened.store.LoadOverride()
	if err != nil || stored != 0 {
		t.Fatal(stored, err)
	}
	if err = g.SetServeOverride(context.Background(), 1); !errors.Is(err, fault.ErrStopped) || g.ServeOverride() != 1 {
		t.Fatal("stopped handle accepted write", err)
	}
}

func TestServeOverrideInvalidOrCanceledDoesNotWrite(t *testing.T) {
	t.Parallel()
	g, err := New(optionsForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(context.Background()) })
	if err = g.SetServeOverride(context.Background(), -1); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if err = g.SetServeOverride(context.Background(), g.rules.Limits.MaxNodes+1); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = g.SetServeOverride(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stored, err := g.store.LoadOverride()
	if err != nil || stored != 0 || g.ServeOverride() != 0 {
		t.Fatal(stored, err)
	}
}

func TestServeOverrideBindsToVerifiedLocalIdentity(t *testing.T) {
	t.Parallel()
	g, err := New(optionsForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(context.Background()) })
	if err = g.SetServeOverride(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err = g.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"node", "cluster"} {
		opts := restartOptions(t, g)
		if kind == "node" {
			opts.Identity.NodeName = "other"
			opts.Node.Name = "other"
			opts.Founders[0].Node.Name = "other"
		} else {
			opts.Identity.ClusterID = "other"
		}
		if other, err := New(opts); !errors.Is(err, fault.ErrIdentityMismatch) {
			if other != nil {
				other.Stop(context.Background())
			}
			t.Fatal("accepted changed pin", kind, err)
		}
	}
	reopened, err := New(restartOptions(t, g))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Stop(context.Background()) })
	if reopened.ServeOverride() != 1 {
		t.Fatal("rejected constructor changed persisted override")
	}
	// A fresh joiner has no pin/store until receiving an authorized admission.
	unpinned := &Group{rules: g.rules, lifecycle: "constructed"}
	if err = unpinned.SetServeOverride(context.Background(), 1); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
}
