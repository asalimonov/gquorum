package meta

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func restartOptions(t *testing.T, g *Group) Options {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	opts := g.opts
	opts.Identity.MemberID = 0
	opts.Network.Listener = listener
	opts.Network.AdvertiseAddress = listener.Addr().String()
	opts.Founders = append([]FoundingMember(nil), opts.Founders...)
	for i := range opts.Founders {
		if opts.Founders[i].Node.Name == opts.Identity.NodeName {
			opts.Founders[i].Address = opts.Network.AdvertiseAddress
		}
	}
	return opts
}

func TestPublicMetaRestartUsesPinReplaysTailAndRefusesChangedRules(t *testing.T) {
	old := readyPublic(t, publicFoundingCluster(t, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc, _ := old.Latest(ctx)
	doc.Nodes[0].Labels = map[string]string{"disk": "retained"}
	if _, err := old.Stage(ctx, doc, 1); err != nil {
		t.Fatal(err)
	}
	if err := old.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// No snapshot was taken (default threshold 512), so Rules must also be
	// checked against materialized state without treating it as a replay hint.
	wrong := restartOptions(t, old)
	wrong.Limits = topology.Limits{MaxNodes: 3, MaxReplicas: 5}
	if g, err := New(wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
		if g != nil {
			g.Stop(ctx)
		}
		t.Fatal("changed persisted Rules", err)
	}
	mismatch := restartOptions(t, old)
	mismatch.Founders = nil
	mismatch.Identity.MemberID = 42
	if g, err := New(mismatch); !errors.Is(err, fault.ErrIdentityMismatch) {
		if g != nil {
			g.Stop(ctx)
		}
		t.Fatal("changed pinned identity", err)
	}
	opts := restartOptions(t, old)
	opts.Founders = nil
	restarted, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Stop(context.Background()) })
	if restarted.currentIdentity().MemberID != old.currentIdentity().MemberID {
		t.Fatal("restart discarded pin")
	}
	if err = restarted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = restarted.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	latest, _ := restarted.Latest(ctx)
	active, _ := restarted.Active(ctx)
	if latest.Version != 2 || active.Version != 1 || latest.Nodes[0].Labels["disk"] != "retained" {
		t.Fatal("restart skipped committed tail", latest, active)
	}
}

func TestPublicMetaConstructorInvalidDoesNotCreateStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "untouched", "meta.bolt")
	if _, err := New(Options{DBPath: path}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatal("invalid constructor touched local storage", err)
	}
}

func TestPublicMetaParentCancellationJoinsWorkers(t *testing.T) {
	old := readyPublic(t, publicFoundingCluster(t, 1))
	if err := old.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	g, err := New(restartOptions(t, old))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(context.Background()) })
	parent, cancel := context.WithCancel(context.Background())
	if err = g.Start(parent); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-g.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("parent cancellation leaked workers")
	}
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err = g.WaitReady(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped runtime appeared ready", err)
	}
}
