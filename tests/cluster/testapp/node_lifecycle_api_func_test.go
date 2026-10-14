//go:build functional

package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

type blockingReadiness struct {
	*lifecycle.DefaultHooks
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (p *blockingReadiness) Readiness(context.Context, lifecycle.ReadinessInput) (lifecycle.ReadinessDecision, error) {
	if p.once.CompareAndSwap(false, true) {
		close(p.entered)
	}
	<-p.release
	return lifecycle.ReadinessDecision{State: lifecycle.Ready}, nil
}

func TestPublicNodeStopJoinsNoncooperativeHooks(t *testing.T) {
	t.Parallel()
	builtin, _ := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
	p := &blockingReadiness{DefaultHooks: builtin, entered: make(chan struct{}), release: make(chan struct{})}
	c := newSingleNodeFixture(t, false, func(_ int, o *quorum.NodeOptions) {
		o.Lifecycle.Hooks = p
		o.CallbackTimeout = 30 * time.Millisecond
	})
	defer close(p.release)
	n := c.nodes[0]
	if err := n.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness hooks not invoked")
	}
	ctx, cancel := context.WithTimeout(c.ctx, 60*time.Millisecond)
	defer cancel()
	if err := n.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("forgot noncooperative hooks", err)
	}
	if n.Status().Ready || n.Status().State != quorum.NodeStopping {
		t.Fatal(n.Status())
	}
	// deferred release runs before the fixture's joined cleanup.
}

func TestPublicNodeCloseErrorKeepsOwnershipUntilExplicitRetry(t *testing.T) {
	t.Parallel()
	var closes atomic.Int32
	want := errors.New("application close retry")
	c := newSingleNodeFixture(t, true, func(_ int, o *quorum.NodeOptions) {
		o.Loads.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
			return load.Application{StateMachine: &apiCounter{}, Close: func(context.Context) error {
				if closes.Add(1) == 1 {
					return want
				}
				return nil
			}}, nil
		}
	})
	c.startAll(t)
	n := c.nodes[0]
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	result, err := n.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "owned"}, quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := n.Cluster().WaitOperator(ctx, result.OperatorID)
	if err != nil || status.State != topology.OperatorSucceeded {
		t.Fatal(status, err)
	}
	if err = n.Stop(ctx); !errors.Is(err, want) {
		t.Fatal("close failure lost", err)
	}
	if n.Status().State != quorum.NodeStopping || closes.Load() != 1 || len(n.Status().Loads) != 1 {
		t.Fatal("incomplete release forgotten", n.Status(), closes.Load())
	}
	for _, path := range []string{filepath.Join(c.options[0].StorageDir, "_meta.bolt"), filepath.Join(c.options[0].StorageDir, "_authority.bolt")} {
		if db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 30 * time.Millisecond}); !errors.Is(err, bolt.ErrTimeout) {
			if db != nil {
				_ = db.Close()
			}
			t.Fatal("owned database closed ahead of incomplete application release", path, err)
		}
	}
	if err = n.Start(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("failed release reused node", err)
	}
	if err = n.Stop(ctx); err != nil {
		t.Fatal("serial close retry failed", err)
	}
	if n.Status().State != quorum.NodeStopped || closes.Load() != 2 || len(n.Status().Loads) != 0 {
		t.Fatal("retry did not release original ownership", n.Status(), closes.Load())
	}
	for _, path := range []string{filepath.Join(c.options[0].StorageDir, "_meta.bolt"), filepath.Join(c.options[0].StorageDir, "_authority.bolt")} {
		db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 30 * time.Millisecond})
		if err != nil {
			t.Fatal("completed Stop retained owned database", path, err)
		}
		_ = db.Close()
	}
}

func TestPublicNodeCanceledFactoryRemainsOwnedThroughLateReturn(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var closed atomic.Int32
	c := newSingleNodeFixture(t, true, func(_ int, o *quorum.NodeOptions) {
		o.Loads.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
			entered <- struct{}{}
			<-release
			return load.Application{StateMachine: &apiCounter{}, Close: func(context.Context) error { closed.Add(1); return nil }}, nil
		}
	})
	var releaseOnce sync.Once
	releaseFactory := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFactory()
	c.startAll(t)
	n := c.nodes[0]
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	if _, err := n.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "slow"}, quorum.MutationOptions{Approve: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("factory was not called")
	}
	brief, end := context.WithTimeout(c.ctx, 60*time.Millisecond)
	defer end()
	if err := n.Stop(brief); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("factory ownership lost", err)
	}
	if len(n.Status().Loads) != 1 || closed.Load() != 0 {
		t.Fatal(n.Status(), closed.Load())
	}
	releaseFactory()
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("late application closed %d times", closed.Load())
	}
}

// Exercise the local Start contract with one real participant and two absent
// peers. Multi-participant readiness/founding behavior belongs to the pool.
func TestPublicNodeStartReturnsBeforeQuorumAndRepeatStartKeepsLifetime(t *testing.T) {
	t.Parallel()
	c := newSingleNodeFixture(t, false, func(_ int, o *quorum.NodeOptions) {
		for i := 2; i <= 3; i++ {
			o.Bootstrap.Founders = append(o.Bootstrap.Founders, quorum.FoundingMember{Node: topology.Node{Name: fmt.Sprintf("n%d", i), Type: "worker"}, MemberID: uint64(i), Address: "127.0.0.1:1"})
		}
	})
	started := time.Now()
	if err := c.nodes[0].Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("Start waited for quorum")
	}
	brief, done := context.WithTimeout(c.ctx, 100*time.Millisecond)
	if err := c.nodes[0].WaitReady(brief); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pre-quorum readiness", err)
	}
	done()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.nodes[0].Start(canceled); err != nil {
		t.Fatal("repeat Start replaced lifetime", err)
	}
	if status := c.nodes[0].Status(); status.State != quorum.NodeStarting || !status.Meta.Found || status.Ready {
		t.Fatal("repeat Start stopped accepted participant", status)
	}
}
