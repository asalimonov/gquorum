//go:build functional

package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

func TestPublicNodeAllFoundersGateKeepsProtocolAvailable(t *testing.T) {
	c := newAPICluster(t, 3, false, func(_ int, o *quorum.NodeOptions) { o.Policies.DefaultLifecycle.Startup = policy.AllFounders })
	for _, n := range c.nodes[:2] {
		if err := n.Start(c.ctx); err != nil {
			t.Fatal(err)
		}
	}
	apiEventually(t, func() bool { return c.nodes[0].OwnSessionLive() && c.nodes[1].OwnSessionLive() })
	ctx, cancel := context.WithTimeout(c.ctx, 50*time.Millisecond)
	if err := c.nodes[0].WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("gate accepted missing founder", err)
	}
	cancel()
	if c.nodes[0].Status().Ready || !c.nodes[0].Status().Meta.Found {
		t.Fatal("gate disabled Raft or completed early", c.nodes[0].Status())
	}
	c.startAll(t)
}

func TestPublicNodeFirstBootstrapTimeoutDoesNotRepeatOnRetainedRestart(t *testing.T) {
	c := newAPICluster(t, 3, false, func(i int, o *quorum.NodeOptions) {
		o.Policies.DefaultLifecycle.Startup = policy.AllFounders
		if i == 0 {
			o.Policies.DefaultLifecycle.FoundersTimeout = 600 * time.Millisecond
		}
	})
	if err := c.nodes[0].Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	if err := c.nodes[0].WaitReady(ctx); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("first bootstrap did not fail", err)
	}
	if err := c.nodes[0].Stop(ctx); err != nil {
		t.Fatal(err)
	}
	opts := c.options[0]
	l, err := net.Listen("tcp", opts.Meta.Network.AdvertiseAddress)
	if err != nil {
		t.Fatal(err)
	}
	opts.Meta.Network.Listener = l
	opts.Identity.InstanceID += "-restart"
	n, err := quorum.NewNode(opts)
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	c.nodes[0] = n
	c.network.install(n)
	if err = n.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(c.ctx, 850*time.Millisecond)
	defer cancel2()
	if err = n.WaitReady(ctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("retained store repeated founding timeout", err, n.Status())
	}
	c.startAll(t)
}

type blockingReadiness struct {
	*policy.DefaultLifecycle
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (p *blockingReadiness) Readiness(context.Context, policy.ReadinessInput) (policy.ReadinessDecision, error) {
	if p.once.CompareAndSwap(false, true) {
		close(p.entered)
	}
	<-p.release
	return policy.ReadinessDecision{State: policy.Ready}, nil
}

func TestPublicNodeStopJoinsNoncooperativePolicy(t *testing.T) {
	builtin, _ := policy.NewDefaultLifecycle(policy.DefaultLifecycleOptions{})
	p := &blockingReadiness{DefaultLifecycle: builtin, entered: make(chan struct{}), release: make(chan struct{})}
	c := newAPICluster(t, 1, false, func(_ int, o *quorum.NodeOptions) {
		o.Policies.Lifecycle = p
		o.Policies.CallbackTimeout = 30 * time.Millisecond
	})
	defer close(p.release)
	n := c.nodes[0]
	if err := n.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness policy not invoked")
	}
	ctx, cancel := context.WithTimeout(c.ctx, 60*time.Millisecond)
	defer cancel()
	if err := n.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("forgot noncooperative policy", err)
	}
	if n.Status().Ready || n.Status().State != quorum.NodeStopping {
		t.Fatal(n.Status())
	}
	// deferred release runs before the fixture's joined cleanup.
}

func TestPublicNodeCloseErrorKeepsOwnershipUntilExplicitRetry(t *testing.T) {
	var closes atomic.Int32
	want := errors.New("application close retry")
	c := newAPICluster(t, 1, true, func(_ int, o *quorum.NodeOptions) {
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
	if db, err := bolt.Open(c.options[0].Meta.DBPath, 0o600, &bolt.Options{Timeout: 30 * time.Millisecond}); !errors.Is(err, bolt.ErrTimeout) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatal("meta database closed ahead of incomplete application release", err)
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
	db, err := bolt.Open(c.options[0].Meta.DBPath, 0o600, &bolt.Options{Timeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal("completed Stop retained meta database", err)
	}
	_ = db.Close()
}

func TestPublicNodeCanceledFactoryRemainsOwnedThroughLateReturn(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var closed atomic.Int32
	c := newAPICluster(t, 1, true, func(_ int, o *quorum.NodeOptions) {
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

type removalPolicy struct {
	*policy.DefaultLifecycle
	node      func() quorum.Node
	calls     atomic.Int32
	violation atomic.Bool
}

func (p *removalPolicy) Removal(ctx context.Context, in policy.RemovalInput) (policy.RemovalDecision, error) {
	s := p.node().Status()
	if s.State != quorum.NodeFenced || s.Ready || s.SessionLive || s.Meta.Found {
		p.violation.Store(true)
	}
	for _, r := range s.Loads {
		if r.State == "ready" || r.State == "starting" {
			p.violation.Store(true)
		}
	}
	if p.calls.Add(1) == 1 {
		return policy.RemovalDecision{}, errors.New("retry removal response")
	}
	return policy.RemovalDecision{Action: policy.KeepDiagnostics}, nil
}

func TestPublicNodeRemovalFencesBeforeReplaceablePolicy(t *testing.T) {
	builtin, _ := policy.NewDefaultLifecycle(policy.DefaultLifecycleOptions{})
	p := &removalPolicy{DefaultLifecycle: builtin}
	c := newAPICluster(t, 3, true, func(i int, o *quorum.NodeOptions) {
		if i == 2 {
			o.Policies.Lifecycle = p
		}
	})
	p.node = func() quorum.Node { return c.nodes[2] }
	c.startAll(t)
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()
	leader := c.leader(t)
	if leader == c.nodes[2] {
		if err := leader.Cluster().TransferLeader(ctx, topology.MetaGroupID, c.nodes[0].Identity().NodeName); err != nil {
			t.Fatal(err)
		}
		apiEventually(t, func() bool { return !c.nodes[2].Cluster().IsLeader() })
		leader = c.leader(t)
	}
	result, err := leader.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "drained"}, quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if s, e := leader.Cluster().WaitOperator(ctx, result.OperatorID); e != nil || s.State != topology.OperatorSucceeded {
		t.Fatal(s, e)
	}
	apiEventually(t, func() bool {
		d, e := leader.Cluster().Active(ctx)
		return e == nil && d.Version == result.Document.Version
	})
	result, err = leader.Cluster().DrainNode(ctx, "n3", quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if s, e := leader.Cluster().WaitOperator(ctx, result.OperatorID); e != nil || s.State != topology.OperatorSucceeded {
		t.Fatal(s, e)
	}
	apiEventually(t, func() bool {
		d, e := leader.Cluster().Active(ctx)
		return e == nil && d.Version == result.Document.Version
	})
	result, err = leader.Cluster().ExcludeNode(ctx, "n3", quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if s, e := leader.Cluster().WaitOperator(ctx, result.OperatorID); e != nil || s.State != topology.OperatorSucceeded {
		t.Fatal(s, e)
	}
	apiEventually(t, func() bool { return p.calls.Load() == 2 })
	if p.violation.Load() {
		t.Fatal("removal policy ran before mandatory fence")
	}
	if s, e := c.nodes[2].Cluster().Status(ctx); e != nil || s.SelfRemoval == nil || !s.SelfRemoval.Fenced {
		t.Fatal("diagnostics lost", s, e)
	}
	if _, e := c.nodes[2].ActivateSession(ctx); !errors.Is(e, fault.ErrStopped) {
		t.Fatal("removed node reactivated", e)
	}
}
