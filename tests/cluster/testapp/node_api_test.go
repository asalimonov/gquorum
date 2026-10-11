package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// This fixture uses only public library imports. Topology orchestration belongs
// to Node; the application supplies its machine and a typed transport adapter.
type apiNetwork struct {
	mu    sync.RWMutex
	nodes map[string]quorum.Node
}

func (n *apiNetwork) lookup(name string) quorum.Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.nodes[name]
}
func (n *apiNetwork) install(node quorum.Node) {
	n.mu.Lock()
	n.nodes[node.Identity().NodeName] = node
	n.mu.Unlock()
}
func (n *apiNetwork) SendRaft(ctx context.Context, batch load.RaftBatch) error {
	target := n.lookup(batch.To.Node.NodeName)
	if target == nil {
		return fault.ErrUnavailable
	}
	if err := target.ReceiveLoadRaft(ctx, batch); err != nil {
		return err
	}
	for _, msg := range batch.Messages {
		if msg.Type == raftpb.MsgSnap {
			return n.lookup(batch.From.Node.NodeName).ReportLoadDelivery(ctx, load.DeliveryReport{From: batch.From, To: batch.To, DeliveryID: batch.DeliveryID, Kind: load.DeliverySnapshotSucceeded})
		}
	}
	return nil
}
func (n *apiNetwork) ReadReplica(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	target := n.lookup(q.To.Node.NodeName)
	if target == nil {
		return load.ReplicaStatus{}, fault.ErrUnavailable
	}
	return target.ReadLoadReplica(ctx, q)
}
func (n *apiNetwork) ApplyAction(ctx context.Context, r load.ActionRequest) (load.ActionResult, error) {
	target := n.lookup(r.To.Node.NodeName)
	if target == nil {
		return load.ActionResult{}, fault.ErrUnavailable
	}
	result, err := target.ApplyLoadAction(ctx, r)
	if err != nil {
		return result, fmt.Errorf("action %s from %+v to %+v: %w", r.Authorization.Kind, r.From, r.To, err)
	}
	return result, nil
}

type apiCounter struct {
	mu    sync.Mutex
	value uint64
}

func (m *apiCounter) Apply(ctx context.Context, c load.Command) (load.Result, error) {
	if err := ctx.Err(); err != nil {
		return load.Result{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(c.Data) != 1 {
		return load.Result{Code: "invalid"}, nil
	}
	m.value += uint64(c.Data[0])
	return load.Result{Data: binary.BigEndian.AppendUint64(nil, m.value)}, nil
}
func (m *apiCounter) Snapshot(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return binary.BigEndian.AppendUint64(nil, m.value), nil
}
func (m *apiCounter) Restore(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(data) == 0 {
		m.value = 0
		return nil
	}
	if len(data) != 8 {
		return fault.ErrUnreadable
	}
	m.value = binary.BigEndian.Uint64(data)
	return nil
}

type apiCluster struct {
	nodes    []quorum.Node
	options  []quorum.NodeOptions
	network  *apiNetwork
	mu       sync.Mutex
	machines map[string]*apiCounter
	ctx      context.Context
	cancel   context.CancelFunc
}

func newAPICluster(t *testing.T, size int, loads bool, configure func(int, *quorum.NodeOptions)) *apiCluster {
	t.Helper()
	if testing.Short() {
		t.Skip("functional public API test; run make test")
	}
	c := &apiCluster{network: &apiNetwork{nodes: map[string]quorum.Node{}}, machines: map[string]*apiCounter{}}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	defer func() { t.Cleanup(func() { c.cancel(); c.stopAll(t) }) }()
	var founders []quorum.FoundingMember
	listeners := make([]net.Listener, size)
	for i := range size {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		t.Cleanup(func() { _ = l.Close() })
		founders = append(founders, quorum.FoundingMember{Node: topology.Node{Name: fmt.Sprintf("n%d", i+1), Type: "worker"}, MemberID: uint64(i + 1), Address: l.Addr().String()})
	}
	for i := range size {
		name := founders[i].Node.Name
		root := t.TempDir()
		opts := quorum.NodeOptions{
			Identity: topology.Identity{NodeName: name, ClusterID: "api-cluster", InstanceID: "process-" + name}, Node: founders[i].Node,
			Bootstrap: quorum.BootstrapOptions{Founders: founders}, Limits: topology.Limits{MaxNodes: max(3, size), MaxReplicas: max(5, size)},
			Meta:   quorum.MetaOptions{DBPath: filepath.Join(root, "meta.bolt"), SessionTTL: 3 * time.Second, Timing: quorum.MetaTiming{TickInterval: 20 * time.Millisecond, ElectionTicks: 8}, Observation: quorum.ObservationOptions{Interval: 100 * time.Millisecond, Timeout: 60 * time.Millisecond}, Network: quorum.MetaNetworkOptions{Listener: listeners[i], AdvertiseAddress: listeners[i].Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("api-fixture")}},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		opts.Policies.Scheduling.Disabled = true
		if loads {
			prewrite := false
			opts.Loads = quorum.LoadOptions{Transport: c.network, Storage: quorum.LoadStorageOptions{WAL: &quorum.WALOptions{RootDir: filepath.Join(root, "loads"), SegmentBytes: 1 << 20, Prewrite: &prewrite}}, Engine: load.EngineOptions{Timing: load.Timing{TickInterval: 20 * time.Millisecond, ElectionTicks: 8}, SnapshotThreshold: 100}, RecoveryInterval: 40 * time.Millisecond}
			opts.Loads.Factory = func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
				m := &apiCounter{}
				c.mu.Lock()
				c.machines[spec.Identity.NodeName+"/"+spec.Identity.GroupID] = m
				c.mu.Unlock()
				return load.Application{StateMachine: m}, nil
			}
		}
		if configure != nil {
			configure(i, &opts)
		}
		n, err := quorum.NewNode(opts)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		c.options = append(c.options, opts)
		c.network.install(n)
	}
	return c
}
func (c *apiCluster) stopAll(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		wg.Go(func() {
			if err := n.Stop(ctx); err != nil {
				t.Errorf("stop %s: %v", n.Identity().NodeName, err)
			}
		})
	}
	wg.Wait()
}
func (c *apiCluster) startAll(t *testing.T) {
	t.Helper()
	for _, n := range c.nodes {
		if err := n.Start(c.ctx); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	for _, n := range c.nodes {
		if err := n.WaitReady(ctx); err != nil {
			for _, other := range c.nodes {
				t.Logf("startup node: %+v", other.Status())
			}
			t.Fatalf("ready %s: %v; %+v", n.Identity().NodeName, err, n.Status())
		}
	}
}
func (c *apiCluster) leader(t *testing.T) quorum.Node {
	t.Helper()
	var leader quorum.Node
	apiEventually(t, func() bool {
		for _, n := range c.nodes {
			if n.Cluster().IsLeader() {
				leader = n
				return true
			}
		}
		return false
	})
	return leader
}
func apiEventually(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("public API condition did not converge")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func (c *apiCluster) value(name, group string) uint64 {
	c.mu.Lock()
	m := c.machines[name+"/"+group]
	c.mu.Unlock()
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.value
}

func TestPublicNodeSequentialStartManagedHoldsAndRecovery(t *testing.T) {
	c := newAPICluster(t, 3, true, nil)
	started := time.Now()
	if err := c.nodes[0].Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("Start waited for quorum")
	}
	brief, cancel := context.WithTimeout(c.ctx, 30*time.Millisecond)
	if err := c.nodes[0].WaitReady(brief); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pre-quorum readiness", err)
	}
	cancel()
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := c.nodes[0].Start(canceled); err != nil {
		t.Fatal("repeat Start replaced lifetime", err)
	}
	c.startAll(t)
	ctx, done := context.WithTimeout(c.ctx, 15*time.Second)
	defer done()
	leader := c.leader(t)
	result, err := leader.Cluster().CreateGroup(ctx, quorum.GroupSpec{ID: "counter"}, quorum.MutationOptions{Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	operator, err := leader.Cluster().WaitOperator(ctx, result.OperatorID)
	if err != nil || operator.State != topology.OperatorSucceeded {
		for _, n := range c.nodes {
			t.Logf("node: %+v", n.Status())
		}
		t.Fatal("create", operator, err)
	}
	var group *load.Group
	apiEventually(t, func() bool {
		for _, n := range c.nodes {
			if g, ok := n.LoadGroup("counter"); ok && g.IsLeader() {
				group = g
				return true
			}
		}
		return false
	})
	if r, err := group.Propose(ctx, load.Proposal{RequestID: "one", Data: []byte{7}}); err != nil || binary.BigEndian.Uint64(r.Data) != 7 {
		t.Fatal(r, err)
	}
	apiEventually(t, func() bool {
		for _, n := range c.nodes {
			if c.value(n.Identity().NodeName, "counter") != 7 {
				return false
			}
		}
		return true
	})
	var held quorum.Node
	for _, n := range c.nodes {
		g, _ := n.LoadGroup("counter")
		if !g.IsLeader() {
			held = n
			break
		}
	}
	old, _ := held.LoadGroup("counter")
	ref := load.ReplicaRef{GroupID: "counter", ReplicaID: old.ReplicaID()}
	if err := held.StopLoadGroup(ctx, ref); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, ok := held.LoadGroup("counter"); ok {
		t.Fatal("supervisor erased explicit hold")
	}
	reopened, err := held.StartLoadGroup(ctx, ref)
	if err != nil || reopened == old || !reopened.Running() {
		t.Fatal("resume exact replica", err)
	}
	if _, err := old.Propose(ctx, load.Proposal{RequestID: "stale", Data: []byte{1}}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("old handle redirected", err)
	}
	if err := group.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	c.stopAll(t)
	for i, opts := range c.options {
		l, err := net.Listen("tcp", opts.Meta.Network.AdvertiseAddress)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		opts.Meta.Network.Listener = l
		opts.Identity.InstanceID += "-restart"
		n, err := quorum.NewNode(opts)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[i] = n
		c.network.install(n)
	}
	c.startAll(t)
	apiEventually(t, func() bool {
		for _, n := range c.nodes {
			g, ok := n.LoadGroup("counter")
			if !ok || !g.Running() || c.value(n.Identity().NodeName, "counter") != 7 {
				return false
			}
		}
		return true
	})
}
