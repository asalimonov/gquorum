//go:build functional

package main

import (
	"context"
	"encoding/binary"
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

func newSingleNodeFixture(t *testing.T, loads bool, configure func(int, *quorum.NodeOptions)) *apiCluster {
	t.Helper()
	const size = 1
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
			StorageDir: root,
			Identity:   topology.Identity{NodeName: name, ClusterID: "api-cluster", InstanceID: "process-" + name}, Node: founders[i].Node,
			Bootstrap: quorum.BootstrapOptions{Founders: founders}, Limits: topology.Limits{MaxNodes: max(3, size), MaxReplicas: max(5, size)},
			Meta:   quorum.MetaOptions{SessionTTL: 3 * time.Second, Timing: quorum.MetaTiming{TickInterval: 20 * time.Millisecond, ElectionTicks: 8}, Observation: quorum.ObservationOptions{Interval: 100 * time.Millisecond, Timeout: 60 * time.Millisecond}, Network: quorum.MetaNetworkOptions{Listener: listeners[i], AdvertiseAddress: listeners[i].Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("api-fixture")}},
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

func (n *apiNetwork) InstallFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	target := n.lookup(request.To.NodeName)
	if target == nil {
		return load.FenceResult{}, fault.ErrUnavailable
	}
	return target.InstallLoadFence(ctx, request)
}
