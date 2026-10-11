package metaengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type runtimeHandler struct {
	group    atomic.Pointer[raftengine.Group]
	identity uint64
}

func (h *runtimeHandler) Step(ctx context.Context, header *metapb.Header, msg raftpb.Message) error {
	if header.MemberId != msg.From || header.NodeName != fmt.Sprint(msg.From) || header.ClusterId != "cluster-abc" || msg.To != h.identity {
		return fault.ErrIdentityMismatch
	}
	if msg.Type == raftpb.MsgSnap {
		if msg.Snapshot == nil {
			return fault.ErrInvalid
		}
		check := newTopologySM(nil, testRules())
		if err := check.Restore(msg.Snapshot.Data); err != nil {
			return err
		}
	}
	group := h.group.Load()
	if group == nil {
		return fault.ErrUnavailable
	}
	return group.Step(ctx, msg)
}
func (h *runtimeHandler) Call(ctx context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	if method != "ProposeMeta" {
		return metatransport.Reply(fault.ErrUnsupported)
	}
	group := h.group.Load()
	if group == nil {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	result, err := group.Propose(ctx, raftengine.Proposal{Data: data})
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: result.Data}
}

type runtimeNode struct {
	group     *raftengine.Group
	machine   *Machine
	store     *metastore.Store
	transport *metatransport.Transport
	handler   *runtimeHandler
}

func runtimeCluster(t *testing.T, count int) ([]*runtimeNode, []string) {
	t.Helper()
	listeners := make([]net.Listener, count)
	addresses := make([]string, count)
	for i := range count {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		addresses[i] = l.Addr().String()
	}
	nodes := make([]*runtimeNode, count)
	for i := range count {
		id := uint64(i + 1)
		store, err := metastore.Open(filepath.Join(t.TempDir(), "meta.bolt"))
		if err != nil {
			t.Fatal(err)
		}
		machine := NewMachine(testRules(), store)
		handler := &runtimeHandler{identity: id}
		transport, err := metatransport.New(metatransport.Options{Header: &metapb.Header{ClusterId: "cluster-abc", NodeName: fmt.Sprint(id), MemberId: id}, Listener: listeners[i], AllowPlaintext: true, Credential: []byte("runtime-test-credential"), Handler: handler, Resolve: func(_ context.Context, id uint64) (string, error) {
			if id < 1 || id > uint64(count) {
				return "", fault.ErrUnavailable
			}
			return addresses[id-1], nil
		}, SendTimeout: 500 * time.Millisecond, SnapshotTimeout: time.Second, RetryInitial: 10 * time.Millisecond, RetryMax: 100 * time.Millisecond, MaxSnapshotBytes: 2 << 20, OnUnreachable: func(id uint64) {
			if group := handler.group.Load(); group != nil {
				group.ReportUnreachable(id)
			}
		}, OnSnapshot: func(id uint64, s raft.SnapshotStatus) {
			if group := handler.group.Load(); group != nil {
				group.ReportSnapshot(id, s)
			}
		}})
		if err != nil {
			t.Fatal(err)
		}
		peers := make([]raft.Peer, count)
		for j := range count {
			peers[j].ID = uint64(j + 1)
		}
		group, err := raftengine.NewMeta(raftengine.Options{GroupID: topology.MetaGroupID, NodeName: fmt.Sprint(id), ClusterID: "cluster-abc", ReplicaID: id, Mode: raftengine.Founding, Peers: peers, Timing: raftengine.Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 50}, Store: &metastore.Adapter{Store: store}, Machine: machine, Transport: transport, SnapshotThreshold: 512})
		if err != nil {
			t.Fatal(err)
		}
		handler.group.Store(group)
		nodes[i] = &runtimeNode{group: group, machine: machine, store: store, transport: transport, handler: handler}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := group.Stop(ctx); err != nil {
				t.Error(err)
			}
			if err := transport.Stop(ctx); err != nil {
				t.Error(err)
			}
			store.Close()
		})
	}
	for _, node := range nodes {
		if err := node.transport.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := node.group.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return nodes, addresses
}
func runtimeLeader(t *testing.T, nodes []*runtimeNode) *runtimeNode {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.group.Error() == nil && node.group.IsLeader() {
				return node
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no meta leader")
	return nil
}
func runtimeWait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("meta convergence timed out")
}
func TestMetaRuntimeRealGRPCConsensusAndBboltRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	nodes, _ := runtimeCluster(t, 3)
	leader := runtimeLeader(t, nodes)
	d := baseDoc()
	d.Nodes[0].Name = "1"
	for i := range d.Groups {
		d.Groups[i].Replicas[0].NodeName = "1"
	}
	for i := 2; i <= 3; i++ {
		name := fmt.Sprint(i)
		d.Nodes = append(d.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		if err := d.SetMemberID(name, uint64(i), time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := leader.group.Propose(ctx, raftengine.Proposal{Data: mustEncode(t, ProposeDocument{Doc: d, ExpectedVersion: 0})})
	if err != nil {
		t.Fatal(err)
	}
	var applied ApplyResult
	if err = json.Unmarshal(result.Data, &applied); err != nil || !applied.Accepted {
		t.Fatal(applied, err)
	}
	runtimeWait(t, func() bool {
		for _, node := range nodes {
			if node.machine.Latest().Version != 1 {
				return false
			}
		}
		return true
	})
	at := time.Unix(1234, 0).UTC()
	for _, p := range []any{beginProposal(1, 0, "begin", at), TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: at.Add(time.Second)}}, ActivateTargetProposal{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1}} {
		result, err := leader.group.Propose(ctx, raftengine.Proposal{Data: mustEncode(t, p)})
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(result.Data, &applied); err != nil || !applied.Accepted {
			t.Fatal(applied, err)
		}
	}
	runtimeWait(t, func() bool {
		for _, node := range nodes {
			if node.machine.Active().Version != 1 {
				return false
			}
		}
		return true
	})
	// All live replicas reconstruct from the full snapshot plus committed tail,
	// even though the materialized state/applied marker is farther ahead.
	node := nodes[0]
	want := node.machine.Active()
	runtimeWait(t, func() bool { index, _ := node.store.AppliedIndex(); return index >= node.group.RaftStatus().Applied })
	if err = node.group.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	hs, entries, snapshot, err := node.store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Metadata.Index >= hs.Commit || len(entries) == 0 {
		t.Fatal("test did not exercise committed tail")
	}
	node.handler.group.Store(nil)
	machine := NewMachine(testRules(), node.store)
	restarted, err := raftengine.NewMeta(raftengine.Options{GroupID: topology.MetaGroupID, NodeName: "1", ClusterID: "cluster-abc", ReplicaID: 1, Mode: raftengine.Joining, Timing: raftengine.Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 50}, Store: &metastore.Adapter{Store: node.store}, Machine: machine, Transport: node.transport})
	if err != nil {
		t.Fatal(err)
	}
	node.handler.group.Store(restarted)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := restarted.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	if err = restarted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = restarted.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, machine.Active()) {
		t.Fatal("committed materialized state not reconstructed")
	}
}

func TestMetaRuntimeLeaderReplacement(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	nodes, _ := runtimeCluster(t, 3)
	leader := runtimeLeader(t, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := leader.group.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := leader.transport.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	next := runtimeLeader(t, nodes)
	if next == leader {
		t.Fatal("stopped leader retained")
	}
	result, err := next.group.Propose(ctx, raftengine.Proposal{Data: mustEncode(t, ProposeDocument{Doc: baseDoc()})})
	if err != nil {
		t.Fatal(err)
	}
	var applied ApplyResult
	if err = json.Unmarshal(result.Data, &applied); err != nil || !applied.Accepted {
		t.Fatal(applied, err)
	}
	runtimeWait(t, func() bool {
		for _, node := range nodes {
			if node != leader && node.machine.Latest().Version != 1 {
				return false
			}
		}
		return true
	})
}
