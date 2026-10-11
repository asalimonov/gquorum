//go:build functional

package managed

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type hostNetwork struct {
	discardTransport
	mu        sync.Mutex
	hosts     map[string]*Host
	machines  map[string]*replicatedMachine
	snapshots atomic.Int32
}

func (n *hostNetwork) ReadReplica(ctx context.Context, query load.ReplicaQuery) (load.ReplicaStatus, error) {
	n.mu.Lock()
	h := n.hosts[query.To.Node.NodeName]
	n.mu.Unlock()
	if h == nil {
		return load.ReplicaStatus{}, fault.ErrUnavailable
	}
	return h.ReadReplica(ctx, query)
}
func (n *hostNetwork) ApplyAction(ctx context.Context, req load.ActionRequest) (load.ActionResult, error) {
	n.mu.Lock()
	h := n.hosts[req.To.Node.NodeName]
	n.mu.Unlock()
	if h == nil {
		return load.ActionResult{}, fault.ErrUnavailable
	}
	return h.ApplyAction(ctx, req)
}

func (n *hostNetwork) SendRaft(ctx context.Context, batch load.RaftBatch) error {
	n.mu.Lock()
	from, to := n.hosts[batch.From.Node.NodeName], n.hosts[batch.To.Node.NodeName]
	n.mu.Unlock()
	if from == nil || to == nil {
		return fault.ErrUnavailable
	}
	if err := to.ReceiveRaft(ctx, batch); err != nil {
		return err
	}
	for _, msg := range batch.Messages {
		if msg.Type == raftpb.MsgSnap {
			n.snapshots.Add(1)
			return from.ReportDelivery(ctx, load.DeliveryReport{From: batch.From, To: batch.To, DeliveryID: batch.DeliveryID, Kind: load.DeliverySnapshotSucceeded})
		}
	}
	return nil
}

type replicatedMachine struct {
	mu       sync.Mutex
	requests map[string]string
}

func (m *replicatedMachine) Apply(_ context.Context, c load.Command) (load.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[c.RequestID] = string(c.Data)
	return load.Result{Code: "ok"}, nil
}
func (m *replicatedMachine) Snapshot(context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Marshal(m.requests)
}
func (m *replicatedMachine) Restore(_ context.Context, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = make(map[string]string)
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, &m.requests)
}
func (m *replicatedMachine) count() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.requests) }

func TestManagedFoundersReplicateAndResumeThroughSnapshotDelivery(t *testing.T) {
	functional(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	network := &hostNetwork{hosts: make(map[string]*Host), machines: make(map[string]*replicatedMachine)}
	identities := make(map[uint64]topology.Identity)
	names := []string{"a", "b", "c"}
	var replicas []topology.Replica
	for i, name := range names {
		id := uint64(i + 1)
		identities[id] = topology.Identity{NodeName: name, ClusterID: "cluster", MemberID: id, InstanceID: "process-" + name}
		replicas = append(replicas, topology.Replica{NodeName: name, ReplicaID: id, Role: topology.ReplicaVoter})
	}
	for i, name := range names {
		id := uint64(i + 1)
		f := newFixtureAt(t, name, id)
		f.setScope("work", id, true, false)
		scope := f.scopes["work"]
		scope.Group.Replicas = replicas
		f.scopes["work"] = scope
		f.cfg.Engine.Timing.ElectionTicks = 50
		f.cfg.Transport = network
		f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
			m := &replicatedMachine{}
			network.mu.Lock()
			network.machines[name] = m
			network.mu.Unlock()
			return load.Application{StateMachine: m}, nil
		}
		f.cfg.ResolvePeer = func(ctx context.Context, ref load.ReplicaRef) (load.Peer, error) {
			identity, found := identities[ref.ReplicaID]
			if !found || ref.GroupID != "work" {
				return load.Peer{}, fault.ErrIdentityMismatch
			}
			return load.Peer{Node: identity, Replica: ref}, ctx.Err()
		}
		f.cfg.ValidatePeer = func(ctx context.Context, peer load.Peer) error {
			if peer.Replica.GroupID != "work" || identities[peer.Replica.ReplicaID] != peer.Node {
				return fault.ErrIdentityMismatch
			}
			return ctx.Err()
		}
		f.cfg.ValidateNode = func(ctx context.Context, identity topology.Identity) error {
			if identities[identity.MemberID] != identity {
				return fault.ErrIdentityMismatch
			}
			return ctx.Err()
		}
		h := f.host(t)
		network.mu.Lock()
		network.hosts[name] = h
		network.mu.Unlock()
	}
	// Starting the first founder completes local replay before the others exist;
	// it must not wait for election and deadlock this fan-out.
	for i, name := range names {
		_, err := network.hosts[name].StartReplica(ctx, load.ReplicaRef{GroupID: "work", ReplicaID: uint64(i + 1)}, StartOperator)
		must(t, err)
	}
	var leader *loadhandle.Handle
	await(t, ctx, func() bool {
		for _, name := range names {
			g, ok := network.hosts[name].Lookup("work")
			if ok && g.Core().IsLeader() {
				leader = g
				return true
			}
		}
		return false
	})
	propose := func(first, last int) {
		t.Helper()
		for i := first; i <= last; i++ {
			result, err := (*load.Group)(leader).Propose(ctx, load.Proposal{RequestID: fmt.Sprint(i), Data: []byte(fmt.Sprint(i))})
			must(t, err)
			if result.Code != "ok" {
				t.Fatal(result)
			}
		}
	}
	allApplied := func(count int) bool {
		network.mu.Lock()
		defer network.mu.Unlock()
		for _, m := range network.machines {
			if m.count() != count {
				return false
			}
		}
		return true
	}
	propose(1, 3)
	await(t, ctx, func() bool { return allApplied(3) })
	var stopped string
	var ref load.ReplicaRef
	for i, name := range names {
		if name != leader.Identity().NodeName {
			stopped, ref = name, load.ReplicaRef{GroupID: "work", ReplicaID: uint64(i + 1)}
			break
		}
	}
	follower := network.hosts[stopped]
	must(t, follower.StopReplica(ctx, ref, StopExplicit))
	propose(4, 8)
	must(t, (*load.Group)(leader).Snapshot(ctx))
	_, err := follower.StartReplica(ctx, ref, StartExplicit)
	must(t, err)
	await(t, ctx, func() bool { return allApplied(8) })
	if network.snapshots.Load() == 0 {
		t.Fatal("compacted gap recovered without exercising managed snapshot delivery")
	}
	propose(9, 9)
	await(t, ctx, func() bool { return allApplied(9) })
}

func await(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}
