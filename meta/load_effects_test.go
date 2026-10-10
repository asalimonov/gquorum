package meta

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// verifiedLoadController models caller-owned state independently of the meta
// process. Its acknowledgements verify exact identities and deduplicate effects.
type verifiedLoadController struct {
	mu          sync.Mutex
	meta        []*Group
	groups      map[string]topology.Group
	hosts       map[string]map[string]uint64
	creates     int
	firstEffect chan struct{}
}

func (c *verifiedLoadController) leader() *Group {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, g := range c.meta {
		if g.IsLeader() {
			return g
		}
	}
	return nil
}
func (c *verifiedLoadController) authorized(ctx context.Context, a topology.AuthorizedStep) (*Group, error) {
	g := c.leader()
	if g == nil {
		return nil, fault.ErrUnavailable
	}
	return g, g.Authorize(ctx, a)
}
func (c *verifiedLoadController) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	if err := ctx.Err(); err != nil {
		return topology.GroupStatus{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	group, found := c.groups[id]
	status := topology.GroupStatus{GroupID: id, Found: found, Commit: 1, HealthKnown: true, LiveVoters: map[uint64]bool{}}
	for _, r := range group.Replicas {
		if r.Role == topology.ReplicaVoter {
			status.ConfState.Voters = append(status.ConfState.Voters, r.ReplicaID)
			if c.hosts[id][r.NodeName] == r.ReplicaID {
				status.LiveVoters[r.ReplicaID] = true
				if status.LeaderID == 0 {
					status.LeaderID = r.ReplicaID
				}
			}
		} else {
			status.ConfState.Learners = append(status.ConfState.Learners, r.ReplicaID)
		}
	}
	if found && status.LeaderID == 0 {
		return status, fault.ErrUnavailable
	}
	return status, nil
}
func (c *verifiedLoadController) CreateGroup(ctx context.Context, a topology.AuthorizedStep) error {
	g, err := c.authorized(ctx, a)
	if err != nil {
		return err
	}
	target := g.state().Target()
	wanted := target.Group(a.GroupID)
	if wanted == nil || a.Kind != topology.StepCreateFoundingGroup || a.ReplicaID != 0 || a.NodeName != "" {
		return fault.ErrIdentityMismatch
	}
	c.mu.Lock()
	existing, exists := c.groups[a.GroupID]
	if exists {
		c.mu.Unlock()
		if !reflect.DeepEqual(existing, *wanted) {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	c.groups[a.GroupID] = *wanted
	c.hosts[a.GroupID] = map[string]uint64{}
	for _, r := range wanted.Replicas {
		c.hosts[a.GroupID][r.NodeName] = r.ReplicaID
	}
	c.creates++
	first := c.firstEffect
	c.firstEffect = nil
	c.mu.Unlock()
	if first != nil {
		close(first)
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}
func (c *verifiedLoadController) PrepareReplica(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *verifiedLoadController) ApplyMembership(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *verifiedLoadController) TransferLeader(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *verifiedLoadController) CleanupReplicaState(context.Context, topology.AuthorizedStep) error {
	return fault.ErrUnsupported
}
func (c *verifiedLoadController) StopReplica(ctx context.Context, a topology.AuthorizedStep) error {
	if _, err := c.authorized(ctx, a); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.Kind != topology.StepStopReplica {
		return fault.ErrExecutionAuthorization
	}
	if existing := c.hosts[a.GroupID][a.NodeName]; existing != 0 && existing != a.ReplicaID {
		return fault.ErrIdentityMismatch
	}
	delete(c.hosts[a.GroupID], a.NodeName)
	return nil
}
func (c *verifiedLoadController) DeleteGroupState(ctx context.Context, a topology.AuthorizedStep) error {
	if _, err := c.authorized(ctx, a); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.Kind != topology.StepDeleteGroupState || a.NodeName != "" || a.ReplicaID != 0 {
		return fault.ErrExecutionAuthorization
	}
	if len(c.hosts[a.GroupID]) > 0 {
		return fault.ErrReplicaNotReady
	}
	delete(c.groups, a.GroupID)
	delete(c.hosts, a.GroupID)
	return nil
}
func loadDocument(t *testing.T, g *Group) topology.Document {
	t.Helper()
	doc := g.state().Latest()
	doc.Origin = topology.OriginOperator
	doc.Groups = append(doc.Groups, topology.Group{ID: "load-one", NextReplicaID: 2, Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1}, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	return doc
}

func TestMetaCallerEffectCompletionLostAcrossLeaderReplacement(t *testing.T) {
	controller := &verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}, firstEffect: make(chan struct{})}
	effect := controller.firstEffect
	groups := publicFoundingClusterWith(t, 3, func(o *Options) { o.LoadGroups = controller })
	leader := readyPublic(t, groups)
	controller.mu.Lock()
	controller.meta = groups
	controller.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := leader.Stage(ctx, loadDocument(t, leader), 1); err != nil {
		t.Fatal(err)
	}
	id, err := leader.Apply(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-effect:
	case <-ctx.Done():
		t.Fatal("load effect was not invoked")
	}
	if err = leader.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	var next *Group
	waitPublic(t, func() bool {
		for _, g := range groups {
			if g != leader && g.IsLeader() {
				next = g
				return true
			}
		}
		return false
	})
	status, err := next.WaitExecution(ctx, id)
	if err != nil || status.State != topology.ExecutionSucceeded {
		t.Fatal(status, err)
	}
	waitPublic(t, func() bool { return next.state().Active().Version == 2 })
	controller.mu.Lock()
	effects := controller.creates
	controller.mu.Unlock()
	if effects != 1 {
		t.Fatal("duplicated caller-owned effect", effects)
	}
	// The old authorization is unusable once the execution completed.
	old := topology.AuthorizedStep{ExecutionID: id, Version: 2, Attempt: 1, StepSeq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "load-one"}
	if err = controller.CreateGroup(ctx, old); !errors.Is(err, fault.ErrExecutionAuthorization) {
		t.Fatal("accepted stale authorized identity", err)
	}
	statusGroup, err := controller.Status(ctx, "load-one")
	if err != nil || !reflect.DeepEqual(statusGroup.ConfState, raftpb.ConfState{Voters: []uint64{1}}) {
		t.Fatal(statusGroup, err)
	}
}
