//go:build functional

package metaservice

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
	mu           sync.Mutex
	meta         []*Group
	groups       map[string]topology.Group
	hosts        map[string]map[string]uint64
	creates      int
	firstEffect  chan struct{}
	firstRelease <-chan struct{}
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
	release := c.firstRelease
	c.firstEffect = nil
	c.mu.Unlock()
	if first != nil {
		close(first)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return fault.ErrUnavailable
		}
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
		return fault.ErrOperatorAuthorization
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
		return fault.ErrOperatorAuthorization
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
	status, err := next.WaitOperator(ctx, id)
	if err != nil || status.State != topology.OperatorSucceeded {
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
	old := topology.AuthorizedStep{OperatorID: id, Version: 2, Attempt: 1, StepSeq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "load-one"}
	if err = controller.CreateGroup(ctx, old); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("accepted stale authorized identity", err)
	}
	statusGroup, err := controller.Status(ctx, "load-one")
	if err != nil || !reflect.DeepEqual(statusGroup.ConfState, raftpb.ConfState{Voters: []uint64{1}}) {
		t.Fatal(statusGroup, err)
	}
}

func TestMetaDeletesInterruptedFoundingBeforeActive(t *testing.T) {
	release := make(chan struct{})
	controller := &verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}, firstEffect: make(chan struct{}), firstRelease: release}
	effect := controller.firstEffect
	groups := publicFoundingClusterWith(t, 1, func(o *Options) { o.LoadGroups = controller })
	leader := readyPublic(t, groups)
	controller.mu.Lock()
	controller.meta = groups
	controller.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if _, err := leader.Stage(ctx, loadDocument(t, leader), 1); err != nil {
		t.Fatal(err)
	}
	creating, err := leader.Apply(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-effect:
	case <-ctx.Done():
		t.Fatal("founding effect did not start")
	}
	active := leader.state().Active()
	if active.Group("load-one") != nil {
		t.Fatal("unacknowledged group reached Active")
	}
	desired := leader.state().Latest()
	for i, g := range desired.Groups {
		if g.ID == "load-one" {
			desired.Groups = append(desired.Groups[:i], desired.Groups[i+1:]...)
			break
		}
	}
	if _, err = leader.Stage(ctx, desired, 2); err != nil {
		t.Fatal(err)
	}
	deleting, err := leader.Apply(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	close(release) // The old RPC returns without a durable completion record.
	status, err := leader.WaitOperator(ctx, deleting)
	if err != nil || status.State != topology.OperatorSucceeded {
		t.Fatal(status, err)
	}
	old, err := leader.Operator(ctx, creating)
	if err != nil || old.State != topology.OperatorSuperseded {
		t.Fatal(old, err)
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if _, exists := controller.groups["load-one"]; exists || len(controller.hosts["load-one"]) != 0 {
		t.Fatal("interrupted founding resources were not deleted")
	}
}
