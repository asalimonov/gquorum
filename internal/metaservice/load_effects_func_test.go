//go:build functional

package metaservice

import (
	"context"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// verifiedLoadController models caller-owned state independently of the meta
// process. Its acknowledgements verify exact identities and deduplicate effects.
type verifiedLoadController struct {
	mu           sync.Mutex
	meta         []*Group
	groups       map[string]topology.Group
	grants       map[string]topology.AuthorityGrant
	deleted      chan struct{}
	hosts        map[string]map[string]uint64
	creates      int
	firstEffect  chan struct{}
	firstRelease <-chan struct{}
	stopRelease  <-chan struct{}
	stopStarted  chan struct{}
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
func (c *verifiedLoadController) authorized(ctx context.Context, grant topology.AuthorityGrant) (*Group, error) {
	g := c.leader()
	if g == nil {
		return nil, fault.ErrUnavailable
	}
	return g, g.ValidateGrant(ctx, grant)
}
func (c *verifiedLoadController) ReadNative(ctx context.Context, id string, _ ...storage.Identity) (topology.NativeState, error) {
	status, err := c.Status(ctx, id)
	if err == nil && !status.Found {
		err = fault.ErrUnavailable
	}
	native := topology.NativeState{GroupID: id, ReplicaID: 1, Applied: 1, ConfState: status.ConfState}
	c.mu.Lock()
	if grant, found := c.grants[id]; found {
		owned := grant.Clone()
		native.Grant = &owned
	}
	c.mu.Unlock()
	return native, err
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
func (c *verifiedLoadController) CreateGroup(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	a := *grant.Action
	g, err := c.authorized(ctx, grant)
	if err != nil {
		return nil, err
	}
	target := g.state().Target()
	wanted := target.Group(a.GroupID)
	if wanted == nil || a.Kind != topology.StepCreateFoundingGroup || a.ReplicaID != 0 || a.NodeName != "" {
		return nil, fault.ErrIdentityMismatch
	}
	c.mu.Lock()
	existing, exists := c.groups[a.GroupID]
	if exists {
		c.mu.Unlock()
		if !reflect.DeepEqual(existing, *wanted) {
			return nil, fault.ErrIdentityMismatch
		}
		return protocolEffectReceipts(grant), nil
	}
	if c.grants == nil {
		c.grants = make(map[string]topology.AuthorityGrant)
	}
	c.grants[a.GroupID] = grant.Clone()
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
			return nil, ctx.Err()
		case <-release:
			return nil, fault.ErrUnavailable
		}
	}
	return protocolEffectReceipts(grant), nil
}

func (c *verifiedLoadController) StopReplica(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	a := *grant.Action
	if _, err := c.authorized(ctx, grant); err != nil {
		return nil, err
	}
	if c.stopRelease != nil {
		select {
		case c.stopStarted <- struct{}{}:
		default:
		}
		select {
		case <-c.stopRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.Kind != topology.StepStopReplica {
		return nil, fault.ErrOperationAuthorization
	}
	if existing := c.hosts[a.GroupID][a.NodeName]; existing != 0 && existing != a.ReplicaID {
		return nil, fault.ErrIdentityMismatch
	}
	delete(c.hosts[a.GroupID], a.NodeName)
	return protocolEffectReceipts(grant), nil
}
func (c *verifiedLoadController) DeleteGroupState(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	a := *grant.Action
	if _, err := c.authorized(ctx, grant); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.Kind != topology.StepDeleteGroupState || a.NodeName != "" || a.ReplicaID != 0 {
		return nil, fault.ErrOperationAuthorization
	}
	if len(c.hosts[a.GroupID]) > 0 {
		return nil, fault.ErrReplicaNotReady
	}
	delete(c.groups, a.GroupID)
	delete(c.grants, a.GroupID)
	if c.deleted != nil {
		close(c.deleted)
		c.deleted = nil
	}
	delete(c.hosts, a.GroupID)
	return protocolEffectReceipts(grant), nil
}

func TestMetaDeletesInterruptedFoundingBeforeConvergence(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	stopRelease := make(chan struct{})
	var releaseOnce, stopOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	defer stopOnce.Do(func() { close(stopRelease) })
	controller := &verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}, firstEffect: make(chan struct{}), firstRelease: release, deleted: make(chan struct{}), stopRelease: stopRelease, stopStarted: make(chan struct{}, 1)}
	effect, deleted := controller.firstEffect, controller.deleted
	groups := singleMetaFixtureWith(t, func(o *Options) { o.LoadGroups = controller; o.InstallLocalFence = controller.installFence })
	leader := readyPublic(t, groups)
	controller.mu.Lock()
	controller.meta = groups
	controller.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if _, err := leader.Stage(ctx, loadDocument(t, leader), 1); err != nil {
		t.Fatal(err)
	}
	creating, err := leader.Approve(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-effect:
	case <-ctx.Done():
		t.Fatal("founding effect did not start")
	}
	active := fixtureConvergedDocument(t, leader.state())
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
	deleting, err := leader.Approve(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	status, err := leader.WaitOperation(ctx, deleting)
	if err != nil || status.State != topology.OperationSucceeded {
		t.Fatal(status, err)
	}
	releaseOnce.Do(func() { close(release) }) // Return the unacknowledged creation only after logical success.
	select {
	case <-controller.stopStarted:
	case <-ctx.Done():
		t.Fatal("deferred host stop was not dispatched", ctx.Err())
	}
	page, err := leader.ReadProgress(ctx, topology.ProgressQuery{GroupID: "load-one"})
	if err != nil || len(page.Cleanup) != 1 || page.Cleanup[0].StoppedAt != nil ||
		page.Cleanup[0].RetiredAt != nil || page.Cleanup[0].CollectedAt != nil {
		t.Fatal("logical completion manufactured physical cleanup proof", page, err)
	}
	stopOnce.Do(func() { close(stopRelease) })
	old, err := leader.Operation(ctx, creating)
	if err != nil || old.State != topology.OperationSuperseded {
		t.Fatal(old, err)
	}
	select {
	case <-deleted: // Physical cleanup is separate from required deletion completion.
	case <-ctx.Done():
		t.Fatal("detached deletion did not finish", ctx.Err())
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if _, exists := controller.groups["load-one"]; exists || len(controller.hosts["load-one"]) != 0 {
		t.Fatal("interrupted founding resources were not deleted")
	}
}

func loadDocument(t *testing.T, g *Group) topology.Document {
	t.Helper()
	doc := g.state().Latest()
	doc.Origin = topology.OriginOperator
	doc.Groups = append(doc.Groups, topology.Group{ID: "load-one", NextReplicaID: 2, Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1}, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	return doc
}

func (c *verifiedLoadController) DispatchEffect(ctx context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	if grant.Action == nil || !slices.Contains(grant.Resources, resource) ||
		resource.Kind == topology.AuthorityHost && !slices.Contains(grant.Targets, target) {
		return topology.EffectReceipt{}, fault.ErrOperationAuthorization
	}
	var receipts []topology.EffectReceipt
	var err error
	switch grant.Action.Kind {
	case topology.StepCreateFoundingGroup:
		receipts, err = c.CreateGroup(ctx, grant)
	case topology.StepStopReplica:
		receipts, err = c.StopReplica(ctx, grant)
	case topology.StepDeleteGroupState:
		receipts, err = c.DeleteGroupState(ctx, grant)
	default:
		return topology.EffectReceipt{}, fault.ErrUnsupported
	}
	for _, receipt := range receipts {
		if receipt.Resource == resource && (resource.Kind == topology.AuthorityGroup || receipt.Target == target) {
			return receipt, err
		}
	}
	if err == nil {
		err = fault.ErrOperationAuthorization
	}
	return topology.EffectReceipt{}, err
}
