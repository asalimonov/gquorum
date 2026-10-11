package metaservice

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// The application controller records membership only after rechecking the exact
// approved identity. This is a functional callback fixture; actual load Raft
// failures belong to the process-cluster suite.
type repairLoadController struct{ verifiedLoadController }

func (c *repairLoadController) PrepareReplica(ctx context.Context, a topology.AuthorizedStep) error {
	g, err := c.authorized(ctx, a)
	if err != nil {
		return err
	}
	target := g.state().Target()
	group := target.Group(a.GroupID)
	if group == nil || a.Kind != topology.StepPrepareReplica {
		return fault.ErrOperatorAuthorization
	}
	for _, r := range group.Replicas {
		if r.NodeName == a.NodeName && r.ReplicaID == a.ReplicaID {
			c.mu.Lock()
			defer c.mu.Unlock()
			previous := c.hosts[a.GroupID][a.NodeName]
			if previous != 0 && previous != a.ReplicaID {
				return fault.ErrIdentityMismatch
			}
			c.hosts[a.GroupID][a.NodeName] = a.ReplicaID
			return nil
		}
	}
	return fault.ErrIdentityMismatch
}
func (c *repairLoadController) ApplyMembership(ctx context.Context, a topology.AuthorizedStep) error {
	g, err := c.authorized(ctx, a)
	if err != nil {
		return err
	}
	target := g.state().Target()
	desired := target.Group(a.GroupID)
	if desired == nil {
		return fault.ErrIdentityMismatch
	}
	var wanted *topology.Replica
	for _, r := range desired.Replicas {
		if r.NodeName == a.NodeName && r.ReplicaID == a.ReplicaID {
			copy := r
			wanted = &copy
		}
	}
	if wanted == nil || a.Kind != topology.StepAddLearner && a.Kind != topology.StepPromoteVoter {
		return fault.ErrOperatorAuthorization
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hosts[a.GroupID][a.NodeName] != a.ReplicaID {
		return fault.ErrReplicaNotReady
	}
	group := c.groups[a.GroupID]
	for i, r := range group.Replicas {
		if r.ReplicaID == a.ReplicaID {
			if r.NodeName != a.NodeName {
				return fault.ErrIdentityMismatch
			}
			if a.Kind == topology.StepPromoteVoter {
				group.Replicas[i].Role = topology.ReplicaVoter
			}
			c.groups[a.GroupID] = group
			return nil
		}
	}
	if a.Kind != topology.StepAddLearner {
		return fault.ErrReplicaNotReady
	}
	wanted.Role = topology.ReplicaLearner
	group.Replicas = append(group.Replicas, *wanted)
	group.NextReplicaID = desired.NextReplicaID
	c.groups[a.GroupID] = group
	return nil
}
func (c *repairLoadController) Status(ctx context.Context, id string) (topology.GroupStatus, error) {
	s, err := c.verifiedLoadController.Status(ctx, id)
	s.Progress = map[uint64]topology.ReplicaProgress{}
	for _, member := range append(append([]uint64(nil), s.ConfState.Voters...), s.ConfState.Learners...) {
		s.Progress[member] = topology.ReplicaProgress{Match: s.Commit, RecentActive: true}
	}
	return s, err
}

func TestMetaAutomaticRepairApprovesAndRunsExactFreshIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	testMetaReplicaRepair(t, true)
}

func TestMetaOperatorReseatRunsExactFreshIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	testMetaReplicaRepair(t, false)
}

func testMetaReplicaRepair(t *testing.T, automatic bool) {
	bus, err := events.NewBus(events.BusOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := bus.Subscribe(ctx, events.Filter{Types: []events.Type{events.SchedulingChanged}}, 256)
	if err != nil {
		t.Fatal(err)
	}
	controller := &repairLoadController{verifiedLoadController: verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}}}
	groups := publicFoundingClusterWith(t, 3, func(o *Options) {
		o.LoadGroups = controller
		o.Events = bus
		o.Scheduling = SchedulingOptions{Disabled: !automatic, Interval: 100 * time.Millisecond, TickTimeout: 75 * time.Millisecond, StabilityWindow: 50 * time.Millisecond, Cooldown: time.Second}
	})
	leader := readyPublic(t, groups)
	controller.mu.Lock()
	controller.meta = groups
	controller.mu.Unlock()
	created, err := leader.CreateLoadGroup(ctx, GroupSpec{ID: "work"}, MutationOptions{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := leader.WaitOperator(ctx, created.OperatorID)
	if err != nil || completed.State != topology.OperatorSucceeded {
		t.Fatal(completed, err)
	}
	waitPublic(t, func() bool { return leader.state().Active().Version == 2 })
	before := leader.state().Active()
	old := before.Group("work").Replicas[2]
	// The caller reports a replica absent from actual membership after storage
	// loss, while the node itself continues answering _meta probes.
	controller.mu.Lock()
	group := controller.groups["work"]
	group.Replicas = append([]topology.Replica(nil), group.Replicas[:2]...)
	controller.groups["work"] = group
	delete(controller.hosts["work"], old.NodeName)
	controller.mu.Unlock()
	if !automatic {
		result, err := leader.ReseatReplica(ctx, "work", old.NodeName, MutationOptions{Actor: "operator", Comment: "replace lost replica"})
		if err != nil || result.OperatorID == "" {
			t.Fatal(result, err)
		}
		completed, err := leader.WaitOperator(ctx, result.OperatorID)
		if err != nil || completed.State != topology.OperatorSucceeded {
			t.Fatal(completed, err)
		}
	}
	waitPublic(t, func() bool { return leader.state().Active().Version == 3 })
	after := leader.state().Active()
	newReplica := after.Group("work").Replicas[2]
	wantOrigin := topology.OriginReconciler
	if !automatic {
		wantOrigin = topology.OriginOperator
	}
	if after.Origin != wantOrigin || after.ParentVersion != 2 || newReplica.NodeName != old.NodeName || newReplica.ReplicaID == old.ReplicaID || newReplica.Role != topology.ReplicaVoter {
		t.Fatal("wrong automatic repair", after)
	}
	s, err := controller.Status(ctx, "work")
	if err != nil || len(s.ConfState.Voters) != 3 || len(s.ConfState.Learners) != 0 {
		t.Fatal(s, err)
	}
	observed := !automatic
	for !observed {
		select {
		case e := <-stream:
			observed = e.Event.State == string(topology.SchedulingRunning) && e.Event.Code == "missing_replica"
		case <-ctx.Done():
			t.Fatal("repair event missing")
		}
	}
	status, err := leader.Status(ctx)
	if err != nil || status.Scheduling.Enabled != automatic || status.Scheduling.Policy != "restore-required-replica" {
		t.Fatal(status, err)
	}
}

func TestMetaSchedulingConstructorSettingsAndDisabledStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	o := optionsForTest(t)
	o.Scheduling.TickTimeout = -time.Second
	if _, err := New(o); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := os.Stat(o.DBPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid repair settings changed storage", err)
	}
	groups := publicFoundingClusterWith(t, 1, func(o *Options) { o.Scheduling.Disabled = true })
	leader := readyPublic(t, groups)
	s := leader.SchedulingStatus()
	if s.Enabled || s.State != topology.SchedulingDisabled || s.Reason != "config_disabled" {
		t.Fatal(s)
	}
}
