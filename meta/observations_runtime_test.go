package meta

import (
	"context"
	"maps"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/topology"
)

type runtimeHealth struct {
	mu     sync.Mutex
	values map[string]topology.Health
}

func (h *runtimeHealth) Snapshot(ctx context.Context) (map[string]topology.Health, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.values), nil
}
func (h *runtimeHealth) set(n string, v topology.Health) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.values[n] = v
}
func waitObservation(t *testing.T, g *Group, state topology.GroupHealth) {
	t.Helper()
	waitPublic(t, func() bool {
		s := g.HealthObservation()
		return s.Version == g.state().Active().Version && len(s.Groups) > 0 && s.Groups[0].ObservedHealth.State == state
	})
}
func eventBarrier(t *testing.T, b *events.Bus, ch <-chan events.Delivery) {
	t.Helper()
	const code = "test_dispatch_barrier"
	if !b.Publish(events.Event{Type: events.RuntimeError, Code: code}) {
		t.Fatal("barrier rejected")
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case d, ok := <-ch:
			if !ok {
				t.Fatal("subscription closed")
			}
			if d.LostBefore != 0 {
				t.Fatal("lost local events", d)
			}
			if d.Event.Code == code {
				return
			}
		case <-timer.C:
			t.Fatal("dispatch barrier timed out")
		}
	}
}
func TestMetaHealthEventsStatusAndDrainUseApprovedActiveScope(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	bus, j := eventFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := bus.Subscribe(ctx, events.Filter{}, 256)
	if err != nil {
		t.Fatal(err)
	}
	health := &runtimeHealth{values: map[string]topology.Health{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive}}
	groups := publicFoundingClusterWith(t, 3, func(o *Options) {
		o.Health = health
		o.Observation = ObservationOptions{Interval: 100 * time.Millisecond, Timeout: 75 * time.Millisecond}
		if o.Identity.NodeName == "n1" {
			o.Events = bus
		}
	})
	leader := readyPublic(t, groups)
	watched := groups[0]
	waitObservation(t, watched, topology.GroupHealthy)
	eventBarrier(t, bus, stream)
	if got := journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumAchieved, events.QuorumLost}}); len(got) != 0 {
		t.Fatal("healthy boot manufactured quorum recovery", got)
	}
	// Raw IDs come from the real Raft runtime, including the local leader.
	for _, g := range groups {
		waitPublic(t, func() bool {
			s, e := g.Status(ctx)
			return e == nil && s.MetaLeader == leader.currentIdentity().MemberID
		})
		s := readStatus(t, g)
		if len(s.Groups) != 1 || !s.Groups[0].Found || s.Groups[0].LeaderID != s.MetaLeader || len(s.Groups[0].ConfState.Voters) != 3 {
			t.Fatal(s)
		}
	}
	waitPublic(t, func() bool {
		return len(journalEvents(t, j, events.Filter{Types: []events.Type{events.LeaderChange}})) > 0
	})
	initial := journalEvents(t, j, events.Filter{Types: []events.Type{events.LeaderChange}})
	if len(initial) == 0 {
		t.Fatal("real election not journaled")
	}
	for _, e := range initial {
		if e.NodeName != "n1" || e.InstanceID != watched.currentIdentity().InstanceID || e.MemberID != 1 || e.ReplicaID != 1 || e.GroupID != "_meta" || e.ClusterID != "cluster" || e.Term == 0 {
			t.Fatal(e)
		}
	}
	health.set("n3", topology.HealthDead)
	waitObservation(t, watched, topology.GroupDegraded)
	health.set("n2", topology.HealthDead)
	waitObservation(t, watched, topology.GroupQuorumLost)
	waitPublic(t, func() bool {
		return len(journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumLost}})) > 0
	})
	eventBarrier(t, bus, stream)
	lost := journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumLost}})
	if len(lost) != 1 || lost[0].Version != 1 || lost[0].HealthyVoters == nil || *lost[0].HealthyVoters != 1 || lost[0].Voters != 3 {
		t.Fatal(lost)
	}
	// Restore one node: degraded still has quorum and must report regained.
	health.set("n2", topology.HealthAlive)
	waitObservation(t, watched, topology.GroupDegraded)
	waitPublic(t, func() bool {
		return len(journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumAchieved}})) > 0
	})
	eventBarrier(t, bus, stream)
	gained := journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumAchieved}})
	if len(gained) != 1 || *gained[0].HealthyVoters != 2 {
		t.Fatal(gained)
	}
	health.set("n3", topology.HealthAlive)
	waitObservation(t, watched, topology.GroupHealthy)
	eventBarrier(t, bus, stream)
	for _, e := range journalEvents(t, j, events.Filter{Types: []events.Type{events.NodeAlive, events.NodeDead, events.GroupHealthChanged, events.QuorumLost, events.QuorumAchieved}}) {
		if e.NodeName != "n1" || e.InstanceID != watched.currentIdentity().InstanceID || e.MemberID != 1 || e.ClusterID != "cluster" || e.Version != 1 || e.At.IsZero() {
			t.Fatal("lost observer or observation scope", e)
		}
	}
	// Sampling changes no replicated document versions.
	if watched.state().Latest().Version != 1 || watched.state().Target().Version != 1 || watched.state().Active().Version != 1 {
		t.Fatal("health churned topology")
	}
	// Choose a follower to drain; it retains its meta vote after convergence.
	var departing *Group
	for _, g := range groups {
		if g != leader {
			departing = g
			break
		}
	}
	name := departing.currentIdentity().NodeName
	r, err := leader.DrainNode(ctx, name, MutationOptions{ExpectedVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitPublic(t, func() bool { return watched.state().Latest().Version == 2 })
	staged := readStatus(t, watched)
	for _, n := range staged.Nodes {
		if n.Node.Name == name && (n.Node.State != topology.NodeDraining || n.EffectiveState != topology.NodeActive) {
			t.Fatal("staged intent looked converged", n)
		}
	}
	ex, err := leader.Apply(ctx, r.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = leader.WaitExecution(ctx, ex); err != nil {
		t.Fatal(err)
	}
	waitPublic(t, func() bool { return watched.state().Active().Version == 2 })
	waitObservation(t, watched, topology.GroupHealthy)
	// Active and observer snapshots become visible before the asynchronous
	// recorder publishes its event. A dispatch barrier only flushes events
	// already published, so wait for the drain event itself first.
	waitPublic(t, func() bool {
		return len(journalEvents(t, j, events.Filter{Types: []events.Type{events.DrainCompleted}})) > 0
	})
	eventBarrier(t, bus, stream)
	drained := journalEvents(t, j, events.Filter{Types: []events.Type{events.DrainCompleted}})
	active := watched.state().Active()
	if len(drained) != 1 || drained[0].SubjectNode != name || drained[0].Version != 2 || active.MemberID(name) != departing.currentIdentity().MemberID {
		t.Fatal(drained)
	}
	executions := journalEvents(t, j, events.Filter{Types: []events.Type{events.ExecutionChanged}})
	found := false
	for _, e := range executions {
		if e.ExecutionID == string(ex) && e.Version == 2 && e.Attempt == 1 && e.State == string(topology.ExecutionSucceeded) {
			found = true
		}
	}
	if !found {
		t.Fatal("missing applied execution result", executions)
	}
	// Transfer is observed directly, without making another topology version.
	target := departing
	if err := target.TransferLeader(ctx, topology.MetaGroupID, name); err == nil {
		t.Fatal("drained node accepted leadership")
	}
	for _, g := range groups {
		if g != leader && g != departing {
			target = g
			break
		}
	}
	if err := leader.TransferLeader(ctx, topology.MetaGroupID, target.currentIdentity().NodeName); err != nil {
		t.Fatal(err)
	}
	waitPublic(t, target.IsLeader)
	waitPublic(t, func() bool {
		for _, e := range journalEvents(t, j, events.Filter{Types: []events.Type{events.LeaderChange}}) {
			if e.NewLeader == strconv.FormatUint(target.currentIdentity().MemberID, 10) {
				return true
			}
		}
		return false
	})
	eventBarrier(t, bus, stream)
	if got := journalEvents(t, j, events.Filter{Types: []events.Type{events.DrainCompleted}}); len(got) != 1 {
		t.Fatal("repeated completed drain", got)
	}
	// Meta Stop owns neither the bus nor its separate journal.
	if err := watched.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	eventBarrier(t, bus, stream)
	if !bus.Publish(events.Event{Type: events.NodeAlive, SubjectNode: "caller"}) {
		t.Fatal("meta closed caller bus")
	}
}
