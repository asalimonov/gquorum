//go:build functional

package metaservice

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type statusHealth map[string]topology.Health

func (s statusHealth) Snapshot(context.Context) (map[string]topology.Health, error) {
	return maps.Clone(s), nil
}
func sampleStatusHealth(t *testing.T, g *Group, h statusHealth) {
	t.Helper()
	g.opts.Health = h
	w, err := observe.New(observe.Config{Source: observationSource{g}})
	if err != nil {
		t.Fatal(err)
	}
	g.observer = w
	t.Cleanup(func() {
		if err := w.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func readStatus(t *testing.T, g *Group) topology.ClusterStatus {
	t.Helper()
	s, err := g.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestClusterStatusWithHealth(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	sampleStatusHealth(t, f.g, statusHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthDead})
	s := readStatus(t, f.g)
	if len(s.Placement) != 1 || s.Placement[0].Version != 1 || s.Placement[0].Source != "approved" ||
		len(s.Placement[0].Groups) != 2 {
		t.Fatal("converged status needs one static placement snapshot", s.Placement)
	}
	if s.Observation.Version != 1 || s.Observation.At.IsZero() || len(s.Observation.Groups) != 2 {
		t.Fatal(s)
	}
	for _, e := range s.Observation.Groups {
		h := e.ObservedHealth
		if e.Voters != 3 || e.Quorum != 2 || h.State != topology.GroupDegraded || h.HealthyVoters == nil || *h.HealthyVoters != 2 || h.FailureTolerance == nil || *h.FailureTolerance != 0 {
			t.Fatal(e)
		}
	}
	if s.Nodes[0].Health != topology.HealthAlive || s.Nodes[2].Health != topology.HealthDead {
		t.Fatal(s.Nodes)
	}
	s.Observation.Nodes["n1"] = topology.HealthDead
	*s.Observation.Groups[0].ObservedHealth.HealthyVoters = 99
	fresh := readStatus(t, f.g)
	if fresh.Nodes[0].Health != topology.HealthAlive || *fresh.Observation.Groups[0].ObservedHealth.HealthyVoters != 2 {
		t.Fatal("aliased observation")
	}
}
func TestClusterStatusHealthUnknown(t *testing.T) {
	t.Parallel()
	for _, sampled := range []bool{false, true} {
		f := newMutationFixture(t)
		if sampled {
			sampleStatusHealth(t, f.g, nil)
		}
		s := readStatus(t, f.g)
		for _, n := range s.Nodes {
			if n.Health != topology.HealthUnknown {
				t.Fatal(n)
			}
		}
		for _, e := range s.Observation.Groups {
			h := e.ObservedHealth
			if h.State != topology.GroupUnknown || h.HealthyVoters != nil || h.FailureTolerance != nil || h.BelowMinReplicas != nil {
				t.Fatal(e)
			}
		}
	}
}
func TestClusterStatusEffectiveState(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	active := f.g.machine.Latest()
	active.Node("n2").State = topology.NodeDraining
	active.Node("n3").State = topology.NodeDraining
	active.Group("work").Replicas = []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}
	f.commit(t, active, true)
	latest := f.g.machine.Latest()
	latest.Nodes = append(latest.Nodes, topology.Node{Name: "n4", Type: "worker", State: topology.NodeActive})
	latest.MetaGroup().Replicas = append(latest.MetaGroup().Replicas, topology.Replica{NodeName: "n4", ReplicaID: 4, Role: topology.ReplicaVoter})
	latest.MetaGroup().NextReplicaID = 5
	f.commit(t, latest, false)
	s := readStatus(t, f.g)
	want := []topology.NodeState{topology.NodeActive, topology.NodeDrained, topology.NodeDrained}
	if len(s.Nodes) != len(want) {
		t.Fatal(s.Nodes)
	}
	for i, n := range s.Nodes {
		if n.EffectiveState != want[i] {
			t.Fatal(n, want[i])
		}
	}
	if s.Nodes[1].Node.State != topology.NodeDraining {
		t.Fatal("lost latest declared intent")
	}
	if s.Nodes[len(s.Nodes)-1].Node.Name == "n4" {
		t.Fatal("staged-only node appeared in effective status", s.Nodes)
	}
}
func TestClusterStatusReportsAllViewVersionsAndUsesActiveGroups(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	target := f.g.machine.Latest()
	target.Node("n3").Labels = map[string]string{topology.LabelReserved: "true"}
	target.Groups = append(target.Groups, topology.Group{ID: "target-work", NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	f.commit(t, target, false)
	f.command(t, metaengine.BeginOperatorProposal{Version: 2, Plan: topology.Plan{Version: 2}, Actor: "test", Origin: topology.OriginFounding, At: time.Now().UTC()})
	latest := f.g.machine.Latest()
	latest.Comment = "staged"
	f.commit(t, latest, false)
	s := readStatus(t, f.g)
	if s.LatestVersion != 3 || s.TargetVersion != 2 || s.ActiveVersion != 1 || len(s.Groups) != 3 || s.Groups[2].GroupID != "work" {
		t.Fatal(s)
	}
	if len(s.Placement) != 2 || s.Placement[0].Version != 1 || s.Placement[1].Version != 2 ||
		len(s.Placement[0].Groups) != 2 || len(s.Placement[1].Groups) != 3 {
		t.Fatal("status omitted captured Active/Target placement", s.Placement)
	}
	if s.Placement[0].Source != "converged" || s.Placement[0].ApprovedVersion != 2 ||
		s.Placement[1].Source != "approved" {
		t.Fatal("placement snapshots lack their source context", s.Placement)
	}
	for _, group := range s.Placement[1].Groups {
		if group.GroupID != "work" {
			continue
		}
		if len(group.Issues) != 0 {
			t.Fatal("grandfathered reservation was reported as group failure", group)
		}
		found := false
		for _, candidate := range group.Candidates {
			if candidate.NodeName == "n3" {
				for _, reason := range candidate.Reasons {
					found = found || reason.Code == "node_reserved"
				}
			}
		}
		if !found {
			t.Fatal("status lost excluded-candidate reason", group)
		}
	}
	s.Placement[0].Groups[0].Candidates[0].NodeName = "tampered"
	if fresh := readStatus(t, f.g); fresh.Placement[0].Groups[0].Candidates[0].NodeName == "tampered" {
		t.Fatal("placement status aliases prior response")
	}
}

// Status must preserve observed IDs and own every nested caller-provided value.
type statusController struct {
	*verifiedLoadController
	value topology.GroupStatus
	err   error
}

func (s *statusController) Status(context.Context, string) (topology.GroupStatus, error) {
	value := s.value.Clone()
	value.Reporter = topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 1, InstanceID: "status-process"}
	value.ReporterReplicaID, value.SourceVersion = 1, 1
	value.ReceivedAt = time.Unix(100, 0).UTC()
	return value, s.err
}
func newObservedStatusFixture(t *testing.T) *mutationFixture {
	t.Helper()
	f := newMutationFixture(t)
	f.command(t, metaengine.GrantSession{SessionID: "status", NodeName: "n1", InstanceID: "status-process", TTLSeconds: 30})
	f.command(t, metaengine.AcquireLock{Key: "node/n1", SessionID: "status"})
	return f
}
func (s *statusController) ReadNative(context.Context, string, ...storage.Identity) (topology.NativeState, error) {
	if s.err == nil && !s.value.Found {
		return topology.NativeState{}, fault.ErrUnavailable
	}
	return topology.NativeState{GroupID: s.value.GroupID, ReplicaID: 1, Applied: 1, ConfState: s.value.ConfState}, s.err
}
func TestClusterStatusReportsLeaders(t *testing.T) {
	t.Parallel()
	f := newObservedStatusFixture(t)
	c := &statusController{value: topology.GroupStatus{GroupID: "work", Found: true, LeaderID: 3, LeaderNodeName: "incorrect-caller-name", ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, Progress: map[uint64]topology.ReplicaProgress{3: {Match: 7}}, LiveVoters: map[uint64]bool{3: true}}}
	f.g.opts.LoadGroups = c
	s := readStatus(t, f.g)
	if s.Groups[1].Observation.LeaderID != 3 || s.Groups[1].Observation.LeaderNodeName != "n3" || c.value.LeaderNodeName != "incorrect-caller-name" {
		t.Fatal(s)
	}
	s.Groups[1].Observation.ConfState.Voters[0] = 99
	s.Groups[1].Observation.Progress[3] = topology.ReplicaProgress{}
	delete(s.Groups[1].Observation.LiveVoters, 3)
	if !reflect.DeepEqual(c.value.ConfState.Voters, []uint64{1, 2, 3}) || c.value.Progress[3].Match != 7 || !c.value.LiveVoters[3] {
		t.Fatal("status aliases caller controller")
	}
}
func TestClusterStatusLeaderUnknown(t *testing.T) {
	t.Parallel()
	f := newObservedStatusFixture(t)
	c := &statusController{value: topology.GroupStatus{GroupID: "work", Found: true, LeaderID: 99, LeaderNodeName: "fabricated"}}
	f.g.opts.LoadGroups = c
	s := readStatus(t, f.g)
	if s.MetaLeaderID != 0 || s.MetaLeaderNodeName != "" || s.Groups[1].Observation.LeaderID != 99 || s.Groups[1].Observation.LeaderNodeName != "" {
		t.Fatal("changed observed identity", s)
	}
	c.value.LeaderID = 0
	if got := readStatus(t, f.g).Groups[1].Observation; got.LeaderID != 0 || got.LeaderNodeName != "" {
		t.Fatal("invented leader during election")
	}
	c.err = fault.ErrUnavailable
	s = readStatus(t, f.g)
	if s.Groups[1].Observation != nil || len(s.Observation.Groups) != 2 {
		t.Fatal(s)
	}
}

func TestClusterStatusLeaderNamesUseActiveIncludingCachedReads(t *testing.T) {
	t.Parallel()
	f := newObservedStatusFixture(t)
	staged := f.g.machine.Latest()
	staged.Group("work").Replicas[2].ReplicaID = 4
	staged.Group("work").NextReplicaID = 5
	f.commit(t, staged, false)
	c := &statusController{value: topology.GroupStatus{GroupID: "work", Found: true, LeaderID: 3}}
	f.g.opts.LoadGroups = c
	if got := readStatus(t, f.g).Groups[1].Observation; got.LeaderNodeName != "n3" || got.LeaderID != 3 {
		t.Fatal("staged replacement hid the active leader", got)
	}
	c.value.LeaderID = 4
	if got := readStatus(t, f.g).Groups[1].Observation; got.LeaderNodeName != "" || got.LeaderID != 4 {
		t.Fatal("staged replacement was reported as an active leader", got)
	}
	c.err = errors.New("cached reads must not call controller")
	cached, _ := c.Status(context.Background(), "work")
	cached.LeaderID, cached.LeaderNodeName = 3, "stale"
	f.g.lastLoadStatus["work"] = cached
	s, err := f.g.CachedStatus(context.Background())
	if err != nil || s.Groups[1].Observation.LeaderNodeName != "n3" {
		t.Fatal(s, err)
	}
	f.commit(t, staged, true)
	s, err = f.g.CachedStatus(context.Background())
	if err != nil || s.Groups[1].Observation != nil {
		t.Fatal("cached leader was attributed to its replacement", s, err)
	}
}
func TestClusterStatusNoRuntimeDeps(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	s := readStatus(t, f.g)
	if s.MetaLeaderID != 0 || len(s.Groups) != 2 || s.Groups[0].Observation != nil || s.Groups[1].Observation != nil {
		t.Fatal(s)
	}
}
func TestClusterStatusNoMetaGroup(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.machine = metaengine.NewMachine(f.g.rules, nil)
	s := readStatus(t, f.g)
	if len(s.Groups) != 0 || len(s.Observation.Groups) != 0 || s.MetaLeaderID != 0 {
		t.Fatal("invented pre-founding groups", s)
	}
}
func TestClusterStatusRejectsWrongGroupAndCanceledCalls(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.LoadGroups = &statusController{value: topology.GroupStatus{GroupID: "other", LeaderID: 1}}
	if s := readStatus(t, f.g); s.Groups[1].Observation != nil {
		t.Fatal(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.g.Status(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestClusterStatusDoesNotMixHealthFromDifferentActiveVersion(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	sampleStatusHealth(t, f.g, statusHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive})
	doc := f.g.machine.Latest()
	doc.Comment = "new active"
	f.commit(t, doc, true)
	s := readStatus(t, f.g)
	if s.Observation.Version != 2 || !s.Observation.At.IsZero() || s.Observation.Groups[0].ObservedHealth.HealthyVoters != nil || s.Nodes[0].Health != topology.HealthUnknown {
		t.Fatal("mixed old health with new Active", s)
	}
}
