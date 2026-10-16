//go:build functional

package metaservice

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecordersNilBusNoOp(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.recordGroupHealth(topology.GroupHealthy, topology.GroupHealthEvaluation{GroupID: "work", ObservedHealth: topology.ObservedGroupHealth{State: topology.GroupQuorumLost}})
	f.g.recordNodeHealth("n2", topology.HealthAlive, topology.HealthDead)
	f.g.recordLegacyApplied(f.g.machine, f.g.machine.Active())
}

func TestHealthRecorderQuorumCrossingsOnly(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	bus, j := eventFixture(t)
	f.g.opts.Events = bus
	healthy := 2
	transitions := []struct {
		group     string
		old, next topology.GroupHealth
		alive     int
	}{
		{"_meta", topology.GroupHealthy, topology.GroupDegraded, 2},
		{"_meta", topology.GroupDegraded, topology.GroupQuorumLost, 1},
		{"_meta", topology.GroupQuorumLost, topology.GroupHealthy, 3},
		{"_meta", topology.GroupUnknown, topology.GroupHealthy, 3},
		{"work", topology.GroupHealthy, topology.GroupQuorumLost, 0},
	}
	for _, x := range transitions {
		healthy = x.alive
		f.g.recordGroupHealth(x.old, topology.GroupHealthEvaluation{GroupID: x.group, Voters: 3, ObservedHealth: topology.ObservedGroupHealth{State: x.next, HealthyVoters: &healthy}})
	}
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumLost, events.QuorumAchieved}})
	if len(got) != 3 || got[0].Type != events.QuorumLost || got[0].GroupID != "_meta" || *got[0].HealthyVoters != 1 || got[1].Type != events.QuorumAchieved || *got[1].HealthyVoters != 3 || got[2].Type != events.QuorumLost || got[2].GroupID != "work" || *got[2].HealthyVoters != 0 {
		t.Fatal(got)
	}
}

func TestHealthRecorderQuorumRegainedWhileDegraded(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	bus, j := eventFixture(t)
	f.g.opts.Events = bus
	healthy := 2
	f.g.recordGroupHealth(topology.GroupQuorumLost, topology.GroupHealthEvaluation{GroupID: "_meta", Voters: 3, ObservedHealth: topology.ObservedGroupHealth{State: topology.GroupDegraded, HealthyVoters: &healthy}})
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumAchieved}})
	if len(got) != 1 || *got[0].HealthyVoters != 2 {
		t.Fatal(got)
	}
}

func TestHealthRecorderRecordsQuorumLostFromUnknown(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	bus, j := eventFixture(t)
	f.g.opts.Events = bus
	f.g.recordGroupHealth(topology.GroupUnknown, topology.GroupHealthEvaluation{GroupID: "_meta", ObservedHealth: topology.ObservedGroupHealth{State: topology.GroupQuorumLost}})
	f.g.recordGroupHealth(topology.GroupUnknown, topology.GroupHealthEvaluation{GroupID: "work", ObservedHealth: topology.ObservedGroupHealth{State: topology.GroupHealthy}})
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := journalEvents(t, j, events.Filter{Types: []events.Type{events.QuorumLost, events.QuorumAchieved}}); len(got) != 1 || got[0].Type != events.QuorumLost || got[0].GroupID != "_meta" {
		t.Fatal(got)
	}
}

func TestLivenessRecorderObserverAndSubjectIdentities(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.identity = topology.Identity{NodeName: "n1", InstanceID: "process", ClusterID: "cluster", MemberID: 1}
	bus, j := eventFixture(t)
	f.g.opts.Events = bus
	f.g.recordNodeHealth("n2", topology.HealthAlive, topology.HealthDead)
	f.g.recordNodeHealth("n2", topology.HealthDead, topology.HealthAlive)
	f.g.recordNodeHealth("n2", topology.HealthAlive, topology.HealthUnknown)
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := journalEvents(t, j, events.Filter{})
	if len(got) != 2 || got[0].Type != events.NodeDead || got[1].Type != events.NodeAlive {
		t.Fatal(got)
	}
	for _, e := range got {
		if e.SubjectNode != "n2" || e.NodeName != "n1" || e.InstanceID != "process" || e.ClusterID != "cluster" || e.MemberID != 1 {
			t.Fatal(e)
		}
	}
}

func TestAppliedRecorderIsLocalAndDeduplicatesUnchangedOperatorAndDrain(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Node("n3").State = topology.NodeDraining
	doc.Group("work").Replicas = doc.Group("work").Replicas[:2]
	f.commit(t, doc, true)
	bus, j := eventFixture(t)
	f.g.opts.Events = bus
	before, err := f.g.machine.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		f.g.recordLegacyApplied(f.g.machine, f.g.machine.Active())
	}
	after, err := f.g.machine.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("recorder modified consensus state")
	}
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := journalEvents(t, j, events.Filter{})
	if len(got) != 2 || got[1].Type != events.DrainCompleted || got[1].SubjectNode != "n3" || got[1].Version != 2 || got[0].Type != events.OperatorChanged || got[0].Version != 2 || got[0].OperatorID == "" || got[0].Attempt != 1 || got[0].State != string(topology.OperatorSucceeded) {
		t.Fatal(got)
	}
	if !reflect.DeepEqual(f.g.lastDrained, map[string]bool{"n3": true}) {
		t.Fatal(f.g.lastDrained)
	}
}
