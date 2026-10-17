//go:build functional

package metaservice

import (
	"bytes"
	"context"
	"fmt"
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
	f.g.recordScopedApplied(f.g.machine)
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

func TestAppliedRecorderIsLocalAndDeduplicatesUnchangedBlockAndDrain(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.recordScopedApplied(f.g.machine) // Establish the already-observed founding blocks.
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
		f.g.recordScopedApplied(f.g.machine)
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
	if len(got) != 3 || got[2].Type != events.DrainCompleted || got[2].SubjectNode != "n3" || got[2].Version != 2 {
		t.Fatal(got)
	}
	for i, group := range []string{topology.MetaGroupID, "work"} {
		event := got[i]
		if event.Type != events.OperatorChanged || event.GroupID != group || event.Version != 2 ||
			event.OperationID != string(topology.MakeOperationID(2, 1)) || event.Attempt != 1 ||
			event.State != string(topology.OperationSucceeded) {
			t.Fatal("block event lost exact operation scope", event)
		}
	}
	if !reflect.DeepEqual(f.g.lastDrained, map[string]bool{"n3": true}) {
		t.Fatal(f.g.lastDrained)
	}
}

func TestAppliedRecorderTracksOlderWorkBeyondStatusHeaderLimit(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	desired := f.g.machine.Target()
	desired.Group("work").Labels = map[string]string{"pending": "old-approval"}
	f.commit(t, desired, false)
	proposal := f.fixtureApproval(t, f.g.machine.Latest())
	proposal.Plan.Steps = []topology.Step{{Seq: 1, Kind: topology.StepTransferLeader,
		GroupID: "work", NodeName: "n1", ReplicaID: 1}}
	proposal.Plan.Blocks[0].Steps = []int{1}
	pending := f.command(t, proposal).OperationID
	// More than 100 newer operation headers must not hide independent old work.
	for i := range 101 {
		desired = f.g.machine.Target()
		desired.Node("n1").Labels = map[string]string{"revision": fmt.Sprint(i)}
		f.commit(t, desired, true)
	}
	status, err := f.g.machine.ProgressStatus()
	if err != nil || !status.OperationsTruncated {
		t.Fatal("fixture did not exceed the bounded header view", status, err)
	}
	operation, err := f.g.machine.Operation(pending)
	if err != nil || operation.State != topology.OperationPending {
		t.Fatal("independent old work must remain pending", operation, err)
	}
	f.g.recordScopedApplied(f.g.machine)
	bus, journal := eventFixture(t)
	f.g.opts.Events = bus
	f.completeFixtureWork(t, topology.Operation{Status: operation, Plan: proposal.Plan}, "work", f.g.machine.Target())
	f.g.recordScopedApplied(f.g.machine)
	f.g.recordScopedApplied(f.g.machine)
	if err := bus.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := journalEvents(t, journal, events.Filter{Types: []events.Type{events.OperatorChanged}})
	if len(got) != 1 || got[0].OperationID != string(pending) || got[0].GroupID != "work" ||
		got[0].State != string(topology.OperationSucceeded) || got[0].Version != 2 {
		t.Fatal("older scoped completion was lost or repeated", got)
	}
}
