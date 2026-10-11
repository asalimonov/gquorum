package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

// Populate the actual FSM through encoded proposals, including bounded history,
// exact completed executions, a permanent tombstone and a pending meta transfer.
func snapshotMatrix(tb testing.TB, size int) *Machine {
	tb.Helper()
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: size, MaxReplicas: 5}}
	machine := NewMachine(rules, nil)
	index := uint64(0)
	submit := func(proposal any) ApplyResult {
		tb.Helper()
		encoded, err := Encode(proposal)
		if err != nil {
			tb.Fatal(err)
		}
		index++
		result, err := machine.Apply(context.Background(), raftengine.Command{Index: index, Term: 1, Data: encoded})
		if err != nil {
			tb.Fatal(err)
		}
		var applied ApplyResult
		if err := json.Unmarshal(result.Data, &applied); err != nil || !applied.Accepted {
			tb.Fatalf("fixture proposal %T refused: %+v: %v", proposal, applied, err)
		}
		return applied
	}
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	doc := topology.Document{ClusterID: "snapshot-matrix", ChangedBy: "benchmark", Origin: topology.OriginAPI, CreatedAt: at, Placement: topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}}
	meta := topology.Group{ID: topology.MetaGroupID, NextReplicaID: uint64(size + 1)}
	for i := range size {
		name := fmt.Sprintf("node%d", i+1)
		typ := topology.NodeType("worker")
		if i >= 3 {
			typ = "observer"
		}
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: typ, State: topology.NodeActive, Labels: map[string]string{"benchmark.payload": "retained-node-metadata-for-snapshot-size-measurement"}})
		meta.Replicas = append(meta.Replicas, topology.Replica{NodeName: name, ReplicaID: uint64(i + 1), Role: topology.ReplicaVoter})
	}
	doc.Groups = append(doc.Groups, meta)
	for i := range 16 {
		id := fmt.Sprintf("work-%02d", i)
		if i == 15 {
			id = "retire-on-approval"
		}
		group := topology.Group{ID: id, NextReplicaID: 4}
		for j := range 3 {
			group.Replicas = append(group.Replicas, topology.Replica{NodeName: fmt.Sprintf("node%d", j+1), ReplicaID: uint64(j + 1), Role: topology.ReplicaVoter})
		}
		doc.Groups = append(doc.Groups, group)
	}
	for version := uint64(1); version <= historyRetention+3; version++ {
		active := machine.Active()
		doc.Version, doc.ParentVersion = version-1, version-1
		doc.Nodes[0].Labels["benchmark.revision"] = fmt.Sprint(version)
		if version == 2 {
			doc.Groups = slices.DeleteFunc(doc.Groups, func(group topology.Group) bool { return group.ID == "retire-on-approval" })
		}
		submit(ProposeDocument{Doc: doc, ExpectedVersion: version - 1, ProposalID: fmt.Sprintf("document-%d", version)})
		desired := machine.Current()
		observed := map[string]topology.GroupStatus{}
		for _, group := range active.Groups {
			status := topology.GroupStatus{GroupID: group.ID, Found: true, LeaderID: group.Replicas[0].ReplicaID}
			for _, replica := range group.Replicas {
				status.ConfState.Voters = append(status.ConfState.Voters, replica.ReplicaID)
			}
			observed[group.ID] = status
		}
		for _, group := range desired.Groups {
			if _, found := observed[group.ID]; !found {
				observed[group.ID] = topology.GroupStatus{GroupID: group.ID}
			}
		}
		preferred := uint64(0)
		if version == historyRetention+3 {
			preferred = 2
		}
		plan, err := topology.BuildPlanPreferred(active, desired, observed, rules, preferred)
		if err != nil {
			tb.Fatal("fixture plan", version, err)
		}
		now := at.Add(time.Duration(index) * time.Second)
		begun := submit(BeginOperatorProposal{Version: version, Plan: plan, Actor: "benchmark", Origin: topology.OriginAPI, At: now, ProposalID: fmt.Sprintf("begin-%d", version)})
		if preferred != 0 {
			if len(plan.Steps) != 1 || plan.Steps[0].Kind != topology.StepTransferLeader {
				tb.Fatal("pending fixture is not an exact meta transfer", plan)
			}
			step := plan.Steps[0]
			submit(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: begun.OperatorID, Version: version, Attempt: 1, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, State: topology.OperatorWaitingForLeader, WaitingReason: "pending benchmark transfer", At: now.Add(time.Second)}})
			break
		}
		if len(plan.Steps) == 0 {
			submit(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: begun.OperatorID, Version: version, Attempt: 1, State: topology.OperatorSucceeded, At: now.Add(time.Second)}})
		} else {
			for i, step := range plan.Steps {
				state := topology.OperatorRunning
				if i == len(plan.Steps)-1 {
					state = topology.OperatorSucceeded
				}
				submit(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: begun.OperatorID, Version: version, Attempt: 1, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, State: state, At: now.Add(time.Duration(i+1) * time.Second)}})
			}
		}
		submit(ActivateTargetProposal{OperatorID: begun.OperatorID, Version: version, Attempt: 1, ProposalID: fmt.Sprintf("activate-%d", version)})
	}
	// Native membership is a separate Raft snapshot field, not serialized in the
	// FSM payload. It is covered by storage/runtime snapshot conformance tests.
	return machine
}

func TestMetaFullSnapshotMatrixPreservesHistoryOperatorsAndRetirement(t *testing.T) {
	t.Parallel()
	for _, size := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			machine := snapshotMatrix(t, size)
			start := time.Now()
			snapshot, err := machine.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot) == 0 || len(snapshot) > 4<<20 {
				t.Fatal("full meta snapshot exceeds four MiB", len(snapshot))
			}
			var state smState
			if err := json.Unmarshal(snapshot, &state); err != nil {
				t.Fatal(err)
			}
			if len(state.History) != historyRetention || len(state.Latest.LoadGroups()) != 15 || len(state.Operators) < historyRetention || len(state.RetiredGroups) != 1 || len(state.Allocations) != 17 || state.Active.Version != historyRetention+2 || state.Target.Version != historyRetention+3 || state.Latest.Version != state.Target.Version {
				t.Fatal("matrix lost retained full-state workload", len(state.History), len(state.Operators), len(state.Allocations), state.Active.Version, state.Target.Version)
			}
			pending, found := machine.Operator(topology.MakeOperatorID(state.Target.Version, 1))
			if !found || pending.State != topology.OperatorWaitingForLeader {
				t.Fatal("pending execution missing", pending)
			}
			restored := NewMachine(state.Rules, nil)
			if err := restored.Restore(context.Background(), snapshot); err != nil {
				t.Fatal(err)
			}
			roundtrip, err := restored.Snapshot(context.Background())
			if err != nil || !bytes.Equal(snapshot, roundtrip) {
				t.Fatal("full snapshot restore changed canonical state", err)
			}
			if elapsed := time.Since(start); elapsed >= 2*time.Second {
				t.Fatal("snapshot serialization/validation/restore exceeded two seconds", elapsed)
			}
			next := restored.Current()
			next.Groups = append(next.Groups, topology.Group{ID: "retire-on-approval", NextReplicaID: 4, Replicas: []topology.Replica{{NodeName: "node1", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "node2", ReplicaID: 2, Role: topology.ReplicaVoter}, {NodeName: "node3", ReplicaID: 3, Role: topology.ReplicaVoter}}})
			encoded, err := Encode(ProposeDocument{Doc: next, ExpectedVersion: next.Version})
			if err != nil {
				t.Fatal(err)
			}
			result, err := restored.Apply(context.Background(), raftengine.Command{Index: state.AppliedIndex + 1, Term: 1, Data: encoded})
			var refusal ApplyResult
			if err != nil || json.Unmarshal(result.Data, &refusal) != nil || refusal.Accepted || refusal.Code != proposalGuardrail {
				t.Fatal("restored retirement allowed ID reuse", refusal, err)
			}
			t.Logf("METRIC nodes=%d groups=15 history=%d snapshot_bytes=%d", size, len(state.History), len(snapshot))
		})
	}
}

func BenchmarkMetaSnapshotMatrix(b *testing.B) {
	for _, size := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("%d-nodes", size), func(b *testing.B) {
			machine := snapshotMatrix(b, size)
			snapshot, err := machine.Snapshot(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			b.Run("serialize", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(snapshot)))
				for b.Loop() {
					result, err := machine.Snapshot(context.Background())
					if err != nil || len(result) != len(snapshot) {
						b.Fatal("snapshot changed", err)
					}
				}
				b.ReportMetric(float64(len(snapshot)), "snapshot_bytes")
			})
			b.Run("restore", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(snapshot)))
				for b.Loop() {
					restored := NewMachine(machine.rules, nil)
					if err := restored.Restore(context.Background(), snapshot); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(snapshot)), "snapshot_bytes")
			})
		})
	}
}
