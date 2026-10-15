package metaengine

import (
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
		plan, err := topology.BuildPlanPreferred(testPlanningInput(tb, machine, desired, observed), desired, topology.PlanOptions{Rules: rules, PreferredMetaTransfer: preferred})
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
				transition := topology.OperatorTransition{OperatorID: begun.OperatorID, Version: version, Attempt: 1, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, State: topology.OperatorRunning, At: now.Add(time.Duration(i+1) * time.Second)}
				submit(TransitionOperatorProposal{Transition: transition})
				if step.Kind != topology.StepWaitCaughtUp {
					action := topology.AuthorizedStep{OperatorID: begun.OperatorID, Version: version, Attempt: 1, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, Kind: step.Kind, TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID, ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest}
					grant := submit(BeginEffectProposal{Action: action}).Grant
					for _, receipt := range fixtureEffectReceipts(*grant) {
						submit(RecordEffectProposal{Receipt: receipt})
					}
				}
				if i == len(plan.Steps)-1 {
					transition.State = topology.OperatorSucceeded
					submit(TransitionOperatorProposal{Transition: transition})
				}
			}
		}
		submit(ActivateTargetProposal{OperatorID: begun.OperatorID, Version: version, Attempt: 1, ProposalID: fmt.Sprintf("activate-%d", version)})
	}
	// Native membership is a separate Raft snapshot field, not serialized in the
	// FSM payload. It is covered by storage/runtime snapshot conformance tests.
	return machine
}
