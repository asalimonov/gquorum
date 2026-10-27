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
	"go.etcd.io/raft/v3/raftpb"
)

// Populate the actual FSM through encoded proposals, including bounded history,
// exact completed executions, a permanent tombstone and a pending meta transfer.
func snapshotMatrix(tb testing.TB, size int) *Machine {
	tb.Helper()
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: size, MaxReplicas: 5}}
	machine := mustMachine(tb, rules, nil)
	index := uint64(0)
	seed := raftpb.ConfState{}
	for i := range size {
		seed.Voters = append(seed.Voters, uint64(i+1))
	}
	submit := func(proposal any) ApplyResult {
		tb.Helper()
		encoded, err := Encode(proposal)
		if err != nil {
			tb.Fatal(err)
		}
		index++
		result, err := machine.Apply(context.Background(), raftengine.Command{Index: index, Term: 1, Data: encoded, ConfState: seed})
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
	doc := topology.Document{ClusterID: "snapshot-matrix", ChangedBy: "benchmark", Origin: topology.OriginFounding, CreatedAt: at, Placement: topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}}
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
		doc.Version, doc.ParentVersion = version-1, version-1
		doc.Nodes[0].Labels["benchmark.revision"] = fmt.Sprint(version)
		if version == 2 {
			doc.Groups = slices.DeleteFunc(doc.Groups, func(group topology.Group) bool { return group.ID == "retire-on-approval" })
		}
		submit(ProposeDocument{Doc: doc, ExpectedVersion: version - 1, ProposalID: fmt.Sprintf("document-%d", version)})
		read := snapshotPlanRead(tb, machine, version, seed)
		preferred := uint64(0)
		if version == historyRetention+3 {
			preferred = 2
		}
		plan, err := topology.BuildPlanPreferred(read.Input, read.Target,
			topology.PlanOptions{Rules: rules, PreferredMetaTransfer: preferred})
		if err != nil {
			tb.Fatal("fixture plan", version, err)
		}
		now := at.Add(time.Duration(index) * time.Second)
		begun := submit(BeginOperationProposal{DeletionSemantics: DeletionSemanticsDetached, Version: version, Plan: plan, Scopes: read.Scopes,
			Actor: "benchmark", Origin: topology.OriginAPI, At: now, ProposalID: fmt.Sprintf("begin-%d", version)})
		if preferred != 0 {
			if len(plan.Steps) != 1 || plan.Steps[0].Kind != topology.StepTransferLeader {
				tb.Fatal("pending fixture is not an exact meta transfer", plan)
			}
			step := plan.Steps[0]
			submit(TransitionOperationProposal{Transition: topology.OperationTransition{
				OperationID: begun.OperationID, Version: version, Attempt: 1, StepSeq: step.Seq,
				GroupID: step.GroupID, State: topology.OperationWaitingForLeader,
				Blocked: &topology.BlockedReason{Code: topology.BlockedLeadership, Message: "pending benchmark transfer"},
				At:      now.Add(time.Second)}, ProposalID: fmt.Sprintf("wait-%d", version)})
			break
		}
		for _, block := range plan.Blocks {
			for position, seq := range block.Steps {
				step := plan.Steps[seq-1]
				transition := topology.OperationTransition{OperationID: begun.OperationID,
					Version: version, Attempt: 1, StepSeq: seq, GroupID: block.GroupID,
					State: topology.OperationRunning, At: now.Add(time.Duration(seq) * time.Second)}
				submit(TransitionOperationProposal{Transition: transition, ProposalID: fmt.Sprintf("running-%d-%d", version, seq)})
				if step.Kind != topology.StepWaitCaughtUp {
					action := topology.AuthorizedStep{OperationID: begun.OperationID, Version: version,
						Attempt: 1, StepSeq: seq, GroupID: step.GroupID, NodeName: step.NodeName,
						ReplicaID: step.ReplicaID, Kind: step.Kind, TransferNodeName: step.TransferNodeName,
						TransferReplicaID: step.TransferReplicaID, ExpectedVotersDigest: step.ExpectedVotersDigest,
						ResultVotersDigest: step.ResultVotersDigest}
					grant := submit(BeginEffectProposal{Action: action, ProposalID: fmt.Sprintf("effect-%d-%d", version, seq)}).Grant
					for i, receipt := range fixtureEffectReceipts(*grant) {
						submit(RecordEffectProposal{Receipt: receipt, At: transition.At, ProposalID: fmt.Sprintf("receipt-%d-%d-%d", version, seq, i)})
					}
				}
				if position == len(block.Steps)-1 {
					transition.State = topology.OperationSucceeded
					submit(TransitionOperationProposal{Transition: transition, ProposalID: fmt.Sprintf("complete-%d-%s", version, block.GroupID)})
				}
			}
		}
	}
	// Native membership is a separate Raft snapshot field, not serialized in the
	// FSM payload. It is covered by storage/runtime snapshot conformance tests.
	return machine
}

func snapshotPlanRead(tb testing.TB, machine *Machine, version uint64, seed raftpb.ConfState) PlanRead {
	tb.Helper()
	query := QueryRequest{Kind: "approval_plan", Version: version}
	read := func() PlanRead {
		tb.Helper()
		data, err := json.Marshal(query)
		if err != nil {
			tb.Fatal(err)
		}
		result, err := machine.Query(tb.Context(), data)
		if err != nil {
			tb.Fatal(err)
		}
		var value PlanRead
		if err := json.Unmarshal(result.Data, &value); err != nil {
			tb.Fatal(err)
		}
		return value
	}
	captured := read()
	query.Native = map[string]topology.NativeState{}
	for _, scope := range captured.Scopes {
		group := captured.Input.Groups[scope.GroupID].Converged
		if group == nil && scope.GroupID != topology.MetaGroupID {
			continue
		}
		conf := seed
		if group != nil {
			conf = raftpb.ConfState{}
			for _, replica := range group.Replicas {
				if replica.Role == topology.ReplicaVoter {
					conf.Voters = append(conf.Voters, replica.ReplicaID)
				} else {
					conf.Learners = append(conf.Learners, replica.ReplicaID)
				}
			}
		}
		query.Native[scope.GroupID] = topology.NativeState{GroupID: scope.GroupID, ReplicaID: conf.Voters[0], Applied: 1, ConfState: conf}
	}
	captured = read()
	for id, native := range query.Native {
		row := captured.Input.Groups[id]
		row.Observation = &topology.GroupStatus{GroupID: id, Found: true, LeaderID: native.ReplicaID, ConfState: native.ConfState}
		captured.Input.Groups[id] = row
	}
	return captured
}
