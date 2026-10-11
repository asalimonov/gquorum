//go:build !functional

package metaengine

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestInterruptedLoadCleanupSurvivesHistoryAndSnapshot(t *testing.T) {
	for _, founding := range []bool{false, true} {
		t.Run(fmt.Sprintf("founding=%v", founding), func(t *testing.T) {
			t.Parallel()
			sm := newTopologySM(nil, testRules())
			base := baseDoc()
			base.Version = 1
			if founding {
				base.Groups = base.Groups[:1]
			}
			sm.latest, sm.target, sm.active = base.Clone(), base.Clone(), base.Clone()
			sm.history = []topology.Document{base.Clone()}
			sm.rememberAllocations(base)
			at := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
			index := uint64(0)
			accepted := func(p any) ApplyResult {
				t.Helper()
				index++
				r := apply(t, sm, index, p)
				if !r.Accepted {
					t.Fatalf("%T refused: %s", p, r.Reason)
				}
				return r
			}
			prepared := baseDoc()
			steps := []topology.Step{{Seq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "shard0"}}
			if !founding {
				g := prepared.Group("shard0")
				g.Replicas[0].ReplicaID, g.NextReplicaID = 2, 3
				steps = []topology.Step{
					{Seq: 1, Kind: topology.StepPrepareReplica, GroupID: "shard0", NodeName: "n1", ReplicaID: 2},
					{Seq: 2, Kind: topology.StepAddLearner, GroupID: "shard0", NodeName: "n1", ReplicaID: 2},
				}
			}
			accepted(ProposeDocument{Doc: prepared, ExpectedVersion: 1})
			old := accepted(BeginOperatorProposal{Version: 2, Plan: topology.Plan{Version: 2, Steps: steps}, At: at}).OperatorID
			accepted(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: old, Version: 2, Attempt: 1, StepSeq: 1, State: topology.OperatorRunning, GroupID: "shard0", NodeName: steps[0].NodeName, At: at}})
			// The effect can exist even though its acknowledgement was lost.
			desired := sm.latest.Clone()
			desired.Groups = desired.Groups[:1]
			accepted(ProposeDocument{Doc: desired, ExpectedVersion: 2})
			planning, err := sm.DeletionBase(t.Context(), sm.latest)
			if err != nil {
				t.Fatal(err)
			}
			observed := map[string]topology.GroupStatus{}
			for _, g := range planning.Groups {
				observed[g.ID] = topology.GroupStatus{GroupID: g.ID}
			}
			// _meta is established. The removed load group may have no quorum.
			meta := observed[topology.MetaGroupID]
			meta.Found, meta.LeaderID, meta.ConfState.Voters = true, 1, []uint64{1}
			observed[topology.MetaGroupID] = meta
			plan, err := topology.BuildPlan(planning, sm.latest, observed, testRules())
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Steps) != len(planning.Group("shard0").Replicas)+1 {
				t.Fatal(plan)
			}
			index++
			omitted := apply(t, sm, index, BeginOperatorProposal{Version: 3, Plan: topology.Plan{Version: 3}, At: at.Add(time.Second)})
			if omitted.Accepted {
				t.Fatal("approval silently omitted an authorized group's cleanup")
			}
			deletion := accepted(BeginOperatorProposal{Version: 3, Plan: plan, At: at.Add(time.Second)}).OperatorID
			for sm.latest.Version < historyRetention+20 {
				accepted(ProposeDocument{Doc: sm.latest, ExpectedVersion: sm.latest.Version})
			}
			if len(sm.history) != historyRetention {
				t.Fatal("history bound changed", len(sm.history))
			}
			if sm.operators[old] == nil {
				t.Fatal("unacknowledged preparation proof was pruned")
			}
			_, retained := sm.Get(2)
			if retained != founding {
				t.Fatal("only founding requires a pinned document", retained)
			}
			for _, step := range plan.Steps {
				accepted(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: deletion, Version: 3, Attempt: 1, StepSeq: step.Seq, State: topology.OperatorRunning, GroupID: step.GroupID, NodeName: step.NodeName, At: at.Add(time.Duration(step.Seq+1) * time.Second)}})
			}
			snapshot, err := sm.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			restored := newTopologySM(nil, testRules())
			if err := restored.Restore(snapshot); err != nil {
				t.Fatal(err)
			}
			a := topology.AuthorizedStep{OperatorID: deletion, Version: 3, Attempt: 1, StepSeq: len(plan.Steps), GroupID: "shard0", Kind: topology.StepDeleteGroupState}
			ids, err := restored.LoadActionTargets(context.Background(), a)
			want := []storage.Identity{{ClusterID: base.ClusterID, NodeName: "n1", GroupID: "shard0", ReplicaID: 1}}
			if !founding {
				want = append(want, storage.Identity{ClusterID: base.ClusterID, NodeName: "n1", GroupID: "shard0", ReplicaID: 2})
			}
			if err != nil || !slices.Equal(ids, want) {
				t.Fatal("incomplete recovered deletion catalog", ids, err)
			}
			catalog, err := restored.RecoveryCatalog(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var recovered []storage.Identity
			for _, group := range catalog {
				if group.ID == "shard0" {
					for _, replica := range group.Replicas {
						recovered = append(recovered, storage.Identity{ClusterID: base.ClusterID, NodeName: replica.NodeName, GroupID: group.ID, ReplicaID: replica.ReplicaID})
					}
				}
			}
			if !slices.Equal(recovered, want) {
				t.Fatal("readmission lost approved preparation after snapshot/history pruning", recovered)
			}
			if sm.active.Group("shard0") != nil && founding {
				t.Fatal("planning rewrote Active")
			}
			accepted(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: deletion, Version: 3, Attempt: 1, StepSeq: len(plan.Steps), State: topology.OperatorSucceeded, GroupID: "shard0", At: at.Add(time.Hour)}})
			accepted(ActivateTargetProposal{OperatorID: deletion, Version: 3, Attempt: 1})
			// Move materialized views beyond the cleanup, then age out ordinary
			// history. Evidence can now be pruned rather than growing forever.
			current := sm.latest.Version
			next := accepted(BeginOperatorProposal{Version: current, Plan: topology.Plan{Version: current}, At: at.Add(2 * time.Hour)}).OperatorID
			accepted(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: next, Version: current, Attempt: 1, State: topology.OperatorSucceeded, At: at.Add(2 * time.Hour)}})
			accepted(ActivateTargetProposal{OperatorID: next, Version: current, Attempt: 1})
			for n := 0; n < historyRetention+1; n++ {
				accepted(ProposeDocument{Doc: sm.latest, ExpectedVersion: sm.latest.Version})
			}
			if sm.operators[old] != nil || sm.operators[deletion] != nil {
				t.Fatal("completed cleanup evidence was retained forever")
			}
		})
	}
}

func TestCleanupHistoryCapacityPreservesRoomForDeletion(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	base := baseDoc()
	base.Groups = base.Groups[:1]
	base.Version = 1
	sm.active, sm.target = base.Clone(), base.Clone()
	for v := uint64(2); v <= historyRetention; v++ {
		doc := baseDoc()
		doc.Version = v
		doc.Groups[1].ID = fmt.Sprintf("group-%03d", v)
		if err := sm.commitLatestLocked(&doc); err != nil {
			t.Fatal(err)
		}
		op := newOperatorRecord(v, 1, []topology.Step{{Seq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: doc.Groups[1].ID}}, "actor", topology.OriginOperator, time.Now())
		op.State = topology.OperatorSuperseded
		sm.operators[op.ID] = &op
	}
	newPlan := topology.Plan{Version: historyRetention + 1, Steps: []topology.Step{{Seq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "next"}}}
	if err := sm.validateCleanupCapacity(newPlan); err == nil {
		t.Fatal("approval could exceed retained founding proof capacity")
	}
	cleanup := topology.Plan{Version: historyRetention + 1, Steps: []topology.Step{{Seq: 1, Kind: topology.StepDeleteGroupState, GroupID: "group-002"}}}
	if err := sm.validateCleanupCapacity(cleanup); err != nil {
		t.Fatal("cleanup was blocked", err)
	}
	for v := uint64(historyRetention + 1); v <= historyRetention+10; v++ {
		doc := base.Clone()
		doc.Version = v
		if err := sm.commitLatestLocked(&doc); err != nil {
			t.Fatal("cannot stage cleanup", err)
		}
	}
	if len(sm.history) != historyRetention {
		t.Fatal(len(sm.history))
	}
	for v := uint64(2); v <= historyRetention; v++ {
		if _, ok := sm.Get(v); !ok {
			t.Fatal("lost cleanup proof", v)
		}
	}
}

func TestDeletionApprovalRetryAfterCleanup(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	base := baseDoc()
	base.Version = 1
	sm.latest, sm.target, sm.active = base.Clone(), base.Clone(), base.Clone()
	sm.history = []topology.Document{base.Clone()}
	sm.rememberAllocations(base)
	at := time.Now().UTC()
	index := uint64(0)
	accepted := func(p any) ApplyResult {
		t.Helper()
		index++
		r := apply(t, sm, index, p)
		if !r.Accepted {
			t.Fatalf("%T refused: %s", p, r.Reason)
		}
		return r
	}
	doc := base.Clone()
	doc.Groups = doc.Groups[:1]
	accepted(ProposeDocument{Doc: doc, ExpectedVersion: 1})
	plan := topology.Plan{Version: 2, Steps: []topology.Step{
		{Seq: 1, Kind: topology.StepStopReplica, GroupID: "shard0", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: topology.StepDeleteGroupState, GroupID: "shard0"},
	}}
	proposal := BeginOperatorProposal{Version: 2, Plan: plan, At: at}
	id := accepted(proposal).OperatorID
	for _, step := range plan.Steps {
		accepted(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: id, Version: 2, Attempt: 1, State: topology.OperatorRunning, StepSeq: step.Seq, GroupID: step.GroupID, NodeName: step.NodeName, At: at}})
	}
	accepted(TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: id, Version: 2, Attempt: 1, State: topology.OperatorSucceeded, StepSeq: 2, GroupID: "shard0", At: at}})
	if got := accepted(proposal).OperatorID; got != id {
		t.Fatal("cleanup approval retry changed operator", got)
	}
	accepted(ActivateTargetProposal{OperatorID: id, Version: 2, Attempt: 1})
	if got := accepted(proposal).OperatorID; got != id {
		t.Fatal("activated approval retry changed operator", got)
	}
	index++
	proposal.Plan.Steps = nil
	if apply(t, sm, index, proposal).Accepted {
		t.Fatal("retry changed the exact approved plan")
	}
}
