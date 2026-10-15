//go:build !functional

package metaengine

import (
	"time"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func populateScopedCodecAuthority(state *scopedState, id storage.Identity, key topology.AuthorityKey, at time.Time) {
	state.Cleanup, state.AuthorityRecords, state.AuthorityFloors = nil, nil, nil
	var prerequisites []scopedAuthorityRecord
	for i := range 2 {
		kind, storageClass := topology.StepCleanupReplicaState, topology.StorageDefault
		if i == 1 {
			id.GroupID, id.ReplicaID, key.GroupID, key.Revision = "b", 8, "b", 9
			kind, storageClass = topology.StepDeleteGroupState, topology.StorageMemory
		}
		action := topology.AuthorizedStep{OperatorID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: 3 + i*2,
			Kind: kind, GroupID: id.GroupID, NodeName: id.NodeName, ReplicaID: id.ReplicaID}
		if kind == topology.StepDeleteGroupState {
			action.NodeName, action.ReplicaID = "", 0
		}
		host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: id.GroupID, NodeName: id.NodeName}
		grant := topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID,
			Action: &action, Resources: []topology.AuthorityResource{host},
			Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: 1, ReplicaID: id.ReplicaID}}}
		receipt := fixtureEffectReceipts(grant)[0]
		state.AuthorityRecords = append(state.AuthorityRecords, scopedAuthorityRecord{Class: "cleanup",
			authorityRecord: authorityRecord{Grant: grant, Effects: []topology.EffectReceipt{receipt}}})
		state.AuthorityFloors = append(state.AuthorityFloors, resourceFloor{Resource: host, Key: key})
		debt := cleanupRecord{Identity: id, Storage: storageClass, MemberID: 1, OperationID: "exec-v2-a1", RetireAction: &action,
			Retired: &receipt, RetiredAt: &at, CreatedAt: at, UpdatedAt: at}
		stop := action
		stop.Kind, stop.StepSeq, stop.NodeName, stop.ReplicaID = topology.StepStopReplica, action.StepSeq-1, id.NodeName, id.ReplicaID
		stopGrant := grant.Clone()
		stopGrant.Key.Revision--
		stopGrant.Action = &stop
		stopped := fixtureEffectReceipts(stopGrant)[0]
		debt.StopAction, debt.Stopped, debt.StoppedAt = &stop, &stopped, &at
		prerequisites = append(prerequisites, scopedAuthorityRecord{Class: "execution",
			authorityRecord: authorityRecord{Grant: stopGrant, Effects: []topology.EffectReceipt{stopped}}})
		if i == 0 {
			debt.Collection = &authoritystore.CollectionReceipt{Identity: id, MemberID: 1, RetirementKey: key,
				Token: "opaque", LocalSequence: 1, CollectedAt: at.Add(-time.Minute)}
			remove := stop
			remove.Kind, remove.StepSeq = topology.StepRemoveReplica, stop.StepSeq-1
			remove.ExpectedVotersDigest = topology.VoterSetDigest([]uint64{7, 8})
			remove.ResultVotersDigest = topology.VoterSetDigest([]uint64{8})
			removeGrant := stopGrant.Clone()
			removeGrant.Key.Revision--
			removeGrant.Action, removeGrant.Resources = &remove, []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: id.GroupID}}
			removed := fixtureEffectReceipts(removeGrant)[0]
			debt.Removed, debt.RemovedAt = &removed, &at
			prerequisites = append(prerequisites, scopedAuthorityRecord{Class: "execution",
				authorityRecord: authorityRecord{Grant: removeGrant, Effects: []topology.EffectReceipt{removed}}})
			state.AuthorityFloors = append(state.AuthorityFloors, resourceFloor{Resource: removeGrant.Resources[0], Key: removeGrant.Key})
		}
		state.Cleanup = append(state.Cleanup, debt)
	}
	state.AuthorityRecords = append(state.AuthorityRecords, prerequisites...)
}

func retainScopedCleanupOrigin(state *scopedState, at time.Time) {
	plan := topology.Plan{Version: 2, Steps: make([]topology.Step, 5),
		VoterBases: []topology.PlanVoterBase{{GroupID: "a", Voters: []uint64{7, 8}}},
		Blocks: []topology.PlanBlock{
			{GroupID: "a", SourceVersion: 1, Steps: []int{1, 2}, Cleanup: []int{3}},
			{GroupID: "b", SourceVersion: 1, Steps: []int{4}, Cleanup: []int{5}},
		}}
	for _, record := range state.AuthorityRecords {
		action := record.Grant.Action
		plan.Steps[action.StepSeq-1] = topology.Step{Seq: action.StepSeq, Kind: action.Kind, GroupID: action.GroupID,
			NodeName: action.NodeName, ReplicaID: action.ReplicaID,
			ExpectedVotersDigest: action.ExpectedVotersDigest, ResultVotersDigest: action.ResultVotersDigest}
	}
	state.Operations = append(state.Operations, topology.Operation{Plan: plan,
		Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2, Attempt: 1,
			State: topology.OperationFailed, TotalBlocks: 2, Blocks: []topology.BlockStatus{
				{GroupID: "a", State: topology.OperationFailed, CurrentStep: 2, TotalSteps: 2},
				{GroupID: "b", State: topology.OperationFailed, CurrentStep: 4, TotalSteps: 1},
			}}, StartedAt: at, UpdatedAt: at})
	state.Groups = append(state.Groups, groupConvergence{GroupID: "b", SourceVersion: 1,
		Declaration: state.History[1].Group("b"), Deleting: true, CompletedAt: at})
}

func retainScopedReadmissionOrigin(s *scopedState) {
	// The later admission is approved at v3; removal debt retains origin v2.
	for _, doc := range []*topology.Document{&s.Target, &s.Latest, &s.History[2], &s.History[3]} {
		doc.MetaGroup().Replicas[0].ReplicaID, doc.MetaGroup().NextReplicaID = 2, 4
	}
	operation := &s.Operations[0]
	transfer := operation.Plan.Steps[0]
	transfer.Seq = 4
	voters, promoted := topology.VoterSetDigest([]uint64{1}), topology.VoterSetDigest([]uint64{1, 2})
	operation.Plan = topology.Plan{Version: 3, Steps: []topology.Step{
		{Seq: 1, GroupID: topology.MetaGroupID, Kind: topology.StepPrepareReplica, NodeName: "n", ReplicaID: 2},
		{Seq: 2, GroupID: topology.MetaGroupID, Kind: topology.StepAddLearner, NodeName: "n", ReplicaID: 2,
			ExpectedVotersDigest: voters, ResultVotersDigest: voters},
		{Seq: 3, GroupID: topology.MetaGroupID, Kind: topology.StepPromoteVoter, NodeName: "n", ReplicaID: 2,
			ExpectedVotersDigest: voters, ResultVotersDigest: promoted}, transfer},
		VoterBases: []topology.PlanVoterBase{{GroupID: topology.MetaGroupID, Voters: []uint64{1}}},
		Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 1, Steps: []int{1, 2, 3}},
			{GroupID: "a", SourceVersion: 1, Steps: []int{4}}}}
	operation.Status.TotalBlocks = 2
	operation.Status.Blocks = []topology.BlockStatus{
		{GroupID: topology.MetaGroupID, State: topology.OperationFailed, CurrentStep: 1, TotalSteps: 3},
		{GroupID: "a", State: topology.OperationFailed, CurrentStep: 4, TotalSteps: 1},
	}
}
