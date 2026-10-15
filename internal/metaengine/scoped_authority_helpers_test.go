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
		action := topology.AuthorizedStep{OperatorID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: 3 + i*3,
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
