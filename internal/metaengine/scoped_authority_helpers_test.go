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
	for i := range 2 {
		kind, storageClass := topology.StepCleanupReplicaState, topology.StorageDefault
		if i == 1 {
			id.GroupID, id.ReplicaID, key.GroupID, key.Revision = "b", 8, "b", 9
			kind, storageClass = topology.StepDeleteGroupState, topology.StorageMemory
		}
		action := topology.AuthorizedStep{OperatorID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: i + 1,
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
		if i == 0 {
			debt.Collection = &authoritystore.CollectionReceipt{Identity: id, MemberID: 1, RetirementKey: key,
				Token: "opaque", LocalSequence: 100, CollectedAt: at}
		}
		state.Cleanup = append(state.Cleanup, debt)
	}
}
