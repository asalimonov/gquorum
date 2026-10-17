//go:build !functional

package authoritystore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func collectionFixture() CollectionRecord {
	id := storage.Identity{ClusterID: "cluster", GroupID: "work", NodeName: "n1", ReplicaID: 7}
	action := topology.AuthorizedStep{OperationID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1,
		StepSeq: 4, GroupID: id.GroupID, NodeName: id.NodeName, ReplicaID: id.ReplicaID, Kind: topology.StepCleanupReplicaState}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: id.GroupID, Revision: 3},
		Purpose: topology.AuthorityEffect, OperationID: action.OperationID, Action: &action,
		Resources: []topology.AuthorityResource{resource(id.GroupID, id.NodeName)},
		Targets:   []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: 5, ReplicaID: id.ReplicaID}}}
	return CollectionRecord{Retired: storage.RetiredReplica{Identity: id, Token: "opaque/token", QuarantinedAt: time.Unix(10, 0)},
		MemberID: 5, Grant: grant, Retirement: topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0],
			Action: &action, Target: grant.Targets[0], Outcome: topology.EffectApplied, LocalSequence: 9},
		Sequence: 12, State: collectionPrepared}
}

func TestCollectionKeyHasUnambiguousExactIdentity(t *testing.T) {
	t.Parallel()
	id := storage.Identity{ClusterID: "cluster", GroupID: "a", NodeName: "bc", ReplicaID: 7}
	key, err := collectionKey(id)
	if err != nil || hex.EncodeToString(key) != "00000001610000000262630000000000000007" {
		t.Fatalf("collection key %x: %v", key, err)
	}
	other := id
	other.GroupID, other.NodeName = "ab", "c"
	second, err := collectionKey(other)
	if err != nil || bytes.Equal(key, second) {
		t.Fatal("key components are ambiguous", err)
	}
	other = id
	other.ReplicaID++
	second, err = collectionKey(other)
	if err != nil || bytes.Equal(key, second) {
		t.Fatal("successor identity shares the former key", err)
	}
	id.ReplicaID = 0
	if _, err := collectionKey(id); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("incomplete identity accepted", err)
	}
}

func TestCollectionRecordRequiresRetirementProof(t *testing.T) {
	t.Parallel()
	pin := topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 6}
	original := collectionFixture()
	if err := original.Validate(pin); err != nil {
		t.Fatal("former member proof lost after rebind", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*CollectionRecord)
	}{
		{"foreign cluster", func(r *CollectionRecord) { r.Retired.Identity.ClusterID = "foreign" }},
		{"foreign host", func(r *CollectionRecord) { r.Retired.Identity.NodeName = "other" }},
		{"future member", func(r *CollectionRecord) { r.MemberID = 7 }},
		{"missing member", func(r *CollectionRecord) { r.MemberID = 0 }},
		{"missing sequence", func(r *CollectionRecord) { r.Sequence = 0 }},
		{"empty token", func(r *CollectionRecord) { r.Retired.Token = " " }},
		{"NUL token", func(r *CollectionRecord) { r.Retired.Token = "bad\x00token" }},
		{"missing quarantine time", func(r *CollectionRecord) { r.Retired.QuarantinedAt = time.Time{} }},
		{"another replica", func(r *CollectionRecord) { r.Retirement.Target.ReplicaID++ }},
		{"another member", func(r *CollectionRecord) { r.Retirement.Target.MemberID++ }},
		{"another revision", func(r *CollectionRecord) { r.Retirement.Key.Revision++ }},
		{"incomplete receipt", func(r *CollectionRecord) { r.Retirement.LocalSequence = 0 }},
		{"stop is not retirement", func(r *CollectionRecord) {
			r.Grant.Action.Kind = topology.StepStopReplica
			r.Retirement.Action.Kind = topology.StepStopReplica
		}},
		{"unknown state", func(r *CollectionRecord) { r.State = "lost" }},
		{"prepared with completion", func(r *CollectionRecord) { at := time.Unix(11, 0); r.CollectedAt = &at }},
		{"collected without time", func(r *CollectionRecord) { r.State = collectionCollected }},
		{"collected with zero time", func(r *CollectionRecord) { r.State = collectionCollected; r.CollectedAt = new(time.Time) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := original.Clone()
			tc.change(&r)
			if err := r.Validate(pin); err == nil {
				t.Fatal("contradictory collection evidence accepted")
			}
		})
	}
}

func TestCollectedProofRoundTripAndOwnedValues(t *testing.T) {
	t.Parallel()
	r := collectionFixture()
	at := time.Unix(11, 0)
	r.State, r.CollectedAt = collectionCollected, &at
	pin := topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 5}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CollectionRecord
	if err := decode(data, &decoded); err != nil || decoded.Validate(pin) != nil {
		t.Fatal("collected proof failed round trip", err)
	}
	owned := r.Clone()
	owned.Grant.Targets[0].MemberID++
	owned.Grant.Resources[0].NodeName = "other"
	owned.Grant.Action.StepSeq++
	owned.Retirement.Action.StepSeq++
	*owned.CollectedAt = time.Time{}
	if err := r.Validate(pin); err != nil {
		t.Fatal("clone aliases retained proof", err)
	}
	receipt := CollectionReceipt{Identity: r.Retired.Identity, MemberID: r.MemberID, RetirementKey: r.Grant.Key,
		Token: r.Retired.Token, LocalSequence: r.Sequence, CollectedAt: *r.CollectedAt}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	receipt.RetirementKey.GroupID = "other"
	if err := receipt.Validate(); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("receipt for another group accepted", err)
	}
}

func TestCollectionReadmissionProofRetainsFormerMember(t *testing.T) {
	t.Parallel()
	r := collectionFixture()
	r.Grant.Purpose, r.Grant.Action, r.Grant.OperationID, r.Grant.AdmissionID = topology.AuthorityReadmit, nil, "", "admit"
	r.Retirement.Action, r.Retirement.AdmissionID = nil, "admit"
	if err := r.Validate(topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 6}); err != nil {
		t.Fatal("readmission retirement proof refused", err)
	}
}
