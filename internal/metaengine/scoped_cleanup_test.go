//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCleanupRejectsContradictoryProofAtomically(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"storage downgrade":                  func(s *scopedState) { s.Cleanup[0].Storage = topology.StorageMemory; s.Cleanup[0].Collection = nil },
		"former member":                      func(s *scopedState) { s.Cleanup[0].MemberID++ },
		"successor identity":                 func(s *scopedState) { s.Cleanup[0].Identity.ReplicaID++ },
		"missing stop action":                func(s *scopedState) { s.Cleanup[0].StopAction = nil },
		"foreign operation":                  func(s *scopedState) { s.Cleanup[0].OperationID = "exec-v3-a1" },
		"future action":                      func(s *scopedState) { s.Cleanup[0].StopAction.Version = 4 },
		"invented native removal":            func(s *scopedState) { s.Cleanup[1].RemovedAt = s.Cleanup[0].RemovedAt },
		"lost native removal":                func(s *scopedState) { s.Cleanup[0].Removed, s.Cleanup[0].RemovedAt = nil, nil },
		"lost required stop":                 func(s *scopedState) { s.Cleanup[1].Stopped, s.Cleanup[1].StoppedAt = nil, nil },
		"retirement timestamp without proof": func(s *scopedState) { s.Cleanup[0].Retired = nil },
		"unrecorded receipt":                 func(s *scopedState) { r := s.Cleanup[0].Retired.Clone(); r.LocalSequence++; s.Cleanup[0].Retired = &r },
		"reversed milestones":                func(s *scopedState) { at := s.Cleanup[0].CreatedAt.Add(-time.Second); s.Cleanup[0].StoppedAt = &at },
		"collection without retirement":      func(s *scopedState) { s.Cleanup[0].Retired, s.Cleanup[0].RetiredAt = nil, nil },
		"memory collection":                  func(s *scopedState) { s.Cleanup[1].Collection = s.Cleanup[0].Collection },
		"collection identity":                func(s *scopedState) { s.Cleanup[0].Collection.Identity.ReplicaID++ },
		"collection member":                  func(s *scopedState) { s.Cleanup[0].Collection.MemberID++ },
		"collection key":                     func(s *scopedState) { s.Cleanup[0].Collection.RetirementKey.Revision++ },
		"collection sequence":                func(s *scopedState) { s.Cleanup[0].Collection.LocalSequence = 0 },
		"collection token":                   func(s *scopedState) { s.Cleanup[0].Collection.Token = " " },
		"collection timestamp":               func(s *scopedState) { s.Cleanup[0].Collection.CollectedAt = time.Time{} },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			state := scopedCodecFixture()
			data, _ := json.Marshal(state)
			var prior scopedState
			if err := json.Unmarshal(data, &prior); err != nil {
				t.Fatal(err)
			}
			index := prior.indexes
			corrupt(&state)
			type wire scopedState
			bad, _ := json.Marshal(wire(state))
			if err := json.Unmarshal(bad, &prior); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt cleanup accepted", err)
			}
			after, _ := json.Marshal(prior)
			if !bytes.Equal(data, after) || prior.indexes != index {
				t.Fatal("refusal changed prior state/indexes")
			}
		})
	}
}

func TestScopedCleanupKeepsIndependentCollectionClockAndSequence(t *testing.T) {
	t.Parallel()
	state := scopedCodecFixture()
	if _, err := prepareScopedGraph(&state, state.Rules); err != nil {
		t.Fatal(err)
	}
	debt := state.Cleanup[0]
	if debt.Collection.LocalSequence >= debt.Retired.LocalSequence || !debt.Collection.CollectedAt.Before(*debt.RetiredAt) {
		t.Fatal("fixture must exercise independent bucket sequences and host clock")
	}

}

func TestScopedCleanupRequiresApprovedDeclarationContext(t *testing.T) {
	t.Parallel()
	state := scopedCodecFixture()
	index, err := state.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	staged := state.History[1].Clone()
	staged.Version = state.Latest.Version
	index.documents = map[uint64]topology.Document{staged.Version: staged}
	check := scopedCleanupValidation{state: &state, index: index}
	if check.declaration(state.Cleanup[0]) {
		t.Fatal("unapproved declaration authorized historical storage/member identity")
	}
}

func scopedReadmissionCleanupFixture() scopedState {
	s := scopedCodecFixture()
	admission := AdmissionRecord{NodeName: "n", InstanceID: "replacement", MemberID: 2,
		OperatorID: "exec-v2-a1", State: AdmissionSuperseded}
	s.Admissions = append(s.Admissions, admission)
	for _, group := range []string{"a", topology.MetaGroupID} {
		target := topology.AuthorityTarget{NodeName: "n", MemberID: 1, ReplicaID: 7}
		if group == topology.MetaGroupID {
			target.ReplicaID = 1
		}
		resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: "n"}
		key := topology.AuthorityKey{ClusterID: "c", GroupID: group, Revision: 20}
		grant := topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityReadmit, AdmissionID: ReadmissionID(admission),
			Resources: []topology.AuthorityResource{resource}, Targets: []topology.AuthorityTarget{target}}
		receipt := fixtureEffectReceipts(grant)[0]
		s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
			authorityRecord: authorityRecord{Grant: grant, Effects: []topology.EffectReceipt{receipt}}})
		if group == "a" {
			s.AuthorityFloors[0].Key = key
			debt := &s.Cleanup[0]
			debt.ReadmissionKey, debt.StopAction, debt.RetireAction, debt.Removed, debt.RemovedAt = &key, nil, nil, nil, nil
			debt.Stopped, debt.Retired, debt.Collection = &receipt, &receipt, nil
		} else {
			s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: resource, Key: key})
		}
	}
	// Old settled execution proof is independent; cleanup authority has been
	// replaced by the exact readmission batch rather than relabeled as execution.
	s.AuthorityRecords = s.AuthorityRecords[1:]
	return s
}

func TestScopedCleanupRequiresCompleteReadmissionBatch(t *testing.T) {
	t.Parallel()
	state := scopedReadmissionCleanupFixture()
	if _, err := prepareScopedGraph(&state, state.Rules); err != nil {
		t.Fatal(err)
	}
	state.AuthorityRecords[len(state.AuthorityRecords)-1].Effects = nil
	if _, err := prepareScopedGraph(&state, state.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("partial readmission retired the old owner", err)
	}
}
