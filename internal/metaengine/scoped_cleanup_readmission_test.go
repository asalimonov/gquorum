//go:build !functional

package metaengine

import (
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCleanupReadmissionPreservesOriginAndConsumedStop(t *testing.T) {
	t.Parallel()
	s := scopedReadmissionCleanupFixture()
	debt := &s.Cleanup[0]
	if debt.OperationID != "exec-v2-a1" || debt.Stopped.Action == nil || debt.Removed == nil ||
		debt.Retired.AdmissionID != ReadmissionID(s.Admissions[len(s.Admissions)-1]) {
		t.Fatal("fixture must retain old removal/stop and independent new admission")
	}
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal(err)
	}
	// A late receipt for the older exact action remains historical proof;
	// the readmission milestone and the original action/grant stay intact.
	s.AuthorityRecords[0].Effects = fixtureEffectReceipts(s.AuthorityRecords[0].Grant)
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("later readmission discarded earlier action authority", err)
	}
	// A stop not reported before readmission is covered by that batch too.
	debt.Stopped = debt.Retired
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("complete readmission failed to supply the missing stop", err)
	}
}

func readmissionCreatedCleanup() scopedState {
	s := scopedReadmissionCleanupFixture()
	receipt := s.AuthorityRecords[len(s.AuthorityRecords)-1].Effects[0]
	at := s.Cleanup[0].UpdatedAt
	s.Cleanup = append(s.Cleanup, cleanupRecord{
		Identity: storage.Identity{ClusterID: "c", GroupID: topology.MetaGroupID, NodeName: "n", ReplicaID: 1},
		MemberID: 1, Storage: s.History[1].MetaGroup().Storage, OperationID: "exec-v3-a1",
		ReadmissionKey: &receipt.Key, Stopped: &receipt, Retired: &receipt, StoppedAt: &at, RetiredAt: &at,
		CreatedAt: at, UpdatedAt: at,
	})
	return s
}

func TestScopedCleanupReadmissionRequiresBothOriginPaths(t *testing.T) {
	t.Parallel()
	s := readmissionCreatedCleanup()
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("readmission-created debt refused", err)
	}
	cases := map[string]func(*scopedState){
		"dropped original actions": func(s *scopedState) { s.Cleanup[0].StopAction, s.Cleanup[0].RetireAction = nil, nil },
		"lost native proof":        func(s *scopedState) { s.Cleanup[0].Removed, s.Cleanup[0].RemovedAt = nil, nil },
		"missing admission operation": func(s *scopedState) {
			s.Operations = slices.DeleteFunc(s.Operations, func(o topology.Operation) bool { return o.Status.ID == "exec-v3-a1" })
		},
		"actionless wrong origin": func(s *scopedState) { s.Cleanup[2].OperationID = "exec-v2-a1" },
		"wrong readmission key": func(s *scopedState) {
			key := *s.Cleanup[0].ReadmissionKey
			key.GroupID = topology.MetaGroupID
			s.Cleanup[0].ReadmissionKey = &key
		},
		"successor member":      func(s *scopedState) { s.Cleanup[0].MemberID = 2 },
		"wrong retained stop":   func(s *scopedState) { s.Cleanup[0].Stopped = s.Cleanup[1].Stopped },
		"missing batch receipt": func(s *scopedState) { s.AuthorityRecords[len(s.AuthorityRecords)-1].Effects = nil },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := readmissionCreatedCleanup()
			corrupt(&s)
			if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("readmission lost exact origin or batch proof", err)
			}
		})
	}
}
