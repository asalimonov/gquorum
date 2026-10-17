//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedRecoveryCatalogRetainsCleanupAndExcludesUnapprovedIntent(t *testing.T) {
	t.Parallel()
	s := pendingReplacementCleanup()
	prepareScopedFixture(t, &s)
	desired := s.Latest.Clone()
	added := desired.Groups[0]
	added.ID = "only_staged"
	desired.Groups = append(desired.Groups, added)
	if _, err := s.applyProposeDocument(s.AppliedIndex+1, ProposeDocument{Doc: desired, ExpectedVersion: s.Latest.Version}); err != nil {
		t.Fatal(err)
	}
	before, err := queryScopedRecoveryCatalog(&s)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(before, func(g topology.Group) bool { return g.ID == "only_staged" }) {
		t.Fatal("staging became recovery authority")
	}
	for _, debt := range s.Cleanup {
		found := false
		for _, group := range before {
			if group.ID != debt.Identity.GroupID {
				continue
			}
			found = slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool {
				return r.NodeName == debt.Identity.NodeName && r.ReplicaID == debt.Identity.ReplicaID
			})
		}
		if !found {
			t.Fatal("lost detached exact identity", debt.Identity)
		}
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	after, err := queryScopedRecoveryCatalog(&restored)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("catalog changed after restore", before, after, err)
	}
	before[0].Replicas[0].NodeName = "mutated"
	again, err := queryScopedRecoveryCatalog(&s)
	if err != nil || reflect.DeepEqual(before, again) {
		t.Fatal("catalog aliases durable evidence", again, err)
	}
	delete(s.indexes.documents, s.Operations[0].Status.Version)
	if _, err := queryScopedRecoveryCatalog(&s); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("missing pinned approval accepted", err)
	}
	if rows, err := mustMachine(t, testRules(), nil).RecoveryCatalog(t.Context()); err != nil || len(rows) != 0 {
		t.Fatal("empty recovery catalog should be readable", rows, err)
	}
}

func TestScopedLoadCleanupPreservesClosedProofAndProtectsSuccessor(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	prepareScopedFixture(t, &s)
	debt := s.Cleanup[0]
	record := s.indexes.grants[debt.Retired.Key]
	grant := record.Grant.Clone()
	target := grant.Targets[0]
	record.Closed = true
	if err := queryScopedLoadCleanup(&s, grant, target); err != nil {
		t.Fatal("closed exact retirement lost proof", err)
	}
	successor := target
	successor.ReplicaID = s.Target.Group(grant.Key.GroupID).Replicas[0].ReplicaID
	if err := queryScopedLoadCleanup(&s, grant, successor); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("old grant targeted successor", err)
	}
	s.Target.Group(grant.Key.GroupID).Replicas = append(s.Target.Group(grant.Key.GroupID).Replicas,
		topology.Replica{NodeName: target.NodeName, ReplicaID: target.ReplicaID})
	if err := queryScopedLoadCleanup(&s, grant, target); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("assigned identity may be forgotten", err)
	}
	grant.Key.Revision = s.AppliedIndex + 1
	if err := queryScopedLoadCleanup(&s, grant, target); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("grant ahead of receiver was not retryable", err)
	}
	called := false
	if err := mustMachine(t, testRules(), nil).WithGrantedLoadCleanup(t.Context(), grant, target, func() error { called = true; return nil }); !errors.Is(err, fault.ErrOperationAuthorization) || called {
		t.Fatal("unapproved cleanup identity was admitted", err, called)
	}
}
