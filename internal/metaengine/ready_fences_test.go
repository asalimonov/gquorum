//go:build !functional

package metaengine

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestReadyFencesSelectCurrentBoundaryWithoutReturningIssuedEffects(t *testing.T) {
	t.Parallel()
	s, approval := approvalFixture(t, "shard0")
	old := scopedPendingTransfer(&s)
	prepareScopedFixture(t, &s)
	work, err := queryReadyFences(&s, topology.AuthorityResource{}, math.MaxInt)
	if err != nil || len(work) != 0 {
		t.Fatal("issued effect was scheduled as a fence", work, err)
	}
	applyScopedApprovalFixture(t, &s, approval)
	before, _ := json.Marshal(s)
	work, err = queryReadyFences(&s, topology.AuthorityResource{}, 1)
	if err != nil || len(work) != 1 {
		t.Fatal(work, err)
	}
	resource := work[0].Resource
	key := s.authorityCatalog().floors[resource]
	if work[0].Grant.Key != key || key == old || work[0].Grant.Purpose != topology.AuthorityFence {
		t.Fatal(work)
	}
	work[0].Grant.Targets[0].NodeName = "caller"
	again, err := queryReadyFences(&s, resource, math.MaxInt)
	if err != nil || len(again) != 1 || again[0].Grant.Targets[0].NodeName == "caller" {
		t.Fatal("wrap or ownership failed", again, err)
	}
	after, _ := json.Marshal(s)
	if string(before) != string(after) {
		t.Fatal("fence selection changed state")
	}
	// A later boundary is selected even while earlier revocation rows retain
	// the old key. Selection never reconstructs a boundary from those rows.
	newer := s.indexes.grants[key].authorityRecord
	newer.Grant = newer.Grant.Clone()
	newer.Grant.Key.Revision++
	s.AppliedIndex = newer.Grant.Key.Revision
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution", authorityRecord: newer})
	for i := range s.AuthorityFloors {
		if s.AuthorityFloors[i].Resource == resource {
			s.AuthorityFloors[i].Key = newer.Grant.Key
		}
	}
	prepareScopedFixture(t, &s)
	work, err = queryReadyFences(&s, topology.AuthorityResource{}, 1)
	if err != nil || len(work) != 1 || work[0].Grant.Key != newer.Grant.Key {
		t.Fatal("old obligation selected instead of current boundary", work, err)
	}
}

func TestCompletedDeletionSettlesOnlyNativeResources(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"complete", "pending", "owned", "source_zero", "before_retirement", "declaration", "no_marker"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := completedRetentionFixture(t)
			row := s.indexes.groups["b"]
			switch mode {
			case "pending":
				row.Deleting = true
			case "owned":
				row.Owner = "exec-v2-a1"
			case "source_zero":
				row.SourceVersion = 0
			case "before_retirement":
				row.SourceVersion = 1
			case "declaration":
				row.Declaration = &topology.Group{ID: "b"}
			case "no_marker":
				s.RetiredGroups = nil
			}
			catalog := s.authorityCatalog()
			native := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "b"}
			host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "b", NodeName: "n"}
			if catalog.deletedResource(native) != (mode == "complete") || catalog.deletedResource(host) {
				t.Fatal(mode, catalog.deletedGroups)
			}
		})
	}
}
