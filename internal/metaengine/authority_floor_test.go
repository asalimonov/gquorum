//go:build !functional

package metaengine

import (
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestScopedArchivedFloorsRemainValidatedWithoutHolder(t *testing.T) {
	t.Parallel()
	s := completedRetentionFixture(t)
	pruneScopedFixture(t, &s)
	for name, mutate := range map[string]func(*resourceFloor){
		"zero revision":    func(f *resourceFloor) { f.Key.Revision = 0 },
		"future revision":  func(f *resourceFloor) { f.Key.Revision = s.AppliedIndex + 1 },
		"cluster":          func(f *resourceFloor) { f.Key.ClusterID = "foreign" },
		"group":            func(f *resourceFloor) { f.Key.GroupID = "other" },
		"kind":             func(f *resourceFloor) { f.Resource.Kind = "unknown" },
		"host":             func(f *resourceFloor) { f.Resource.Kind = topology.AuthorityHost; f.Resource.NodeName = "" },
		"group names host": func(f *resourceFloor) { f.Resource.Kind = topology.AuthorityGroup; f.Resource.NodeName = "n" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bad := s
			bad.AuthorityFloors = append([]resourceFloor(nil), s.AuthorityFloors...)
			mutate(&bad.AuthorityFloors[0])
			if _, err := prepareScopedGraph(&bad, bad.Rules); err == nil {
				t.Fatal("malformed record-less floor accepted")
			}
		})
	}
}
