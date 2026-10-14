//go:build !functional

package gquorum

import (
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func ensureFixture() (GroupSpec, topology.Group) {
	voterConfig := topology.DefaultVoterConfig()
	spec := GroupSpec{ID: "work", Labels: map[string]string{"app": "demo"}, VoterConfig: &voterConfig, Nodes: []string{"n2", "n1"}}
	group := topology.Group{ID: "work", Labels: map[string]string{"app": "demo"}, VoterConfig: &voterConfig,
		Hosting: &topology.HostingRule{Mode: topology.HostingExplicit, Nodes: []string{"n1", "n2"}}}
	return spec, group
}

func TestGroupMatchesComparesOnlySetFields(t *testing.T) {
	t.Parallel()
	spec, group := ensureFixture()
	if !groupMatches(spec, group) {
		t.Fatal("matching group refused")
	}
	if !groupMatches(GroupSpec{ID: "work"}, group) {
		t.Fatal("unset fields must be unconstrained")
	}
	// Storage is always compared: an empty class means the default store.
	memory := group
	memory.Storage = topology.StorageMemory
	if groupMatches(GroupSpec{ID: "work"}, memory) || !groupMatches(GroupSpec{ID: "work", Storage: topology.StorageMemory}, memory) {
		t.Fatal("storage class must always match")
	}
	cases := map[string]func(*GroupSpec){
		"id":        func(s *GroupSpec) { s.ID = "other" },
		"labels":    func(s *GroupSpec) { s.Labels = map[string]string{"app": "other"} },
		"nodes":     func(s *GroupSpec) { s.Nodes = []string{"n1"} },
		"voters":    func(s *GroupSpec) { p := *s.VoterConfig; p.TargetVoters = 5; s.VoterConfig = &p },
		"placement": func(s *GroupSpec) { s.Placement = &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1} },
		"hosting":   func(s *GroupSpec) { s.Hosting = &topology.HostingRule{Mode: topology.HostingAllEligible} },
		"storage":   func(s *GroupSpec) { s.Storage = topology.StorageMemory },
	}
	for name, edit := range cases {
		changed, _ := ensureFixture()
		edit(&changed)
		if groupMatches(changed, group) {
			t.Errorf("%s: differing spec matched", name)
		}
	}
}

func TestOwnCreationRecognizesOnlyTheActorsSingleGroupAddition(t *testing.T) {
	t.Parallel()
	spec, group := ensureFixture()
	other := topology.Group{ID: "kept", Labels: map[string]string{"k": "v"}}
	target := topology.Document{Version: 4, Nodes: []topology.Node{{Name: "n1"}, {Name: "n2"}}, Groups: []topology.Group{other}}
	latest := target.Clone()
	latest.Version, latest.ParentVersion, latest.ChangedBy = 5, 4, "app"
	latest.Groups = append(latest.Groups, group)
	if !ownCreation(latest, target, spec, "app") {
		t.Fatal("own creation not recognized")
	}
	cases := map[string]func(*topology.Document){
		"actor":       func(d *topology.Document) { d.ChangedBy = "operator" },
		"parent":      func(d *topology.Document) { d.ParentVersion = 3 },
		"other group": func(d *topology.Document) { d.Groups[0].Labels = map[string]string{"k": "changed"} },
		"node edit":   func(d *topology.Document) { d.Nodes = d.Nodes[:1] },
		"missing":     func(d *topology.Document) { d.Groups = d.Groups[:1] },
	}
	for name, edit := range cases {
		changed := latest.Clone()
		edit(&changed)
		if ownCreation(changed, target, spec, "app") {
			t.Errorf("%s: foreign intent accepted", name)
		}
	}
}
