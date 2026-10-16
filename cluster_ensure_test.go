//go:build !functional

package gquorum

import (
	"errors"
	"github.com/asalimonov/gquorum/fault"
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
	if !ownCreation(latest, target, spec, EnsureOptions{Actor: "app"}) {
		t.Fatal("own creation not recognized")
	}
	cases := map[string]func(*topology.Document){
		"comment":     func(d *topology.Document) { d.Comment = "other intent" },
		"actor":       func(d *topology.Document) { d.ChangedBy = "operator" },
		"parent":      func(d *topology.Document) { d.ParentVersion = 3 },
		"other group": func(d *topology.Document) { d.Groups[0].Labels = map[string]string{"k": "changed"} },
		"node edit":   func(d *topology.Document) { d.Nodes = d.Nodes[:1] },
		"missing":     func(d *topology.Document) { d.Groups = d.Groups[:1] },
	}
	for name, edit := range cases {
		changed := latest.Clone()
		edit(&changed)
		if ownCreation(changed, target, spec, EnsureOptions{Actor: "app"}) {
			t.Errorf("%s: foreign intent accepted", name)
		}
	}
}

func TestEnsureGroupUsesExactProgressAndRefusesOverlappingIntent(t *testing.T) {
	t.Parallel()
	spec, group := ensureFixture()
	row := topology.GroupProgress{GroupID: spec.ID, Desired: &group, Approved: &group,
		Converged: &group, ConvergedVersion: 3}
	result, err := ensureKnownGroup(spec, row)
	if err != nil || !result.Done || result.Group.ID != spec.ID {
		t.Fatal(result, err)
	}
	result.Group.Labels["app"] = "caller"
	if group.Labels["app"] != "demo" {
		t.Fatal("borrowed declaration")
	}
	for _, mode := range []string{"staged delete", "approved delete", "desired change", "approved change", "deleting"} {
		changed := row.Clone()
		switch mode {
		case "staged delete":
			changed.Desired = nil
		case "approved delete":
			changed.Desired, changed.Approved = nil, nil
		case "desired change":
			changed.Desired.Labels["app"] = "different"
		case "approved change":
			changed.Approved.Labels["app"] = "different"
		case "deleting":
			changed.Deleting = true
		}
		if result, err := ensureKnownGroup(spec, changed); !result.Done || !errors.Is(err, fault.ErrConflict) {
			t.Fatal(mode, result, err)
		}
	}
	changed := row.Clone()
	changed.Converged.Labels["app"] = "old"
	if result, err := ensureKnownGroup(spec, changed); result.Done || err != nil {
		t.Fatal("approved matching declaration did not wait for convergence", result, err)
	}
	row.ConvergedVersion = 0
	if result, err := ensureKnownGroup(spec, row); result.Done || err != nil {
		t.Fatal("unknown source returned convergence", result, err)
	}
	row.Converged = nil
	if result, err := ensureKnownGroup(spec, row); result.Done || err != nil {
		t.Fatal("approval returned convergence", result, err)
	}
}
