//go:build !functional

package topology

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func TestVoterConfigValidation(t *testing.T) {
	t.Parallel()
	p := DefaultVoterConfig()
	if p.TargetVoters != 3 || p.FailureWindow != 30*time.Second || p.StandbyWindow != time.Minute || p.SwitchCooldown != 5*time.Minute || !p.AllowNewFailureDuringCooldown {
		t.Fatal("unexpected defaults", p)
	}
	for name, edit := range map[string]func(*VoterConfig){
		"zero target":                func(p *VoterConfig) { p.TargetVoters = 0 },
		"above bounds":               func(p *VoterConfig) { p.TargetVoters = 8 },
		"surge":                      func(p *VoterConfig) { p.MaxSurgeVoters = 2 },
		"negative surge":             func(p *VoterConfig) { p.MaxSurgeVoters = -1 },
		"failure":                    func(p *VoterConfig) { p.FailureWindow = 0 },
		"standby":                    func(p *VoterConfig) { p.StandbyWindow = -1 },
		"gap":                        func(p *VoterConfig) { p.MaxObservationGap = 0 },
		"no progress":                func(p *VoterConfig) { p.NoProgressWindow = 0 },
		"negative disabled progress": func(p *VoterConfig) { p.ReplaceNoProgress = false; p.NoProgressWindow = -1 },
		"cooldown":                   func(p *VoterConfig) { p.SwitchCooldown = -1 },
		"demotion cooldown":          func(p *VoterConfig) { p.DemotionCooldown = -1 },
		"promotion core bound":       func(p *VoterConfig) { p.StandbyLagEntries = 1001; p.FailureLagEntries = 2000 },
		"hysteresis":                 func(p *VoterConfig) { p.FailureLagEntries = p.StandbyLagEntries },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bad := DefaultVoterConfig()
			edit(&bad)
			if err := bad.Validate(1, 7); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal("accepted invalid configuration", bad, err)
			}
		})
	}
	if err := p.Validate(1, 7); err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(4, 7); err == nil {
		t.Fatal("accepted target below minimum")
	}
	p.MaxSurgeVoters, p.StandbyLagEntries, p.StandbyApplyBacklogEntries = 0, 0, 0
	p.SwitchCooldown, p.DemotionCooldown, p.NoProgressWindow = 0, 0, 0
	p.ReplaceNoProgress = false
	if err := p.Validate(1, 7); err != nil {
		t.Fatal("meaningful zero rejected", err)
	}
}

func TestVoterConfigLegacyEncodingAndClone(t *testing.T) {
	t.Parallel()
	legacy := testDocument()
	b, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"voter_config", "hosting", "pinned_learner"} {
		if bytes.Contains(b, []byte(key)) {
			t.Fatal("legacy encoding acquired default", key)
		}
	}
	var round Document
	if err := json.Unmarshal(b, &round); err != nil || !reflect.DeepEqual(round, legacy) {
		t.Fatal(err, round)
	}
	p := DefaultVoterConfig()
	legacy.MetaGroup().VoterConfig = &p
	p2 := p
	p2.TargetVoters = 2
	g := legacy.Group("load")
	g.VoterConfig = &p2
	g.Hosting = &HostingRule{Mode: HostingExplicit, Nodes: []string{"a", "c"}}
	g.Replicas[1].PinnedLearner = true
	before := legacy.Clone()
	clone := legacy.Clone()
	clone.MetaGroup().VoterConfig.TargetVoters = 2
	clone.Group("load").VoterConfig.StandbyWindow = time.Hour
	clone.Group("load").Hosting.Nodes[0] = "other"
	clone.Group("load").Replicas[1].PinnedLearner = false
	if !reflect.DeepEqual(before, legacy) {
		t.Fatal("clone changed source")
	}
	b, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"voter_config"`)) || bytes.Contains(b, []byte(`"voter_policy"`)) {
		t.Fatal("unexpected voter configuration encoding", string(b))
	}
	if err := json.Unmarshal(b, &round); err != nil || !reflect.DeepEqual(round, legacy) {
		t.Fatal("configuration roundtrip", err)
	}
	if err := round.Validate(testRules()); err != nil {
		t.Fatal(err)
	}
	round.MetaGroup().Replicas[0].PinnedLearner = true
	if err := round.Validate(testRules()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("pinned voter accepted", err)
	}
}

func TestAllEligibleHostingAndCapacity(t *testing.T) {
	t.Parallel()
	d := testDocument()
	g := d.Group("load")
	p := DefaultVoterConfig()
	g.VoterConfig = &p
	g.Placement = nil
	g.Hosting = &HostingRule{Mode: HostingAllEligible}
	for i := 4; i <= 7; i++ {
		d.Nodes = append(d.Nodes, Node{Name: fmt.Sprint(i), Type: "worker", State: NodeActive})
	}
	want := []string{"4", "5", "6", "7", "a", "b", "c"}
	before := d.Clone()
	hosts, err := RequiredLoadHosts(d, "load")
	if err != nil || !slices.Equal(hosts, want) {
		t.Fatal(hosts, err)
	}
	rules := Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 7, MaxReplicas: 7}}
	if err := ValidateHostingCapacity(d, rules); err != nil {
		t.Fatal(err)
	}
	rules.Limits.MaxReplicas = 5
	if err := ValidateHostingCapacity(d, rules); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("overflow accepted", err)
	}
	if !reflect.DeepEqual(d, before) {
		t.Fatal("capacity validation allocated or truncated topology")
	}
	g.Hosting = &HostingRule{Mode: HostingExplicit, Nodes: []string{"4", "a", "c"}}
	d.Node("4").State = NodeDraining
	g.Placement = &PlacementRule{MinReplicas: 1, MaxReplicas: 3, AllowedNodeTypes: []NodeType{"worker"}}
	hosts, err = RequiredLoadHosts(d, "load")
	if err != nil || !slices.Equal(hosts, []string{"a", "c"}) {
		t.Fatal(hosts, err)
	}
	d.Node("4").State = NodeActive
	hosts, err = RequiredLoadHosts(d, "load")
	if err != nil || !slices.Equal(hosts, []string{"4", "a", "c"}) {
		t.Fatal("explicit intent lost", hosts, err)
	}
}

func TestFoundingVotersSmallAndPinned(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 2, 3, 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			var hosts []string
			for i := n; i > 0; i-- {
				hosts = append(hosts, fmt.Sprint(i))
			}
			roles := FoundingRoles(hosts, nil, 3)
			for name, role := range roles {
				want := ReplicaLearner
				if name <= "3" {
					want = ReplicaVoter
				}
				if role != want {
					t.Fatal(name, role, want)
				}
			}
		})
	}
	roles := FoundingRoles([]string{"c", "a", "b", "a"}, map[string]bool{"a": true}, 2)
	if roles["a"] != ReplicaLearner || roles["b"] != ReplicaVoter || roles["c"] != ReplicaVoter {
		t.Fatal(roles)
	}
}

func TestVoterConfigTargetShortfallAndUnsafeReduction(t *testing.T) {
	t.Parallel()
	d := testDocument()
	p := DefaultVoterConfig()
	p.TargetVoters = 5
	d.MetaGroup().VoterConfig = &p
	rules := testRules()
	rules.Limits.MaxNodes = 7
	if err := d.Validate(rules); err != nil {
		t.Fatal("target shortfall rejected", err)
	}
	after := d.Clone()
	after.MetaGroup().VoterConfig.TargetVoters = 1
	if err := ValidateTransition(d, after, rules, nil); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("unsupported reduction accepted", err)
	}
	if err := after.Validate(rules); err != nil {
		t.Fatal("target one invalid for fresh founding", err)
	}
}
