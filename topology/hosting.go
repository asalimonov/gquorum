package topology

import (
	"fmt"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
)

// HostingMode selects all eligible nodes or an explicit host list.
type HostingMode string

// Hosting modes distinguish automatic eligibility from explicit host intent.
const (
	HostingAllEligible HostingMode = "all_eligible"
	HostingExplicit    HostingMode = "explicit"
)

// HostingRule selects load hosts independently of voter seats. Explicit names
// persist as intent even while a host is drained or excluded.
type HostingRule struct {
	Mode  HostingMode `json:"mode"`
	Nodes []string    `json:"nodes,omitempty"`
}

// Validate checks the mode and requires sorted, unique names for explicit
// hosting.
func (h HostingRule) Validate() error {
	switch h.Mode {
	case HostingAllEligible:
		if len(h.Nodes) != 0 {
			return invalid("all-eligible hosting cannot include explicit nodes")
		}
	case HostingExplicit:
		if len(h.Nodes) == 0 {
			return invalid("explicit hosting requires node names")
		}
		for i, name := range h.Nodes {
			if i > 0 && name <= h.Nodes[i-1] || strings.TrimSpace(name) == "" || strings.ContainsRune(name, 0) {
				return invalid("explicit hosts must be nonempty, sorted and unique")
			}
		}
	default:
		return invalid("unknown hosting mode")
	}
	return nil
}

// RequiredLoadHosts returns a deterministic complete hosting set without
// allocating identities or truncating to capacity. Legacy groups retain their
// current assignments; their historical placement algorithm is separate.
func RequiredLoadHosts(doc Document, groupID string) ([]string, error) {
	g := doc.Group(groupID)
	if g == nil || groupID == MetaGroupID {
		return nil, invalid("hosting requires an existing load group")
	}
	if g.VoterConfig == nil {
		hosts := make([]string, 0, len(g.Replicas))
		for _, r := range g.Replicas {
			hosts = append(hosts, r.NodeName)
		}
		slices.Sort(hosts)
		return hosts, nil
	}
	if g.Hosting == nil {
		return nil, invalid("voter-configured load group requires a hosting rule")
	}
	if err := g.Hosting.Validate(); err != nil {
		return nil, err
	}
	p := doc.GroupPlacement(groupID)
	var hosts []string
	for _, n := range doc.Nodes {
		if n.State != NodeActive || !p.Allows(n.Type) {
			continue
		}
		if g.Hosting.Mode == HostingExplicit && !slices.Contains(g.Hosting.Nodes, n.Name) {
			continue
		}
		hosts = append(hosts, n.Name)
	}
	slices.Sort(hosts)
	return hosts, nil
}

// RequiredLoadHostsFor computes the complete final set against an approved
// reference. Only exact approved identities can stay on a reserved node.
func RequiredLoadHostsFor(approved, desired Document, groupID string) ([]string, error) {
	g := desired.Group(groupID)
	if g == nil || groupID == MetaGroupID {
		return nil, invalid("hosting requires an existing load group")
	}
	if g.VoterConfig != nil {
		if g.Hosting == nil {
			return nil, invalid("voter-configured load group requires a hosting rule")
		}
		if err := g.Hosting.Validate(); err != nil {
			return nil, err
		}
	}
	report, err := EvaluatePlacement(PlacementInput{Approved: approved, Desired: desired, GroupID: groupID})
	if err != nil {
		return nil, err
	}
	var hosts []string
	legacyHostTarget := len(g.Replicas) + max(0, desired.GroupPlacement(groupID).MaxReplicas-
		len(desired.VotersOf(groupID)))
	if g.VoterConfig == nil && g.Placement != nil {
		legacyHostTarget = desired.GroupPlacement(groupID).MaxReplicas
	}
	hosts = retainedLoadHosts(g, report.Candidates, legacyHostTarget)
	if g.VoterConfig == nil && len(hosts) < legacyHostTarget {
		hosts = appendLegacyHosts(g, report.Candidates, hosts, legacyHostTarget)
		slices.Sort(hosts)
	}
	return hosts, nil // candidates are sorted by node name
}

// ValidateHostingCapacityFor checks final host counts against capacity with
// exact approved retention.
func ValidateHostingCapacityFor(approved, desired Document, rules Rules) error {
	for _, group := range desired.Groups {
		if group.ID == MetaGroupID {
			continue
		}
		hosts, err := RequiredLoadHostsFor(approved, desired, group.ID)
		if err != nil {
			return err
		}
		if len(hosts) > rules.Limits.MaxReplicas {
			return invalid(fmt.Sprintf("group %q requires %d hosts, exceeding replica capacity %d", group.ID, len(
				hosts), rules.Limits.MaxReplicas))
		}
	}
	return nil
}

// SelectFoundingRoles chooses eligible, unpinned voters in stable node order.
// Strict domains require a complete target and otherwise fail before founding.
func SelectFoundingRoles(doc Document, groupID string, pinned map[string]bool) (map[string]ReplicaRole, error) {
	g := doc.Group(groupID)
	if g == nil || g.VoterConfig == nil {
		return nil, invalid("founding selection requires a voter configuration")
	}
	if g.ID != MetaGroupID && g.Hosting == nil {
		return nil, invalid("voter-configured load group requires a hosting rule")
	}
	if g.Hosting != nil {
		if err := g.Hosting.Validate(); err != nil {
			return nil, err
		}
	}
	report, err := EvaluatePlacement(PlacementInput{Desired: doc, GroupID: groupID})
	if err != nil {
		return nil, err
	}
	roles, selected := selectFoundingCandidates(g, report.Candidates, pinned)
	minimum, _ := doc.GroupBounds(groupID)
	if groupID == MetaGroupID {
		minimum = 1
	}
	if selected < minimum || g.VoterConfig.DistinctVoterLabel != "" && selected != g.VoterConfig.TargetVoters {
		code := "insufficient_voter_candidates"
		message := fmt.Sprintf("group %q requires at least %d founding voters; %d eligible voters were selected",
			groupID, minimum, selected)
		if g.VoterConfig.DistinctVoterLabel != "" {
			code = "insufficient_distinct_zones"
			message = fmt.Sprintf("group %q requires %d distinct voter domains; %d eligible voters were selected",
				groupID, g.VoterConfig.TargetVoters, selected)
		}
		report.Issues = append(report.Issues, PlacementIssue{
			Code: code, GroupID: groupID,
			Key:     g.VoterConfig.DistinctVoterLabel,
			Message: message,
		})
		return nil, &PlacementError{
			Code:   fault.Invalid,
			Report: PlacementReport{Version: doc.Version, Groups: []GroupPlacementReport{report}},
		}
	}
	return roles, nil
}

// ValidateHostingCapacity rejects overflow before any identities are allocated
// or a partial desired document is staged.
func ValidateHostingCapacity(doc Document, rules Rules) error {
	for _, g := range doc.Groups {
		if g.ID == MetaGroupID {
			continue
		}
		hosts, err := RequiredLoadHosts(doc, g.ID)
		if err != nil {
			return err
		}
		if len(hosts) > rules.Limits.MaxReplicas {
			return invalid(fmt.Sprintf("group %q requires %d hosts, exceeding replica capacity %d", g.ID, len(
				hosts), rules.Limits.MaxReplicas))
		}
	}
	return nil
}

// FoundingRoles assigns the first target unpinned hosts in name order as
// voters.
// Subsequent admissions use learners and do not use this founding selection.
func FoundingRoles(hosts []string, pinned map[string]bool, target int) map[string]ReplicaRole {
	names := slices.Clone(hosts)
	slices.Sort(names)
	names = slices.Compact(names)
	roles := make(map[string]ReplicaRole, len(names))
	remaining := target
	for _, name := range names {
		role := ReplicaLearner
		if !pinned[name] && remaining > 0 {
			role = ReplicaVoter
			remaining--
		}
		roles[name] = role
	}
	return roles
}

func retainedLoadHosts(g *Group, candidates []PlacementCandidate, legacyHostTarget int) []string {
	var hosts []string
	for _, candidate := range candidates {
		retained := groupReplicaOnNode(g, candidate.NodeName) != nil
		if g.VoterConfig == nil && !retained {
			continue // replacement choices are considered after retained identities
		}
		if g.VoterConfig == nil && len(hosts) >= legacyHostTarget {
			continue
		}
		if retained && candidate.CanRetain || !retained && candidate.CanAdd {
			hosts = append(hosts, candidate.NodeName)
		}
	}

	return hosts
}

func appendLegacyHosts(g *Group, candidates []PlacementCandidate, hosts []string, legacyHostTarget int) []string {
	selected := hosts
	for _, candidate := range candidates {
		if len(selected) >= legacyHostTarget {
			break
		}
		if candidate.CanAdd && groupReplicaOnNode(g, candidate.NodeName) == nil {
			selected = append(selected, candidate.NodeName)
		}
	}
	return selected
}

func selectFoundingCandidates(g *Group, candidates []PlacementCandidate,
	pinned map[string]bool) (map[string]ReplicaRole, int) {
	roles := make(map[string]ReplicaRole)
	selected := 0
	domains := make(map[string]bool)
	for _, candidate := range candidates {
		if !candidate.CanAdd {
			continue
		}
		roles[candidate.NodeName] = ReplicaLearner
		if selected >= g.VoterConfig.TargetVoters || pinned[candidate.NodeName] || !candidate.CanVote {
			continue
		}
		if g.VoterConfig.DistinctVoterLabel != "" {
			if domains[candidate.Domain] {
				continue
			}
			domains[candidate.Domain] = true
		}
		roles[candidate.NodeName] = ReplicaVoter
		selected++
	}

	return roles, selected
}
