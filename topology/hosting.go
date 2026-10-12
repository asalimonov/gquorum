package topology

import (
	"fmt"
	"slices"
	"strings"
)

type HostingMode string

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
			if strings.TrimSpace(name) == "" || strings.ContainsRune(name, 0) || i > 0 && name <= h.Nodes[i-1] {
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
	if g.VoterPolicy == nil {
		hosts := make([]string, 0, len(g.Replicas))
		for _, r := range g.Replicas {
			hosts = append(hosts, r.NodeName)
		}
		slices.Sort(hosts)
		return hosts, nil
	}
	if g.Hosting == nil {
		return nil, invalid("policy-managed load group requires a hosting rule")
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
			return invalid(fmt.Sprintf("group %q requires %d hosts, exceeding replica capacity %d", g.ID, len(hosts), rules.Limits.MaxReplicas))
		}
	}
	return nil
}

// FoundingRoles assigns the first target unpinned hosts in name order as voters.
// Subsequent admissions use learners and do not use this founding selection.
func FoundingRoles(hosts []string, pinned map[string]bool, target int) map[string]ReplicaRole {
	names := slices.Clone(hosts)
	slices.Sort(names)
	names = slices.Compact(names)
	roles := make(map[string]ReplicaRole, len(names))
	for _, name := range names {
		role := ReplicaLearner
		if !pinned[name] && target > 0 {
			role = ReplicaVoter
			target--
		}
		roles[name] = role
	}
	return roles
}
