package topology

import (
	"fmt"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
)

// ValidateID ensures an identifier cannot escape its caller-supplied storage root.
func ValidateID(id string) error {
	if id == "" || id == "." || id == ".." || strings.Contains(id, ".quarantine-") {
		return invalid("invalid group ID")
	}
	for _, c := range id {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' {
			continue
		}
		return invalid("group ID must be a safe path component")
	}
	return nil
}

func ValidateLoadID(id string) error {
	if id == MetaGroupID {
		return invalid("meta ID is reserved")
	}
	return ValidateID(id)
}

func (p PlacementRule) validate(limits Limits) error {
	if p.MinReplicas < 1 || p.MaxReplicas < p.MinReplicas || p.MaxReplicas > limits.MaxReplicas {
		return invalid("invalid replica placement bounds")
	}
	for i, t := range p.AllowedNodeTypes {
		if strings.TrimSpace(string(t)) == "" || i > 0 && t <= p.AllowedNodeTypes[i-1] {
			return invalid("allowed node types must be nonempty, sorted and unique")
		}
	}
	return nil
}

func (d *Document) Validate(rules Rules) error {
	if rules.SchemaVersion != 1 {
		return invalid("unsupported topology schema")
	}
	if err := rules.Limits.Validate(); err != nil {
		return err
	}
	if d.Version == 0 && len(d.Nodes) == 0 && len(d.Groups) == 0 {
		return nil
	}
	if strings.TrimSpace(d.ClusterID) == "" || strings.ContainsRune(d.ClusterID, 0) {
		return invalid("empty cluster ID")
	}
	if err := d.Placement.validate(rules.Limits); err != nil {
		return err
	}
	if a := d.Audit; a != nil && a.Forced {
		if strings.TrimSpace(a.Operation) == "" || strings.TrimSpace(a.TargetNode) == "" || strings.TrimSpace(a.Reason) == "" || strings.TrimSpace(a.StagedBy) == "" || a.StagedAt.IsZero() {
			return invalid("incomplete forced change audit")
		}
	}
	nodes := make(map[string]Node, len(d.Nodes))
	live := 0
	for _, n := range d.Nodes {
		if strings.TrimSpace(n.Name) == "" || strings.ContainsRune(n.Name, 0) || strings.TrimSpace(string(n.Type)) == "" {
			return invalid("node name and type must be nonempty")
		}
		if !n.State.Places() && n.State != NodeExcluded {
			return invalid("unknown node state")
		}
		if _, exists := nodes[n.Name]; exists {
			return invalid("duplicate node name")
		}
		nodes[n.Name] = n
		if n.State.Places() {
			live++
		}
	}
	if live == 0 || live > rules.Limits.MaxNodes {
		return invalid("live node count outside limits")
	}
	groups := make(map[string]bool, len(d.Groups))
	metaNodes := make(map[string]bool)
	for _, g := range d.Groups {
		if err := ValidateID(g.ID); err != nil {
			return err
		}
		if groups[g.ID] {
			return invalid("duplicate group ID")
		}
		groups[g.ID] = true
		placement := d.GroupPlacement(g.ID)
		if g.ID == MetaGroupID {
			if g.Placement != nil {
				return invalid("meta has no separate placement rule")
			}
		} else if err := placement.validate(rules.Limits); err != nil {
			return err
		}
		ids := make(map[uint64]bool, len(g.Replicas))
		hosts := make(map[string]bool, len(g.Replicas))
		voters := 0
		for _, r := range g.Replicas {
			n, exists := nodes[r.NodeName]
			if !exists {
				return invalid(fmt.Sprintf("group %q references unknown node %q", g.ID, r.NodeName))
			}
			if r.ReplicaID == 0 || r.ReplicaID >= g.NextReplicaID || ids[r.ReplicaID] || hosts[r.NodeName] {
				return invalid("duplicate or invalid replica identity")
			}
			if r.Role != ReplicaVoter && r.Role != ReplicaLearner {
				return invalid("unknown replica role")
			}
			ids[r.ReplicaID], hosts[r.NodeName] = true, true
			if r.Role == ReplicaVoter {
				voters++
			}
			if g.ID == MetaGroupID {
				metaNodes[r.NodeName] = true
			} else {
				if n.State == NodeExcluded || !placement.Allows(n.Type) {
					return invalid("load replica on excluded or ineligible node")
				}
			}
		}
		if g.NextReplicaID == 0 {
			return invalid("zero replica allocation counter")
		}
		if g.ID == MetaGroupID {
			if voters == 0 {
				return invalid("meta group must retain a voter")
			}
		} else if len(g.Replicas) > rules.Limits.MaxReplicas || voters > placement.MaxReplicas {
			return invalid("load replica count exceeds placement or limits")
		}
	}
	if !groups[MetaGroupID] {
		return invalid("missing meta group")
	}
	for _, n := range d.Nodes {
		if n.State.Places() && !metaNodes[n.Name] {
			return invalid("admitted node lacks meta membership")
		}
	}
	return nil
}

// ValidateTransition checks identity and allocator invariants against the previous declaration.
func ValidateTransition(previous, next Document, rules Rules, retired map[string]bool) error {
	if err := next.Validate(rules); err != nil {
		return err
	}
	if previous.ClusterID != "" && next.ClusterID != previous.ClusterID {
		return invalid("cluster identity cannot change")
	}
	for _, n := range next.Nodes {
		old := previous.Node(n.Name)
		if old != nil && old.State != NodeExcluded && n.Type != old.Type {
			return invalid("node type is immutable before readmission")
		}
		if old != nil && old.State == NodeExcluded && n.State.Places() && next.MemberID(n.Name) == previous.MemberID(n.Name) {
			return invalid("readmission requires a fresh meta identity")
		}
	}
	for _, g := range next.Groups {
		if retired[g.ID] {
			return fault.New(fault.GroupRetired, "retired group ID", nil)
		}
		old := previous.Group(g.ID)
		if old == nil {
			continue
		}
		if g.NextReplicaID < old.NextReplicaID {
			return invalid("replica allocation counter cannot decrease")
		}
		for _, r := range g.Replicas {
			if r.ReplicaID >= old.NextReplicaID {
				continue
			}
			found := slices.ContainsFunc(old.Replicas, func(p Replica) bool { return p.ReplicaID == r.ReplicaID && p.NodeName == r.NodeName })
			if !found {
				return invalid("retired replica identity cannot be reused")
			}
		}
	}
	return nil
}
