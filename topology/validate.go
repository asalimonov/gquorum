package topology

import (
	"fmt"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
)

// ValidateID ensures an identifier cannot escape its caller-supplied storage
// root.
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

// ValidateLoadID validates a group identifier and rejects the reserved meta
// name.
func ValidateLoadID(id string) error {
	if id == MetaGroupID {
		return invalid("meta ID is reserved")
	}
	return ValidateID(id)
}

func (p PlacementRule) validate(limits Limits) error {
	if err := ValidateSelector(p.NodeSelector); err != nil {
		return err
	}
	if p.MinReplicas < 1 || p.MaxReplicas < p.MinReplicas || p.MaxReplicas > limits.MaxReplicas {
		return invalid("invalid replica placement bounds")
	}
	for i, t := range p.AllowedNodeTypes {
		if i > 0 && t <= p.AllowedNodeTypes[i-1] || strings.TrimSpace(string(t)) == "" {
			return invalid("allowed node types must be nonempty, sorted and unique")
		}
	}
	return nil
}

// Validate checks a complete document against cluster rules and topology
// invariants, including directly assigned labels and placement declarations.
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
	if err := validateAudit(d.Audit); err != nil {
		return err
	}
	v := documentValidation{doc: d, rules: rules, nodes: make(map[string]Node, len(d.Nodes)),
		groups: make(map[string]bool, len(d.Groups)), metaNodes: make(map[string]bool)}
	if err := v.validateNodes(); err != nil {
		return err
	}
	for _, g := range d.Groups {
		if err := v.validateGroup(g); err != nil {
			return err
		}
	}
	if err := v.validateMetaMembership(); err != nil {
		return err
	}
	return ValidateHostingCapacity(*d, rules)
}

// ValidateTransition checks identity and allocator invariants against the
// previous declaration.
func ValidateTransition(previous, next Document, rules Rules, retired map[string]bool) error {
	if err := next.Validate(rules); err != nil {
		return err
	}
	if previous.ClusterID != "" && next.ClusterID != previous.ClusterID {
		return invalid("cluster identity cannot change")
	}
	if err := validateNodeTransitions(previous, next); err != nil {
		return err
	}
	for _, g := range next.Groups {
		if retired[g.ID] {
			return fault.New(fault.GroupRetired, "retired group ID", nil)
		}
		old := previous.Group(g.ID)
		if old == nil {
			continue
		}
		if err := validateGroupTransition(previous, g, old); err != nil {
			return err
		}
	}
	return nil
}

type documentValidation struct {
	doc       *Document
	rules     Rules
	nodes     map[string]Node
	groups    map[string]bool
	metaNodes map[string]bool
}

func (v *documentValidation) validateNodes() error {
	nodes := v.nodes
	live := 0
	for _, n := range v.doc.Nodes {
		if err := ValidateLabels(n.Labels, NodeLabels); err != nil {
			return err
		}
		if strings.TrimSpace(n.Name) == "" || strings.ContainsRune(n.Name, 0) ||
			strings.TrimSpace(string(n.Type)) == "" {
			return invalid("node name and type must be nonempty")
		}
		if n.State != NodeExcluded && !n.State.Places() {
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
	if live == 0 || live > v.rules.Limits.MaxNodes {
		return invalid("live node count outside limits")
	}
	return nil
}

func (v *documentValidation) validateGroup(g Group) error {
	if err := ValidateLabels(g.Labels, GroupLabels); err != nil {
		return err
	}
	if err := ValidateID(g.ID); err != nil {
		return err
	}
	if v.groups[g.ID] {
		return invalid("duplicate group ID")
	}
	v.groups[g.ID] = true
	if err := ValidateStorageClass(g.Storage); err != nil {
		return err
	}
	if g.ID == MetaGroupID && g.Storage != StorageDefault {
		return invalid("meta uses the default storage class")
	}
	placement := v.doc.GroupPlacement(g.ID)
	if g.ID == MetaGroupID {
		if g.Placement != nil {
			return invalid("meta has no separate placement rule")
		}
	} else if err := placement.validate(v.rules.Limits); err != nil {
		return err
	}
	if err := v.validateGroupConfiguration(g, placement); err != nil {
		return err
	}
	return v.validateGroupReplicas(g, placement)
}

func (v *documentValidation) validateGroupConfiguration(g Group, placement PlacementRule) error {
	if err := v.validateHosting(g); err != nil {
		return err
	}
	if g.VoterConfig != nil {
		lo, hi := placement.MinReplicas, placement.MaxReplicas
		if g.ID == MetaGroupID {
			lo, hi = 1, v.rules.Limits.MaxNodes
		} else if g.Hosting == nil {
			return invalid("voter-configured load group requires a hosting rule")
		}
		if err := g.VoterConfig.Validate(lo, hi); err != nil {
			return err
		}
	}
	return nil
}

func (v *documentValidation) validateGroupReplicas(g Group, placement PlacementRule) error {
	state := replicaValidation{group: g, placement: placement, nodes: v.nodes, metaNodes: v.metaNodes,
		ids: make(map[uint64]bool, len(g.Replicas)), hosts: make(map[string]bool, len(g.Replicas))}
	for _, r := range g.Replicas {
		if err := state.add(r); err != nil {
			return err
		}
	}

	if g.NextReplicaID == 0 {
		return invalid("zero replica allocation counter")
	}
	if g.ID == MetaGroupID {
		if state.voters == 0 {
			return invalid("meta group must retain a voter")
		}
	} else if len(g.Replicas) > v.rules.Limits.MaxReplicas || state.voters > placement.MaxReplicas {
		return invalid("load replica count exceeds placement or limits")
	}
	return nil
}

type replicaValidation struct {
	group     Group
	placement PlacementRule
	nodes     map[string]Node
	metaNodes map[string]bool
	ids       map[uint64]bool
	hosts     map[string]bool
	voters    int
}

func (v *replicaValidation) add(r Replica) error {
	n, exists := v.nodes[r.NodeName]
	if !exists {
		return invalid(fmt.Sprintf("group %q references unknown node %q", v.group.ID, r.NodeName))
	}
	if r.ReplicaID == 0 || r.ReplicaID >= v.group.NextReplicaID || v.ids[r.ReplicaID] || v.hosts[r.NodeName] {
		return invalid("duplicate or invalid replica identity")
	}
	if r.Role != ReplicaVoter && r.Role != ReplicaLearner {
		return invalid("unknown replica role")
	}
	if r.PinnedLearner && r.Role != ReplicaLearner {
		return invalid("pinned learner cannot be a voter")
	}
	v.ids[r.ReplicaID], v.hosts[r.NodeName] = true, true
	if r.Role == ReplicaVoter {
		v.voters++
	}
	if v.group.ID == MetaGroupID {
		v.metaNodes[r.NodeName] = true
	} else {
		if n.State == NodeExcluded || !v.placement.Allows(n.Type) {
			return invalid("load replica on excluded or ineligible node")
		}
	}
	return nil
}

func validateGroupTransition(previous Document, g Group, old *Group) error {
	if g.VoterConfig != nil && g.VoterConfig.TargetVoters == 1 &&
		(old.VoterConfig == nil || old.VoterConfig.TargetVoters != 1) && len(previous.VotersOf(g.ID)) > 1 {
		return invalid("voter target reduction would require an unsupported two-to-one transition")
	}
	if g.NextReplicaID < old.NextReplicaID {
		return invalid("replica allocation counter cannot decrease")
	}
	if g.Storage != old.Storage {
		return invalid("storage class is immutable")
	}
	for _, r := range g.Replicas {
		if r.ReplicaID >= old.NextReplicaID {
			continue
		}
		found := slices.ContainsFunc(old.Replicas,
			func(p Replica) bool { return p.ReplicaID == r.ReplicaID && p.NodeName == r.NodeName })
		if !found {
			return invalid("retired replica identity cannot be reused")
		}
	}
	return nil
}

func validateAudit(a *ChangeAudit) error {
	if a != nil && a.Forced {
		if strings.TrimSpace(a.Operation) == "" || strings.TrimSpace(a.TargetNode) == "" ||
			strings.TrimSpace(a.Reason) == "" ||
			strings.TrimSpace(a.StagedBy) == "" || a.StagedAt.IsZero() {
			return invalid("incomplete forced change audit")
		}
	}

	return nil
}

func (v *documentValidation) validateMetaMembership() error {
	if !v.groups[MetaGroupID] {
		return invalid("missing meta group")
	}
	for _, n := range v.doc.Nodes {
		if !v.metaNodes[n.Name] && n.State.Places() {
			return invalid("admitted node lacks meta membership")
		}
	}

	return nil
}

func validateNodeTransitions(previous, next Document) error {
	for _, n := range next.Nodes {
		old := previous.Node(n.Name)
		if old != nil && old.State != NodeExcluded && n.Type != old.Type {
			return invalid("node type is immutable before readmission")
		}
		if old != nil && old.State == NodeExcluded && n.State.Places() &&
			next.MemberID(n.Name) == previous.MemberID(n.Name) {
			return invalid("readmission requires a fresh meta identity")
		}
	}

	return nil
}

func (v *documentValidation) validateHosting(g Group) error {
	if g.Hosting != nil {
		if g.ID == MetaGroupID || g.VoterConfig == nil {
			return invalid("hosting rule requires a voter-configured load group")
		}
		if err := g.Hosting.Validate(); err != nil {
			return err
		}
		for _, name := range g.Hosting.Nodes {
			if _, ok := v.nodes[name]; !ok {
				return invalid("explicit hosting references an unknown node")
			}
		}
	}

	return nil
}
