// Package topology defines replicated cluster declarations and exact approved
// effects.
package topology

import (
	"maps"
	"math"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

const (
	defaultPendingAuthorityRecords = 1024
	defaultMaxNodes                = 3
	defaultMaxReplicas             = 5
	defaultConcurrentGroups        = 4
	defaultConcurrentCleanup       = 2
)

// MetaGroupID names the reserved Raft group that replicates cluster topology.
const MetaGroupID = "_meta"

// NodeActive and the following values describe placement eligibility, drain and
// exclusion.
const (
	NodeActive   NodeState = "active"
	NodeDraining NodeState = "draining"
	NodeDrained  NodeState = "drained"
	NodeExcluded NodeState = "excluded"
)

// Places reports whether the state retains placement participation rather than
// exclusion.
func (s NodeState) Places() bool { return s == NodeActive || s == NodeDraining || s == NodeDrained }

// ReplicaVoter and ReplicaLearner identify desired Raft membership roles.
const (
	ReplicaVoter   ReplicaRole = "voter"
	ReplicaLearner ReplicaRole = "learner"
)

// Origin records the source of a topology edit.
type Origin string

// OriginAPI and the following values classify topology-edit provenance.
const (
	OriginAPI        Origin = "api"
	OriginReconciler Origin = "reconciler"
	OriginSystem     Origin = "system"
	OriginFounding   Origin = "founding"
	OriginOperator   Origin = "operator"
	OriginInternal   Origin = "internal"
)

// WithDefaults fills absent capacity limits with the library defaults.
func (l Limits) WithDefaults() Limits {
	if l.MaxPendingAuthorityRecords == 0 {
		l.MaxPendingAuthorityRecords = defaultPendingAuthorityRecords
	}
	if l.MaxNodes == 0 {
		l.MaxNodes = defaultMaxNodes
	}
	if l.MaxReplicas == 0 {
		l.MaxReplicas = defaultMaxReplicas
	}
	if l.MaxConcurrentGroups == 0 {
		l.MaxConcurrentGroups = defaultConcurrentGroups
	}
	if l.MaxConcurrentCleanup == 0 {
		l.MaxConcurrentCleanup = defaultConcurrentCleanup
	}
	return l
}

// Validate checks positive topology capacities and nonnegative execution limits.
func (l Limits) Validate() error {
	if l.MaxNodes < 1 || l.MaxReplicas < 1 || l.MaxPendingAuthorityRecords < 0 {
		return invalid("limits must be positive")
	}
	if l.MaxConcurrentGroups < 0 || l.MaxConcurrentCleanup < 0 {
		return invalid("execution limits cannot be negative")
	}
	return nil
}

// Allows reports whether the node type passes the rule's type restriction.
func (p PlacementRule) Allows(t NodeType) bool {
	return len(p.AllowedNodeTypes) == 0 || slices.Contains(p.AllowedNodeTypes, t)
}

// ChangeAudit records operator justification for an audited topology edit,
// including the explicit proof fields required by forced retirement.
type ChangeAudit struct {
	// Operation and TargetNode identify the audited action and subject.
	Operation  string `json:"operation"`
	TargetNode string `json:"target_node,omitempty"`
	// Forced, Reason and the staging identity/time record operator intent.
	Forced   bool      `json:"forced"`
	Reason   string    `json:"reason"`
	StagedBy string    `json:"staged_by"`
	StagedAt time.Time `json:"staged_at"`
}

// AuditOperationForceRetireNode identifies the audited forced-retirement
// operation.
const AuditOperationForceRetireNode = "force_retire_node"

// Document is one versioned cluster-topology declaration. Latest may be
// unapproved intent, Target is approved intent, and Active is fully converged;
// those views carry the same value type but have different authority.
type Document struct {
	// Version and ClusterID identify this declaration within one cluster.
	Version   uint64 `json:"version"`
	ClusterID string `json:"cluster_id"`
	// Cluster-wide placement and the declared nodes and Raft groups.
	Placement PlacementRule `json:"placement"`
	Nodes     []Node        `json:"nodes"`
	Groups    []Group       `json:"groups"`
	// Edit provenance and its previous version.
	ChangedBy     string       `json:"changed_by"`
	Origin        Origin       `json:"origin"`
	Comment       string       `json:"comment,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
	ParentVersion uint64       `json:"parent_version"`
	RebasedFrom   uint64       `json:"rebased_from,omitempty"` // prior staged edit, not approval
	Audit         *ChangeAudit `json:"audit,omitempty"`
}

// Document lookups return aliases into the receiver; edit an owned clone.

// Group returns an alias into the document; mutate only an owned clone.
func (d *Document) Group(id string) *Group {
	for i := range d.Groups {
		if d.Groups[i].ID == id {
			return &d.Groups[i]
		}
	}
	return nil
}

// Node returns the named node as an alias into the document.
func (d *Document) Node(name string) *Node {
	for i := range d.Nodes {
		if d.Nodes[i].Name == name {
			return &d.Nodes[i]
		}
	}
	return nil
}

// MetaGroup returns the reserved _meta group, if present.
func (d *Document) MetaGroup() *Group { return d.Group(MetaGroupID) }

// LoadGroups lists the non-_meta group values in document order.
func (d *Document) LoadGroups() []Group {
	var groups []Group
	for _, g := range d.Groups {
		if g.ID != MetaGroupID {
			groups = append(groups, g)
		}
	}
	return groups
}

// MemberID resolves a node name to its _meta ReplicaID, or zero if absent.
func (d *Document) MemberID(name string) uint64 {
	if g := d.MetaGroup(); g != nil {
		for _, r := range g.Replicas {
			if r.NodeName == name {
				return r.ReplicaID
			}
		}
	}
	return 0
}

// Replica and _meta member IDs are allocated from each group's high-water mark.

// AllocateReplicaID advances the group's allocation cursor without reusing IDs.
func (g *Group) AllocateReplicaID() (uint64, error) {
	next := g.NextReplicaID
	for _, r := range g.Replicas {
		if r.ReplicaID == math.MaxUint64 {
			return 0, invalid("replica ID exhausted")
		}
		if next <= r.ReplicaID {
			next = r.ReplicaID + 1
		}
	}
	if next == 0 {
		next = 1
	}
	if next == math.MaxUint64 {
		return 0, invalid("replica ID exhausted")
	}
	g.NextReplicaID = next + 1
	return next, nil
}

// AllocateMemberID allocates the next _meta replica ID.
func (d *Document) AllocateMemberID() (uint64, error) {
	g := d.MetaGroup()
	if g == nil {
		return 0, invalid("missing meta group")
	}
	return g.AllocateReplicaID()
}

// SetMemberID assigns a numeric _meta identity to a declared node.
func (d *Document) SetMemberID(name string, id uint64, at time.Time) error {
	g := d.MetaGroup()
	if g == nil || id == 0 || id == math.MaxUint64 || d.Node(name) == nil {
		return invalid("invalid meta identity")
	}
	for _, r := range g.Replicas {
		if r.NodeName != name && r.ReplicaID == id {
			return invalid("duplicate meta member ID")
		}
	}
	if id >= g.NextReplicaID {
		g.NextReplicaID = id + 1
	}
	for i := range g.Replicas {
		if g.Replicas[i].NodeName == name {
			g.Replicas[i].ReplicaID = id
			g.Replicas[i].UpdatedAt = at
			return nil
		}
	}
	role := ReplicaVoter
	if g.VoterConfig != nil {
		role = ReplicaLearner
	}
	g.Replicas = append(g.Replicas, Replica{NodeName: name, ReplicaID: id, Role: role, CreatedAt: at, UpdatedAt: at})
	return nil
}

// VotersOf lists desired voter replicas for a group in document order.
func (d *Document) VotersOf(id string) []Replica {
	var out []Replica
	if g := d.Group(id); g != nil {
		for _, r := range g.Replicas {
			if r.Role == ReplicaVoter {
				out = append(out, r)
			}
		}
	}
	return out
}

// QuorumOf returns the majority required for the supplied voter count.
func QuorumOf(voters int) int { return voters/2 + 1 }

// Placement queries use a group's complete override when present.

// GroupPlacement returns the effective rule for a group.
func (d *Document) GroupPlacement(id string) PlacementRule {
	if g := d.Group(id); g != nil && g.Placement != nil {
		return *g.Placement
	}
	return d.Placement
}

// GroupBounds returns the effective minimum and maximum voter counts.
func (d *Document) GroupBounds(id string) (minimum, maximum int) {
	p := d.GroupPlacement(id)
	return p.MinReplicas, p.MaxReplicas
}

// EffectiveNodeState reports a draining node as drained once its active load
// assignments are gone.
func EffectiveNodeState(active Document, name string) NodeState {
	n := active.Node(name)
	if n == nil {
		return ""
	}
	if n.State != NodeDraining {
		return n.State
	}
	for _, g := range active.Groups {
		if g.ID == MetaGroupID {
			continue
		}
		for _, r := range g.Replicas {
			if r.NodeName == name {
				return NodeDraining
			}
		}
	}
	return NodeDrained
}

// Clone returns an owned copy of the document and nested mutable values.
func (d Document) Clone() Document {
	out := d
	if d.Audit != nil {
		a := *d.Audit
		out.Audit = &a
	}
	out.Placement.AllowedNodeTypes = slices.Clone(d.Placement.AllowedNodeTypes)
	out.Placement.NodeSelector = maps.Clone(d.Placement.NodeSelector)
	out.Nodes = slices.Clone(d.Nodes)
	for i := range out.Nodes {
		out.Nodes[i].Labels = maps.Clone(d.Nodes[i].Labels)
	}
	out.Groups = slices.Clone(d.Groups)
	for i := range out.Groups {
		out.Groups[i] = cloneGroup(d.Groups[i])
	}
	return out
}

func invalid(message string) error {
	return fault.New(fault.Invalid, message, nil)
}
