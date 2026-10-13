// Package topology defines replicated cluster declarations and exact approved effects.
package topology

import (
	"maps"
	"math"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

const MetaGroupID = "_meta"

type NodeType string
type NodeState string

const (
	NodeActive   NodeState = "active"
	NodeDraining NodeState = "draining"
	NodeDrained  NodeState = "drained"
	NodeExcluded NodeState = "excluded"
)

func (s NodeState) Places() bool { return s == NodeActive || s == NodeDraining || s == NodeDrained }

type ReplicaRole string

const (
	ReplicaVoter   ReplicaRole = "voter"
	ReplicaLearner ReplicaRole = "learner"
)

type Origin string

const (
	OriginAPI        Origin = "api"
	OriginReconciler Origin = "reconciler"
	OriginSystem     Origin = "system"
	OriginFounding   Origin = "founding"
	OriginOperator   Origin = "operator"
	OriginInternal   Origin = "internal"
)

type Limits struct {
	MaxPendingAuthorityRecords int `json:"max_pending_authority_records,omitempty"`
	MaxNodes                   int `json:"max_nodes"`
	MaxReplicas                int `json:"max_replicas"`
}

func (l Limits) WithDefaults() Limits {
	if l.MaxPendingAuthorityRecords == 0 {
		l.MaxPendingAuthorityRecords = 1024
	}
	if l.MaxNodes == 0 {
		l.MaxNodes = 3
	}
	if l.MaxReplicas == 0 {
		l.MaxReplicas = 5
	}
	return l
}

func (l Limits) Validate() error {
	if l.MaxNodes < 1 || l.MaxReplicas < 1 || l.MaxPendingAuthorityRecords < 0 {
		return invalid("limits must be positive")
	}
	return nil
}

type Rules struct {
	SchemaVersion uint32 `json:"schema_version"`
	Limits        Limits `json:"limits"`
}

type PlacementRule struct {
	// MinReplicas and MaxReplicas bound voters, not hosted learners.
	// Limits.MaxReplicas bounds the total replicas of a load group.
	MinReplicas      int        `json:"min_replicas"`
	MaxReplicas      int        `json:"max_replicas"`
	AllowedNodeTypes []NodeType `json:"allowed_node_types,omitempty"`
}

func (p PlacementRule) Allows(t NodeType) bool {
	return len(p.AllowedNodeTypes) == 0 || slices.Contains(p.AllowedNodeTypes, t)
}

type Node struct {
	Name      string            `json:"name"`
	Type      NodeType          `json:"type"`
	State     NodeState         `json:"state"`
	Region    string            `json:"region,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type Replica struct {
	NodeName      string      `json:"node_name"`
	ReplicaID     uint64      `json:"replica_id"`
	Role          ReplicaRole `json:"role"`
	PinnedLearner bool        `json:"pinned_learner,omitempty"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

type Group struct {
	ID            string         `json:"id"`
	Replicas      []Replica      `json:"replicas"`
	NextReplicaID uint64         `json:"next_replica_id"`
	Placement     *PlacementRule `json:"placement,omitempty"`
	VoterPolicy   *VoterPolicy   `json:"voter_policy,omitempty"`
	Hosting       *HostingRule   `json:"hosting,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type ChangeAudit struct {
	Operation  string    `json:"operation"`
	TargetNode string    `json:"target_node,omitempty"`
	Forced     bool      `json:"forced"`
	Reason     string    `json:"reason"`
	StagedBy   string    `json:"staged_by"`
	StagedAt   time.Time `json:"staged_at"`
}

const AuditOperationForceRetireNode = "force_retire_node"

type Document struct {
	Version       uint64        `json:"version"`
	ClusterID     string        `json:"cluster_id"`
	Placement     PlacementRule `json:"placement"`
	Nodes         []Node        `json:"nodes"`
	Groups        []Group       `json:"groups"`
	ChangedBy     string        `json:"changed_by"`
	Origin        Origin        `json:"origin"`
	Comment       string        `json:"comment,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
	ParentVersion uint64        `json:"parent_version"`
	Audit         *ChangeAudit  `json:"audit,omitempty"`
}

// Group returns an alias into the document; mutate only an owned clone.
func (d *Document) Group(id string) *Group {
	for i := range d.Groups {
		if d.Groups[i].ID == id {
			return &d.Groups[i]
		}
	}
	return nil
}

func (d *Document) Node(name string) *Node {
	for i := range d.Nodes {
		if d.Nodes[i].Name == name {
			return &d.Nodes[i]
		}
	}
	return nil
}

func (d *Document) MetaGroup() *Group { return d.Group(MetaGroupID) }
func (d *Document) LoadGroups() []Group {
	var groups []Group
	for _, g := range d.Groups {
		if g.ID != MetaGroupID {
			groups = append(groups, g)
		}
	}
	return groups
}

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

func (d *Document) AllocateMemberID() (uint64, error) {
	g := d.MetaGroup()
	if g == nil {
		return 0, invalid("missing meta group")
	}
	return g.AllocateReplicaID()
}

func (d *Document) SetMemberID(name string, id uint64, at time.Time) error {
	g := d.MetaGroup()
	if g == nil || d.Node(name) == nil || id == 0 || id == math.MaxUint64 {
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
	if g.VoterPolicy != nil {
		role = ReplicaLearner
	}
	g.Replicas = append(g.Replicas, Replica{NodeName: name, ReplicaID: id, Role: role, CreatedAt: at, UpdatedAt: at})
	return nil
}

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

func QuorumOf(voters int) int { return voters/2 + 1 }

func (d *Document) GroupPlacement(id string) PlacementRule {
	if g := d.Group(id); g != nil && g.Placement != nil {
		return *g.Placement
	}
	return d.Placement
}

func (d *Document) GroupBounds(id string) (int, int) {
	p := d.GroupPlacement(id)
	return p.MinReplicas, p.MaxReplicas
}

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

func (d Document) Clone() Document {
	out := d
	if d.Audit != nil {
		a := *d.Audit
		out.Audit = &a
	}
	out.Placement.AllowedNodeTypes = slices.Clone(d.Placement.AllowedNodeTypes)
	out.Nodes = slices.Clone(d.Nodes)
	for i := range out.Nodes {
		out.Nodes[i].Labels = maps.Clone(d.Nodes[i].Labels)
	}
	out.Groups = slices.Clone(d.Groups)
	for i := range out.Groups {
		out.Groups[i].Replicas = slices.Clone(d.Groups[i].Replicas)
		if d.Groups[i].VoterPolicy != nil {
			p := *d.Groups[i].VoterPolicy
			out.Groups[i].VoterPolicy = &p
		}
		if d.Groups[i].Hosting != nil {
			h := *d.Groups[i].Hosting
			h.Nodes = slices.Clone(h.Nodes)
			out.Groups[i].Hosting = &h
		}
		if d.Groups[i].Placement != nil {
			p := *d.Groups[i].Placement
			p.AllowedNodeTypes = slices.Clone(p.AllowedNodeTypes)
			out.Groups[i].Placement = &p
		}
	}
	return out
}

func invalid(message string) error { return fault.New(fault.Invalid, message, nil) }
