package metaservice

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

// MutationOptions controls one CAS edit. Zero ExpectedVersion uses the locally
// observed Latest as the CAS pin. Preview computes the same document without a
// proposal; Preview and Apply cannot be combined. Apply authorizes convergence
// after staging, and can fail after Document has already committed.
type MutationOptions struct {
	ExpectedVersion uint64 `json:"expected_version,omitempty"`
	Actor           string `json:"actor,omitempty"`
	Comment         string `json:"comment,omitempty"`
	Apply           bool   `json:"apply"`
	Preview         bool   `json:"preview"`
}
type MutationResult struct {
	Document   topology.Document   `json:"document"`
	OperatorID topology.OperatorID `json:"operator_id,omitempty"`
}
type GroupSpec struct {
	ID          string                  `json:"id"`
	Placement   *topology.PlacementRule `json:"placement,omitempty"`
	Nodes       []string                `json:"nodes,omitempty"`
	VoterPolicy *topology.VoterPolicy   `json:"voter_policy,omitempty"`
	Hosting     *topology.HostingRule   `json:"hosting,omitempty"`
}
type RetireOptions struct {
	Reason   string          `json:"reason"`
	Mutation MutationOptions `json:"mutation"`
}

type documentEdit func(*topology.Document, time.Time) error

func (g *Group) mutation(ctx context.Context, o MutationOptions, comment string, checkHealth bool, edit documentEdit) (MutationResult, error) {
	base, next, err := g.computeMutation(ctx, o, comment, checkHealth, edit)
	if err != nil {
		return MutationResult{}, err
	}
	if o.Preview {
		return MutationResult{Document: next}, nil
	}
	if err := g.ensureRunning(ctx); err != nil {
		return MutationResult{}, err
	}
	doc, err := g.Stage(ctx, next, base.Version)
	result := MutationResult{Document: doc}
	if err != nil || !o.Apply {
		return result, err
	}
	result.OperatorID, err = g.Apply(ctx, doc.Version)
	return result, err
}
func (g *Group) computeMutation(ctx context.Context, o MutationOptions, comment string, checkHealth bool, edit documentEdit) (topology.Document, topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, topology.Document{}, err
	}
	if o.Apply && o.Preview {
		return topology.Document{}, topology.Document{}, fault.New(fault.Invalid, "preview cannot approve a mutation", nil)
	}
	base := g.state().Latest()
	if base.Version == 0 || base.Version == math.MaxUint64 {
		return base, topology.Document{}, fault.ErrUnavailable
	}
	if o.ExpectedVersion != 0 && o.ExpectedVersion != base.Version {
		return base, topology.Document{}, &fault.Error{Code: fault.VersionConflict, ExpectedVersion: o.ExpectedVersion, CurrentVersion: base.Version}
	}
	next := base.Clone()
	next.Audit = nil
	now := time.Now().UTC()
	if err := edit(&next, now); err != nil {
		return base, topology.Document{}, err
	}
	for i := range next.Groups {
		group := &next.Groups[i]
		if old := base.Group(group.ID); old != nil && (!reflect.DeepEqual(old.Replicas, group.Replicas) || !reflect.DeepEqual(old.Placement, group.Placement) || !reflect.DeepEqual(old.VoterPolicy, group.VoterPolicy) || !reflect.DeepEqual(old.Hosting, group.Hosting) || old.NextReplicaID != group.NextReplicaID) {
			group.UpdatedAt = now
		}
	}
	next.Version = base.Version + 1
	next.ParentVersion = base.Version
	next.Origin = topology.OriginOperator
	next.ChangedBy = o.Actor
	if next.ChangedBy == "" {
		next.ChangedBy = g.currentIdentity().NodeName
	}
	next.Comment = o.Comment
	if next.Comment == "" {
		next.Comment = comment
	}
	next.CreatedAt = now
	if err := next.Validate(g.rules); err != nil {
		return base, topology.Document{}, err
	}
	if err := checkLoadBounds(next); err != nil {
		return base, topology.Document{}, err
	}
	if err := g.state().ValidateDesired(next); err != nil {
		return base, topology.Document{}, err
	}
	if checkHealth {
		if err := g.checkMutationHealth(ctx, base, next); err != nil {
			return base, topology.Document{}, err
		}
	}
	return base, next, nil
}
func checkLoadBounds(doc topology.Document) error {
	for _, group := range doc.LoadGroups() {
		lo, hi := doc.GroupBounds(group.ID)
		count := len(doc.VotersOf(group.ID))
		if count < lo || count > hi {
			return fault.New(fault.Invalid, fmt.Sprintf("group %q would have %d voters outside [%d,%d]", group.ID, count, lo, hi), nil)
		}
	}
	return nil
}
func (g *Group) checkMutationHealth(ctx context.Context, before, after topology.Document) error {
	if g.opts.Health == nil {
		return nil
	} // Unknown health cannot judge a staged declaration.
	health, err := g.opts.Health.Snapshot(ctx)
	if err != nil {
		return err
	}
	if health == nil {
		return nil
	}
	for _, group := range after.Groups {
		a, b := before.VotersOf(group.ID), after.VotersOf(group.ID)
		unchanged := len(a) == len(b)
		for _, r := range a {
			unchanged = unchanged && slices.ContainsFunc(b, func(next topology.Replica) bool { return next.ReplicaID == r.ReplicaID && next.NodeName == r.NodeName })
		}
		if unchanged {
			continue
		}
		alive := 0
		for _, r := range b {
			if health[r.NodeName] == topology.HealthAlive {
				alive++
			}
		}
		if alive < topology.QuorumOf(len(b)) {
			return fault.New(fault.UnsafeMembership, fmt.Sprintf("group %q would retain %d live voters of %d", group.ID, alive, len(b)), nil)
		}
	}
	return nil
}
func (g *Group) enforcePlacement(doc *topology.Document, now time.Time) error {
	if err := topology.ValidateHostingCapacity(*doc, g.rules); err != nil {
		return err
	}
	for i := range doc.Groups {
		group := &doc.Groups[i]
		if group.ID == topology.MetaGroupID {
			continue
		}
		if group.VoterPolicy != nil {
			if err := materializeHosting(doc, group, now); err != nil {
				return err
			}
			continue
		}
		rule := doc.GroupPlacement(group.ID)
		voters := len(doc.VotersOf(group.ID))
		for _, node := range doc.Nodes {
			if voters >= rule.MaxReplicas || len(group.Replicas) >= g.rules.Limits.MaxReplicas {
				break
			}
			if node.State != topology.NodeActive || !rule.Allows(node.Type) || slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool { return r.NodeName == node.Name }) {
				continue
			}
			id, err := group.AllocateReplicaID()
			if err != nil {
				return err
			}
			group.Replicas = append(group.Replicas, topology.Replica{NodeName: node.Name, ReplicaID: id, Role: topology.ReplicaVoter, CreatedAt: now, UpdatedAt: now})
			voters++
		}
	}
	return nil
}
func removeNodeReplicas(doc *topology.Document, name string, includeMeta bool) {
	for i := range doc.Groups {
		group := &doc.Groups[i]
		if group.ID == topology.MetaGroupID && !includeMeta {
			continue
		}
		group.Replicas = slices.DeleteFunc(group.Replicas, func(r topology.Replica) bool { return r.NodeName == name })
	}
}
func unknownNode(name string) error {
	return fault.New(fault.Invalid, fmt.Sprintf("unknown node %q", name), nil)
}
func unknownGroup(id string) error {
	return fault.New(fault.Invalid, fmt.Sprintf("unknown group %q", id), nil)
}

// AddNode admits a caller-defined type. Every admitted node joins _meta;
// placement eligibility controls only its load groups. Excluded names get fresh IDs.
func (g *Group) AddNode(ctx context.Context, node topology.Node, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, fmt.Sprintf("add node %q", node.Name), false, func(doc *topology.Document, now time.Time) error {
		if strings.TrimSpace(node.Name) == "" || strings.TrimSpace(string(node.Type)) == "" {
			return fault.ErrInvalid
		}
		next := topology.Document{Nodes: []topology.Node{node}}.Clone().Nodes[0]
		next.State = topology.NodeActive
		next.CreatedAt = now
		next.UpdatedAt = now
		if existing := doc.Node(node.Name); existing != nil {
			if existing.State != topology.NodeExcluded {
				return fault.New(fault.Invalid, "node already admitted", nil)
			}
			*existing = next
		} else {
			doc.Nodes = append(doc.Nodes, next)
		}
		id, err := doc.AllocateMemberID()
		if err != nil {
			return err
		}
		if err = doc.SetMemberID(node.Name, id, now); err != nil {
			return err
		}
		return g.enforcePlacement(doc, now)
	})
}

// ReadmitNode rotates the meta and load identities of a declared active node
// observed dead. It preserves its type, placement and other nodes' identities.
// Latest, Target and Active must already agree; approval remains an ordinary
// learner-first execution that can wait for the returning process. The caller
// restarts that process with Join and an exact OnReadmission quarantine hook.
func (g *Group) ReadmitNode(ctx context.Context, name string, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, fmt.Sprintf("readmit node %q", name), false, func(doc *topology.Document, now time.Time) error {
		node := doc.Node(name)
		if node == nil {
			return unknownNode(name)
		}
		if node.State != topology.NodeActive || doc.MemberID(name) == 0 {
			return fault.New(fault.Invalid, "readmission requires an admitted active node", nil)
		}
		_, target, active := g.state().Views()
		if doc.Version != target.Version || doc.Version != active.Version {
			return fault.New(fault.OperatorAuthorization, "readmission requires converged topology; apply or restore pending intent first", nil)
		}
		health, err := (observationSource{g}).Health(ctx, active)
		if err != nil {
			return err
		}
		switch health[name] {
		case topology.HealthDead:
		case topology.HealthAlive:
			return fault.New(fault.UnsafeMembership, "cannot readmit a live node", nil)
		default:
			return fault.New(fault.Unavailable, "readmission requires known dead node health", nil)
		}
		fresh, err := metaengine.ReadmissionDocument(*doc, name, now)
		if err != nil {
			return err
		}
		*doc = fresh
		return nil
	})
}
func (g *Group) DrainNode(ctx context.Context, name string, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, fmt.Sprintf("drain node %q", name), false, func(doc *topology.Document, now time.Time) error {
		node := doc.Node(name)
		if node == nil {
			return unknownNode(name)
		}
		if node.State != topology.NodeActive {
			active := g.state().Active()
			n := active.Node(name)
			if (node.State == topology.NodeDraining || node.State == topology.NodeDrained) && doc.Version > active.Version && (n == nil || n.State == topology.NodeActive) {
				return fault.New(fault.Invalid, fmt.Sprintf("drain of %q already staged as v%d; apply it or activate the node to undo", name, doc.Version), nil)
			}
			return fault.New(fault.Invalid, fmt.Sprintf("node %q is %s; activate to return it to service or exclude to retire it", name, node.State), nil)
		}
		node.State = topology.NodeDraining
		node.UpdatedAt = now
		removeNodeReplicas(doc, name, false)
		return nil
	})
}
func (g *Group) ActivateNode(ctx context.Context, name string, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, fmt.Sprintf("activate node %q", name), false, func(doc *topology.Document, now time.Time) error {
		node := doc.Node(name)
		if node == nil {
			return unknownNode(name)
		}
		if node.State == topology.NodeActive {
			return nil
		}
		if node.State != topology.NodeDraining && node.State != topology.NodeDrained {
			return fault.New(fault.Invalid, "excluded node needs fresh admission", nil)
		}
		node.State = topology.NodeActive
		node.UpdatedAt = now
		return g.enforcePlacement(doc, now)
	})
}
func (g *Group) ExcludeNode(ctx context.Context, name string, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, fmt.Sprintf("exclude node %q", name), true, func(doc *topology.Document, now time.Time) error {
		node := doc.Node(name)
		if node == nil {
			return unknownNode(name)
		}
		active := g.state().Active()
		old := active.Node(name)
		if old != nil && old.State != topology.NodeDraining && old.State != topology.NodeDrained {
			return fault.New(fault.Invalid, "node must have a converged drain before retirement", nil)
		}
		if old == nil {
			target := g.state().Target()
			if admitted := target.Node(name); admitted != nil && admitted.State.Places() {
				return fault.New(fault.Invalid, "approved admission must converge before normal retirement", nil)
			}
			removeNodeReplicas(doc, name, false)
		} else if node.State != topology.NodeDraining && node.State != topology.NodeDrained {
			return fault.ErrInvalid
		}
		if node.State == topology.NodeExcluded {
			return fault.New(fault.Invalid, "node already excluded", nil)
		}
		for _, group := range doc.LoadGroups() {
			for _, r := range group.Replicas {
				if r.NodeName == name {
					return fault.New(fault.Invalid, "node still holds a load replica", nil)
				}
			}
		}
		node.State = topology.NodeExcluded
		node.UpdatedAt = now
		removeNodeReplicas(doc, name, true)
		return nil
	})
}
func (g *Group) ForceRetireNode(ctx context.Context, name string, o RetireOptions) (MutationResult, error) {
	if strings.TrimSpace(o.Reason) == "" || strings.TrimSpace(o.Mutation.Actor) == "" {
		return MutationResult{}, fault.New(fault.Invalid, "forced retirement requires reason and actor", nil)
	}
	return g.mutation(ctx, o.Mutation, fmt.Sprintf("force retire node %q", name), false, func(doc *topology.Document, now time.Time) error {
		node := doc.Node(name)
		if node == nil {
			return unknownNode(name)
		}
		if node.State == topology.NodeExcluded {
			return fault.ErrInvalid
		}
		node.State = topology.NodeExcluded
		node.UpdatedAt = now
		removeNodeReplicas(doc, name, true)
		if err := g.enforcePlacement(doc, now); err != nil {
			return err
		}
		doc.Audit = &topology.ChangeAudit{Operation: topology.AuditOperationForceRetireNode, TargetNode: name, Forced: true, Reason: strings.TrimSpace(o.Reason), StagedBy: o.Mutation.Actor, StagedAt: now}
		return nil
	})
}
func (g *Group) SetPlacement(ctx context.Context, rule topology.PlacementRule, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, "set placement", true, func(doc *topology.Document, now time.Time) error {
		doc.Placement = rule
		doc.Placement.AllowedNodeTypes = slices.Clone(rule.AllowedNodeTypes)
		return g.enforcePlacement(doc, now)
	})
}

// SetGroupPlacement edits one load group's policy and desired replicas in one
// CAS document. Eligible existing identities are retained; new hosts get fresh
// identities. Approval still checks actual membership and live-majority safety.
func (g *Group) SetGroupPlacement(ctx context.Context, id string, rule topology.PlacementRule, o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, fmt.Sprintf("set placement of group %q", id), true, func(doc *topology.Document, now time.Time) error {
		group := doc.Group(id)
		if group == nil {
			return unknownGroup(id)
		}
		if rule.MinReplicas < 1 || rule.MaxReplicas < rule.MinReplicas || rule.MaxReplicas > g.rules.Limits.MaxReplicas {
			return fault.ErrInvalid
		}
		policy := rule
		policy.AllowedNodeTypes = slices.Clone(rule.AllowedNodeTypes)
		group.Placement = &policy
		if group.VoterPolicy != nil {
			return g.enforcePlacement(doc, now)
		}
		voters := 0
		group.Replicas = slices.DeleteFunc(group.Replicas, func(replica topology.Replica) bool {
			node := doc.Node(replica.NodeName)
			if node == nil || node.State != topology.NodeActive || !policy.Allows(node.Type) {
				return true
			}
			if replica.Role == topology.ReplicaVoter {
				if voters >= policy.MaxReplicas {
					return true
				}
				voters++
			}
			return false
		})
		for _, node := range doc.Nodes {
			if voters >= policy.MaxReplicas || len(group.Replicas) >= g.rules.Limits.MaxReplicas {
				break
			}
			if node.State != topology.NodeActive || !policy.Allows(node.Type) || slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool { return r.NodeName == node.Name }) {
				continue
			}
			fresh, err := group.AllocateReplicaID()
			if err != nil {
				return err
			}
			group.Replicas = append(group.Replicas, topology.Replica{NodeName: node.Name, ReplicaID: fresh, Role: topology.ReplicaVoter, CreatedAt: now, UpdatedAt: now})
			voters++
		}
		if voters < policy.MinReplicas {
			return fault.New(fault.UnsafeMembership, "too few eligible active voters for group placement", nil)
		}
		return nil
	})
}
func (g *Group) CreateLoadGroup(ctx context.Context, spec GroupSpec, o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(spec.ID); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, fmt.Sprintf("create group %q", spec.ID), true, func(doc *topology.Document, now time.Time) error {
		return g.createLoadGroup(doc, spec, now)
	})
}
func (g *Group) DeleteLoadGroup(ctx context.Context, id string, o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, fmt.Sprintf("delete group %q", id), false, func(doc *topology.Document, _ time.Time) error {
		if doc.Group(id) == nil {
			return unknownGroup(id)
		}
		doc.Groups = slices.DeleteFunc(doc.Groups, func(group topology.Group) bool { return group.ID == id })
		return nil
	})
}
func (g *Group) ChangeReplica(ctx context.Context, id, name string, role topology.ReplicaRole, o MutationOptions) (MutationResult, error) {
	if role != topology.ReplicaVoter && role != topology.ReplicaLearner {
		return MutationResult{}, fault.ErrInvalid
	}
	return g.mutation(ctx, o, "change replica role", true, func(doc *topology.Document, now time.Time) error {
		group := doc.Group(id)
		if group == nil {
			return unknownGroup(id)
		}
		for i := range group.Replicas {
			r := &group.Replicas[i]
			if r.NodeName == name {
				pin := group.VoterPolicy != nil && role == topology.ReplicaLearner
				if r.Role != role || r.PinnedLearner != pin {
					r.Role = role
					r.PinnedLearner = pin
					r.UpdatedAt = now
				}
				return nil
			}
		}
		return unknownNode(name)
	})
}
func (g *Group) RollbackTo(ctx context.Context, version uint64, o MutationOptions) (MutationResult, error) {
	old, err := g.Get(ctx, version)
	if err != nil {
		return MutationResult{}, err
	}
	base, next, err := g.computeMutation(ctx, o, fmt.Sprintf("rollback to version %d", version), true, func(doc *topology.Document, _ time.Time) error {
		if doc.Version == version {
			return fault.New(fault.Invalid, "version is already current", nil)
		}
		floors := g.state().AllocationFloors()
		for i := range old.Groups {
			old.Groups[i].NextReplicaID = max(old.Groups[i].NextReplicaID, floors[old.Groups[i].ID])
		}
		for i := range old.Groups {
			if group := doc.Group(old.Groups[i].ID); group != nil {
				old.Groups[i].NextReplicaID = max(old.Groups[i].NextReplicaID, group.NextReplicaID)
			}
		}
		*doc = old.Clone()
		return nil
	})
	if err != nil {
		return MutationResult{}, err
	}
	next.ParentVersion = version
	if o.Preview {
		return MutationResult{Document: next}, nil
	}
	doc, err := g.Stage(ctx, next, base.Version)
	result := MutationResult{Document: doc}
	if err != nil || !o.Apply {
		return result, err
	}
	result.OperatorID, err = g.Apply(ctx, doc.Version)
	return result, err
}

// ReseatReplica atomically commits and approves a fresh-ID repair against equal
// Latest/Target/Active views. Preview computes it without approval. Apply is
// unnecessary for this operation: a real reseat always returns an execution.
func (g *Group) ReseatReplica(ctx context.Context, id, name string, o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	latest, target, active := g.state().Views()
	if !reflect.DeepEqual(latest, target) || !reflect.DeepEqual(target, active) {
		return MutationResult{}, fault.New(fault.OperatorAuthorization, "reseat requires equal Latest, Target and Active", nil)
	}
	if ex, ok := g.state().CurrentOperator(); ok && !ex.Status.State.Terminal() {
		return MutationResult{}, fault.New(fault.OperatorAuthorization, "reseat requires no running execution", nil)
	}
	base, next, err := g.computeMutation(ctx, o, "reseat replica", false, func(doc *topology.Document, now time.Time) error {
		group := doc.Group(id)
		if group == nil {
			return unknownGroup(id)
		}
		for _, r := range group.Replicas {
			if r.NodeName != name {
				continue
			}
			actor := o.Actor
			if actor == "" {
				actor = g.currentIdentity().NodeName
			}
			repair, _, err := metaengine.ReplicaRepairDocument(active, latest, id, name, r.ReplicaID, now, topology.OriginOperator, actor)
			if err != nil {
				return err
			}
			*doc = repair
			return nil
		}
		return unknownNode(name)
	})
	if err != nil {
		return MutationResult{}, err
	}
	if o.Preview {
		return MutationResult{Document: next}, nil
	}
	plan, err := g.PlanFor(ctx, next)
	if err != nil {
		return MutationResult{}, err
	}
	if plan.IsEmpty() {
		return MutationResult{}, fault.ErrOperatorAuthorization
	}
	committed, err := g.propose(ctx, metaengine.CommitRepairOperatorProposal{Doc: next, ExpectedVersion: base.Version, Plan: plan, At: time.Now().UTC()})
	result := MutationResult{Document: committed.Doc, OperatorID: committed.OperatorID}
	if err == nil {
		g.controller.Signal()
	}
	return result, err
}

// TransferLeader uses the exact locally applied Active identity without creating
// a document version. Load transport/effects remain caller-owned.
func (g *Group) TransferLeader(ctx context.Context, id, name string) error {
	if err := g.ensureRunning(ctx); err != nil {
		return err
	}
	doc := g.state().Active()
	group := doc.Group(id)
	if group == nil {
		return unknownGroup(id)
	}
	node := doc.Node(name)
	if node == nil {
		return unknownNode(name)
	}
	if node.State != topology.NodeActive {
		return fault.New(fault.Invalid, "leadership target must be active", nil)
	}
	var member uint64
	for _, r := range group.Replicas {
		if r.NodeName == name && r.Role == topology.ReplicaVoter {
			member = r.ReplicaID
		}
	}
	if member == 0 {
		return fault.New(fault.Invalid, "leadership target must be a declared voter", nil)
	}
	if g.opts.Health != nil {
		health, err := g.opts.Health.Snapshot(ctx)
		if err != nil {
			return err
		}
		if health != nil && health[name] != topology.HealthAlive {
			return fault.New(fault.UnsafeMembership, "leadership target is not alive", nil)
		}
	}
	a := topology.AuthorizedStep{Version: doc.Version, Kind: topology.StepTransferLeader, GroupID: id, NodeName: name, ReplicaID: member}
	if id == topology.MetaGroupID && !g.IsLeader() {
		_, err := g.callLeader(ctx, "MetaControl", toWireAuthorization(a))
		return err
	}
	return (groupControllers{g}).TransferLeader(ctx, a)
}
