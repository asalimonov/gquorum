package metaservice

import (
	"context"
	"fmt"
	"maps"
	"math"
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

// MutationResult preserves a staged document, optional operation and placement
// diagnostics.
type MutationResult struct {
	OperationID topology.OperationID      `json:"operation_id,omitempty"`
	RebasedTo   uint64                    `json:"rebased_to,omitempty"`
	Document    topology.Document         `json:"document"`
	Placement   *topology.PlacementReport `json:"placement,omitempty"`
}

// GroupSpec describes initial load group placement, storage, hosting and voter
// configuration.
type GroupSpec struct {
	ID          string                  `json:"id"`
	Labels      map[string]string       `json:"labels,omitempty"`
	Placement   *topology.PlacementRule `json:"placement,omitempty"`
	Nodes       []string                `json:"nodes,omitempty"`
	VoterConfig *topology.VoterConfig   `json:"voter_config,omitempty"`
	Hosting     *topology.HostingRule   `json:"hosting,omitempty"`
	Storage     topology.StorageClass   `json:"storage,omitempty"`
}

type documentEdit func(*topology.Document, time.Time) error

type mutationPolicy struct {
	Kind        string
	Input       mutationInput
	Comment     string
	CheckHealth bool
}

type mutationDocuments struct {
	Replay     *metaengine.ApplyResult
	Intent     *metaengine.Intent
	Base, Next topology.Document
}

// preparePlacementEdit completes a declarative edit against the approved
// reservation reference. The caller captures before from the same owned Latest
// document it is editing; Stage/Approve still revalidate replicated state.
func (g *Group) preparePlacementEdit(before topology.Document, doc *topology.Document, now time.Time) error {
	views := g.state().Views()
	target := views.Target
	if doc.ChangedBy == "" {
		doc.ChangedBy = g.currentIdentity().NodeName
	}
	preparation, err := topology.PreparePlacement(topology.ReadView{Latest: before, Target: target}, *doc, g.rules, now)
	prepared := preparation.Document
	if err != nil {
		return err
	}
	*doc = prepared
	return nil
}

func (g *Group) mutation(ctx context.Context, o MutationOptions, policy mutationPolicy,
	edit documentEdit) (MutationResult, error) {
	documents, err := g.computeMutation(ctx, o, policy, edit)
	base, next := documents.Base, documents.Next
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, err
	}
	if documents.Replay != nil {
		return mutationReplayResult(*documents.Replay), nil
	}
	report, err := placementReportFor(g.state().Target(), next, topology.AffectedPlacementGroups(base, next))
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, err
	}
	if o.Preview {
		return MutationResult{Document: next, Placement: report}, nil
	}
	if err := g.ensureRunning(ctx); err != nil {
		return MutationResult{}, err
	}
	documents.Next = next
	accepted, err := g.stageMutation(ctx, documents)
	result := mutationReplayResult(accepted)
	result.Placement = report
	if err != nil || !o.Apply {
		return result.outcome(err)
	}
	result.OperationID, err = g.Approve(ctx, result.Document.Version)
	return result.outcome(err)
}

func (g *Group) computeMutation(ctx context.Context, o MutationOptions, policy mutationPolicy,
	edit documentEdit) (mutationDocuments, error) {
	if err := ctx.Err(); err != nil {
		return mutationDocuments{}, err
	}
	documents, err := g.mutationBase(o, policy)
	if err != nil || documents.Replay != nil {
		return documents, err
	}
	base, intent := documents.Base, documents.Intent

	next := base.Clone()
	next.Audit = nil
	now := time.Now().UTC()
	if err := edit(&next, now); err != nil {
		return mutationDocuments{Base: base}, err
	}
	stampMutationGroups(base, &next, now)

	next.Version = base.Version + 1
	next.ParentVersion = base.Version
	next.RebasedFrom = 0
	next.Origin = topology.OriginOperator
	next.ChangedBy = o.Actor
	if next.ChangedBy == "" {
		next.ChangedBy = g.currentIdentity().NodeName
	}
	next.Comment = o.Comment
	if next.Comment == "" {
		next.Comment = policy.Comment
	}
	next.CreatedAt = now
	if err := next.Validate(g.rules); err != nil {
		return mutationDocuments{Base: base}, err
	}
	if err := checkLoadBounds(next); err != nil {
		return mutationDocuments{Base: base}, err
	}
	if err := g.state().ValidateDesired(next); err != nil {
		return mutationDocuments{Base: base}, err
	}
	if policy.CheckHealth {
		if err := g.checkMutationHealth(ctx, base, next); err != nil {
			return mutationDocuments{Base: base}, err
		}
	}
	return mutationDocuments{Base: base, Next: next, Intent: intent}, nil
}
func checkLoadBounds(doc topology.Document) error {
	for _, group := range doc.LoadGroups() {
		lo, hi := doc.GroupBounds(group.ID)
		count := len(doc.VotersOf(group.ID))
		if count < lo || count > hi {
			return fault.New(fault.Invalid,
				fmt.Sprintf("group %q would have %d voters outside [%d,%d]", group.ID, count, lo, hi), nil)
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
			unchanged = unchanged &&
				slices.ContainsFunc(b, func(next topology.Replica) bool {
					return next.ReplicaID == r.ReplicaID && next.NodeName == r.NodeName
				})
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
			return fault.New(fault.UnsafeMembership, fmt.Sprintf("group %q would retain %d live voters of %d", group.ID,
				alive, len(b)), nil)
		}
	}
	return nil
}

type replicaRemovalScope uint8

const (
	loadReplicaRemoval replicaRemovalScope = iota
	allReplicaRemoval
)

func removeNodeReplicas(doc *topology.Document, name string, scope replicaRemovalScope) {
	for i := range doc.Groups {
		group := &doc.Groups[i]
		if group.ID == topology.MetaGroupID && scope != allReplicaRemoval {
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
// placement eligibility controls only its load groups. Excluded names get fresh
// IDs.
func (g *Group) AddNode(ctx context.Context, node topology.Node, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, mutationPolicy{Kind: "add_node",
		Input:   mutationInput{NodeName: node.Name, Value: node},
		Comment: fmt.Sprintf("add node %q", node.Name)},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
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
			if topology.NodeLearnerOnly(next) {
				setMetaLearner(doc, node.Name)
			}
			return g.preparePlacementEdit(before, doc, now)
		})
}

// ReadmitNode rotates the meta and load identities of a declared active node
// observed dead. It preserves its type, placement and other nodes' identities.
// Every affected group and the node's exact prior member must be converged and
// unowned; unrelated groups may continue converging independently. Approval
// remains an ordinary learner-first execution that can wait for the returning
// process. The caller restarts with Join and an exact OnReadmission hook.
func (g *Group) ReadmitNode(ctx context.Context, name string, expectedMemberID uint64,
	o MutationOptions) (MutationResult, error) {
	if expectedMemberID == 0 {
		return MutationResult{}, fault.ErrInvalid
	}
	return g.mutation(ctx, o, mutationPolicy{Kind: "readmit_node",
		Input:   mutationInput{NodeName: name, PreviousID: expectedMemberID},
		Comment: fmt.Sprintf("readmit node %q", name)},
		func(doc *topology.Document, now time.Time) error {
			if err := g.validateReadmissionScope(ctx, *doc, name, expectedMemberID); err != nil {
				return err
			}
			target := g.state().Target()
			health, err := (observationSource{g}).Health(ctx, target)
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

// DrainNode stages a node drain through guarded topology mutation.
func (g *Group) DrainNode(ctx context.Context, name string, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, mutationPolicy{Kind: "drain_node",
		Input:   mutationInput{NodeName: name},
		Comment: fmt.Sprintf("drain node %q", name)},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
			node := doc.Node(name)
			if node == nil {
				return unknownNode(name)
			}
			if node.State != topology.NodeActive {
				return g.validateDrainRetry(doc, node)
			}
			node.State = topology.NodeDraining
			node.UpdatedAt = now
			return g.preparePlacementEdit(before, doc, now)
		})
}

func (g *Group) validateDrainRetry(doc *topology.Document, node *topology.Node) error {
	page, err := g.state().Progress(topology.ProgressQuery{NodeName: node.Name})
	if err != nil {
		return err
	}
	var converged *topology.NodeProgress
	for i := range page.Nodes {
		if page.Nodes[i].NodeName == node.Name {
			converged = &page.Nodes[i]
			break
		}
	}
	var sourceVersion uint64
	if converged != nil {
		sourceVersion = converged.ConvergedVersion
	}
	if (node.State == topology.NodeDraining || node.State == topology.NodeDrained) &&
		doc.Version > sourceVersion && (converged == nil || converged.Converged == nil ||
		converged.Converged.State == topology.NodeActive) {
		return fault.New(fault.Invalid,
			fmt.Sprintf("drain of %q already staged as v%d; apply it or activate the node to undo", node.Name,
				doc.Version), nil)
	}
	return fault.New(fault.Invalid,
		fmt.Sprintf("node %q is %s; activate to return it to service or exclude to retire it", node.Name,
			node.State), nil)
}

// ActivateNode stages reactivation of an eligible declared node.
func (g *Group) ActivateNode(ctx context.Context, name string, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, mutationPolicy{Kind: "activate_node",
		Input:   mutationInput{NodeName: name},
		Comment: fmt.Sprintf("activate node %q", name)},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
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
			return g.preparePlacementEdit(before, doc, now)
		})
}

// ExcludeNode stages removal of a node from placement under topology safety
// checks.
func (g *Group) ExcludeNode(ctx context.Context, name string, o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, mutationPolicy{Kind: "exclude_node",
		Input:   mutationInput{NodeName: name},
		Comment: fmt.Sprintf("exclude node %q", name), CheckHealth: true},
		func(doc *topology.Document, now time.Time) error {
			node := doc.Node(name)
			if node == nil {
				return unknownNode(name)
			}
			if err := g.prepareNormalRetirement(doc, node, name); err != nil {
				return err
			}
			if node.State == topology.NodeExcluded {
				return fault.New(fault.Invalid, "node already excluded", nil)
			}
			if hasNodeLoadReplica(*doc, name) {
				return fault.New(fault.Invalid, "node still holds a load replica", nil)
			}
			node.State = topology.NodeExcluded
			node.UpdatedAt = now
			removeNodeReplicas(doc, name, allReplicaRemoval)
			return nil
		})
}

// ForceRetireNode stages explicit audited retirement with mandatory membership
// and placement checks.
func (g *Group) ForceRetireNode(ctx context.Context, name string, o RetireOptions) (MutationResult, error) {
	if strings.TrimSpace(o.Reason) == "" || strings.TrimSpace(o.Mutation.Actor) == "" {
		return MutationResult{}, fault.New(fault.Invalid, "forced retirement requires reason and actor", nil)
	}
	return g.mutation(ctx, o.Mutation, mutationPolicy{Kind: "force_retire_node",
		Input: mutationInput{NodeName: name, Value: topology.ChangeAudit{
			Operation: topology.AuditOperationForceRetireNode, TargetNode: name, Forced: true,
			Reason: o.Reason, StagedBy: o.Mutation.Actor}},
		Comment: fmt.Sprintf("force retire node %q", name)},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
			node := doc.Node(name)
			if node == nil {
				return unknownNode(name)
			}
			if node.State == topology.NodeExcluded {
				return fault.ErrInvalid
			}
			// Prepare the load relocation while _meta still contains the
			// departing
			// member. Its removal is the exact expert membership edit below.
			node.State = topology.NodeDraining
			node.UpdatedAt = now
			if err := g.preparePlacementEdit(before, doc, now); err != nil {
				return err
			}
			doc.Node(name).State = topology.NodeExcluded
			removeNodeReplicas(doc, name, allReplicaRemoval)
			doc.Audit = &topology.ChangeAudit{
				Operation:  topology.AuditOperationForceRetireNode,
				TargetNode: name,
				Forced:     true,
				Reason:     strings.TrimSpace(o.Reason),
				StagedBy:   o.Mutation.Actor,
				StagedAt:   now,
			}
			return nil
		},
	)
}

// SetPlacement stages cluster or group placement intent under compare-and-swap.
func (g *Group) SetPlacement(ctx context.Context, rule topology.PlacementRule,
	o MutationOptions) (MutationResult, error) {
	return g.mutation(ctx, o, mutationPolicy{Kind: "set_placement",
		Input:   mutationInput{Value: rule},
		Comment: "set placement", CheckHealth: true},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
			for _, group := range before.LoadGroups() {
				if group.Placement == nil && group.VoterConfig == nil &&
					len(before.VotersOf(group.ID)) > rule.MaxReplicas {
					return fault.New(fault.Invalid, "placement cannot silently shrink legacy voters", nil)
				}
			}
			doc.Placement = rule
			doc.Placement.AllowedNodeTypes = slices.Sorted(slices.Values(rule.AllowedNodeTypes))
			doc.Placement.NodeSelector = maps.Clone(rule.NodeSelector)
			return g.preparePlacementEdit(before, doc, now)
		})
}

// SetGroupPlacement edits one load group's placement rule and desired replicas
// in one
// CAS document. Eligible existing identities are retained; new hosts get fresh
// identities. Approval still checks actual membership and live-majority safety.
func (g *Group) SetGroupPlacement(ctx context.Context, id string, rule topology.PlacementRule,
	o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, mutationPolicy{Kind: "set_group_placement",
		Input:   mutationInput{GroupID: id, Value: rule},
		Comment: fmt.Sprintf("set placement of group %q", id), CheckHealth: true},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
			group := doc.Group(id)
			if group == nil {
				return unknownGroup(id)
			}
			if rule.MinReplicas < 1 || rule.MaxReplicas < rule.MinReplicas ||
				rule.MaxReplicas > g.rules.Limits.MaxReplicas {
				return fault.ErrInvalid
			}
			placement := rule
			placement.AllowedNodeTypes = slices.Sorted(slices.Values(rule.AllowedNodeTypes))
			placement.NodeSelector = maps.Clone(rule.NodeSelector)
			group.Placement = &placement
			return g.preparePlacementEdit(before, doc, now)
		})
}

// CreateLoadGroup stages a fresh load group with validated placement and
// monotonic identity allocation.
func (g *Group) CreateLoadGroup(ctx context.Context, spec GroupSpec, o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(spec.ID); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, mutationPolicy{Kind: "create_group",
		Input:   mutationInput{Value: spec},
		Comment: fmt.Sprintf("create group %q", spec.ID), CheckHealth: true},
		func(doc *topology.Document, now time.Time) error {
			before := doc.Clone()
			if err := createLoadGroup(doc, spec, now); err != nil {
				return err
			}
			return g.preparePlacementEdit(before, doc, now)
		})
}

// DeleteLoadGroup stages group deletion with retained cleanup authorization.
func (g *Group) DeleteLoadGroup(ctx context.Context, id string, o MutationOptions) (MutationResult, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	return g.mutation(ctx, o, mutationPolicy{Kind: "delete_group",
		Input:   mutationInput{GroupID: id},
		Comment: fmt.Sprintf("delete group %q", id)},
		func(doc *topology.Document, _ time.Time) error {
			if doc.Group(id) == nil {
				return unknownGroup(id)
			}
			doc.Groups = slices.DeleteFunc(doc.Groups, func(group topology.Group) bool { return group.ID == id })
			return nil
		})
}

// ChangeReplica stages a guarded exact replica role change.
func (g *Group) ChangeReplica(ctx context.Context, id string, change topology.ReplicaRoleChange,
	o MutationOptions) (MutationResult, error) {
	name, role := change.NodeName, change.Role
	if role != topology.ReplicaVoter && role != topology.ReplicaLearner {
		return MutationResult{}, fault.ErrInvalid
	}
	return g.mutation(ctx, o, mutationPolicy{Kind: "change_replica",
		Input:   mutationInput{GroupID: id, Value: change},
		Comment: "change replica role", CheckHealth: true},
		func(doc *topology.Document, now time.Time) error {
			group := doc.Group(id)
			if group == nil {
				return unknownGroup(id)
			}
			for i := range group.Replicas {
				r := &group.Replicas[i]
				if r.NodeName == name {
					pin := group.VoterConfig != nil && role == topology.ReplicaLearner
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

// RollbackTo stages a retained version's intent as a new validated topology
// version.
func (g *Group) RollbackTo(ctx context.Context, version uint64, o MutationOptions) (MutationResult, error) {
	policy := mutationPolicy{Kind: "rollback_to",
		Input:   mutationInput{Version: version},
		Comment: fmt.Sprintf("rollback to version %d", version), CheckHealth: true}
	documents, err := g.computeMutation(ctx, o, policy,
		func(doc *topology.Document, now time.Time) error {
			old, err := g.Get(ctx, version)
			if err != nil {
				return err
			}
			return g.prepareRollback(doc, old, version, now)
		},
	)
	base, next := documents.Base, documents.Next
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, err
	}
	if documents.Replay != nil {
		return mutationReplayResult(*documents.Replay), nil
	}
	next.ParentVersion = version
	report, err := placementReportFor(g.state().Target(), next, topology.AffectedPlacementGroups(base, next))
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, err
	}
	if o.Preview {
		return MutationResult{Document: next, Placement: report}, nil
	}
	documents.Next = next
	accepted, err := g.stageMutation(ctx, documents)
	result := mutationReplayResult(accepted)
	result.Placement = report
	if err != nil || !o.Apply {
		return result.outcome(err)
	}
	result.OperationID, err = g.Approve(ctx, result.Document.Version)
	return result.outcome(err)
}

// ReseatReplica atomically approves a fresh-ID repair against the proven group.
// Independent staged intent is rebased; conflicting intent refuses the commit.
// Preview computes the repair without approval; Apply is unnecessary.
func (g *Group) ReseatReplica(ctx context.Context, id string, key topology.ReplicaKey,
	o MutationOptions) (MutationResult, error) {
	if key.ReplicaID == 0 || strings.TrimSpace(key.NodeName) == "" {
		return MutationResult{}, fault.ErrInvalid
	}
	name := key.NodeName
	if err := topology.ValidateLoadID(id); err != nil {
		return MutationResult{}, err
	}
	documents, err := g.computeMutation(ctx, o, mutationPolicy{Kind: "reseat_replica",
		Input:   mutationInput{GroupID: id, NodeName: name, PreviousID: key.ReplicaID},
		Comment: "reseat replica"},
		func(doc *topology.Document, now time.Time) error {
			options := metaengine.ReplicaRepairOptions{GroupID: id, NodeName: name, ReplicaID: key.ReplicaID, At: now,
				Origin: topology.OriginOperator, Actor: o.Actor}
			return g.prepareReseat(ctx, doc, options)
		})
	base, next := documents.Base, documents.Next
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, err
	}
	if documents.Replay != nil {
		return mutationReplayResult(*documents.Replay), nil
	}
	report, err := placementReportFor(g.state().Target(), next, topology.AffectedPlacementGroups(base, next))
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, err
	}
	if o.Preview {
		return MutationResult{Document: next, Placement: report}, nil
	}
	read, err := g.capturePlan(ctx, metaengine.QueryRequest{Kind: "repair_plan", Key: id}, g.readPlan)
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, fmt.Errorf("capture reseat scope %q: %w", id, err)
	}
	plan, err := (schedulingHost{g}).PlanRepair(ctx, read, next)
	if err != nil {
		return MutationResult{Placement: reportFromError(err)}, fmt.Errorf("plan reseat of %q: %w", id, err)
	}
	if len(plan.Steps) == 0 {
		return MutationResult{}, fault.New(fault.OperationAuthorization,
			fmt.Sprintf("reseat of %q produced no operation steps", id), nil)
	}
	committed, err := g.propose(ctx, metaengine.CommitRepairOperationProposal{
		Doc: next, Intent: documents.Intent, ExpectedVersion: read.Latest.Version,
		ExpectedTarget: read.Target.Version, Scopes: read.Scopes, Plan: plan,
		At: time.Now().UTC(), ProposalID: newProposalID(),
	})
	if err != nil {
		return MutationResult{Document: committed.Doc, OperationID: committed.OperationID, Placement: report},
			fmt.Errorf("commit reseat of %q: %w", id, err)
	}
	result := MutationResult{Document: committed.Doc, OperationID: committed.OperationID, Placement: report}
	if err == nil {
		g.controller.Signal()
	}
	return result.outcome(err)
}

// Pin zero CAS once, and resolve stale acceptance before invoking an edit.
func (g *Group) mutationBase(o MutationOptions, policy mutationPolicy) (mutationDocuments, error) {
	if o.Apply && o.Preview {
		return mutationDocuments{}, fault.New(fault.Invalid, "preview cannot approve a mutation",
			nil)
	}
	intent, err := g.mutationIntent(o, policy)
	if err != nil {
		return mutationDocuments{}, err
	}
	base := g.state().Latest()
	if base.Version == 0 {
		return mutationDocuments{Base: base}, fault.ErrUnavailable
	}
	if o.ExpectedVersion != 0 && o.ExpectedVersion != base.Version {
		if intent != nil && !o.Preview {
			replay, err := g.state().ReplayMutation(metaengine.ProposeDocument{
				ExpectedVersion: o.ExpectedVersion, Intent: intent})
			return mutationDocuments{Base: base, Next: replay.Doc, Intent: intent, Replay: &replay}, err
		}
		return mutationDocuments{Base: base}, &fault.Error{
			Code: fault.VersionConflict, ExpectedVersion: o.ExpectedVersion, CurrentVersion: base.Version,
		}
	}
	if base.Version == math.MaxUint64 {
		return mutationDocuments{Base: base}, fault.ErrUnavailable
	}
	if intent != nil {
		intent.ExpectedVersion, intent.Version = base.Version, base.Version+1
	}
	return mutationDocuments{Base: base, Intent: intent}, nil
}
