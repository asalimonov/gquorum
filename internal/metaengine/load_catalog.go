package metaengine

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LoadCatalog describes current approved routing/observation candidates. It
// does
// not grant an external effect; effects require immutable committed grants.
func (sm *topologySM) LoadCatalog(ctx context.Context, group string) ([]storage.Identity, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := topology.ValidateLoadID(group); err != nil {
		return nil, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ids := make(map[storage.Identity]bool)
	for _, doc := range []topology.Document{sm.active, sm.target} {
		if g := doc.Group(group); g != nil {
			for _, r := range g.Replicas {
				ids[storage.Identity{ClusterID: doc.ClusterID, NodeName: r.NodeName, GroupID: group,
					ReplicaID: r.ReplicaID}] = true
			}
		}
	}
	return orderedIdentities(ids), nil
}

func orderedIdentities(ids map[storage.Identity]bool) []storage.Identity {
	out := make([]storage.Identity, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	slices.SortFunc(out, func(a, b storage.Identity) int {
		if order := cmp.Compare(a.NodeName, b.NodeName); order != 0 {
			return order
		}
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
	return out
}

// DeletionBase supplies approved historical identities for planning cleanup,
// including interrupted preparations in surviving groups.
// Legacy composite adapter; remove remaining nonplanner consumers at Task 73.
// New planners use PlanningInput for actual convergence plus separate evidence.
func (sm *topologySM) DeletionBase(ctx context.Context, desired topology.Document) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.deletionBaseLocked(desired)
}

// OperatorBase preserves the exact retired identities named by an immutable
// retry even when earlier consumption removed them from new planning input.
// Remove this legacy composite adapter at Task 73.
func (sm *topologySM) OperatorBase(ctx context.Context, id topology.OperatorID) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	op := sm.operators[id]
	if op == nil || op.Version != sm.target.Version {
		return topology.Document{}, fault.ErrOperatorAuthorization
	}
	return sm.operatorBaseLocked(op)
}

func (sm *topologySM) operatorBaseLocked(op *operatorRecord) (topology.Document, error) {
	base, err := sm.deletionBaseLocked(sm.target)
	if err != nil {
		return topology.Document{}, err
	}
	for _, step := range op.Steps {
		if step.Kind != topology.StepRemoveReplica && step.Kind != topology.StepStopReplica &&
			step.Kind != topology.StepCleanupReplicaState {
			continue
		}
		group := base.Group(step.GroupID)
		if group == nil {
			base.Groups = append(base.Groups, topology.Group{ID: step.GroupID})
			group = &base.Groups[len(base.Groups)-1]
		}
		if !slices.ContainsFunc(group.Replicas,
			func(r topology.Replica) bool { return r.ReplicaID == step.ReplicaID && r.NodeName == step.NodeName }) {
			group.Replicas = append(group.Replicas,
				topology.Replica{NodeName: step.NodeName, ReplicaID: step.ReplicaID, Role: topology.ReplicaVoter})
		}
	}
	canonicalizeDocument(&base)
	return base, nil
}

func (sm *topologySM) deletionBaseLocked(desired topology.Document) (topology.Document, error) {
	input, err := sm.planningInputLocked(desired, nil)
	if err != nil {
		return topology.Document{}, err
	}
	b := deletionBaseBuilder{sm: sm, desired: desired, base: sm.active.Clone(), input: input}
	groups := make(map[string]bool)
	for _, g := range b.base.Groups {
		groups[g.ID] = true
	}
	for _, op := range sm.operators {
		for _, step := range op.Steps {
			groups[step.GroupID] = true
		}
	}
	for _, record := range sm.authority.records {
		groups[record.Grant.Key.GroupID] = true
	}
	b.proofs = sm.cleanupProofsLocked()
	for group := range groups {
		if err := b.addGroup(group); err != nil {
			return topology.Document{}, err
		}
	}
	canonicalizeDocument(&b.base)
	return b.base, nil
}

type deletionBaseBuilder struct {
	sm            *topologySM
	desired, base topology.Document
	proofs        cleanupProofs
	input         topology.PlanInput
}

func (b *deletionBaseBuilder) addGroup(group string) error {
	// A deletion already incorporated into Active needs no new operator.
	if b.sm.retired[group] != 0 && b.sm.active.Version >= b.sm.retired[group] &&
		b.desired.Group(group) == nil && b.base.Group(group) == nil {
		return nil
	}
	ids, err := b.sm.deletionCatalogLocked(group)
	if err != nil {
		return err
	}
	if b.desired.Group(group) != nil {
		b.addReadmittedTargets(group, ids)
	}
	b.retainPossibleTargets(group, ids)
	g := b.base.Group(group)
	if b.desired.Group(group) != nil {
		b.removeDesiredTargets(group, ids)
		if g != nil {
			b.addRetainedDesiredTargets(g, ids)
		}
	}
	if g == nil && len(ids) == 0 {
		return nil
	}
	if g == nil {
		b.base.Groups = append(b.base.Groups, topology.Group{ID: group})
		g = &b.base.Groups[len(b.base.Groups)-1]
	}
	previous := g.Replicas
	g.Replicas = nil
	for _, id := range orderedIdentities(ids) {
		r := deletionReplica(previous, id)
		g.Replicas = append(g.Replicas, r)
		if group == topology.MetaGroupID && b.base.Node(id.NodeName) == nil {
			b.addMetaNode(id.NodeName)
		}
	}
	return nil
}

// The legacy document carrier remains until Task 24 converts construction.
// Restrict its new work to the same factual owners used by approval validation;
// an unissued approved action cannot establish a resource. OperatorBase separately
// preserves original steps for immutable retry attribution.
func (b *deletionBaseBuilder) retainPossibleTargets(group string, ids map[storage.Identity]bool) {
	for id := range ids {
		if b.sm.activeContains(id) {
			continue
		}
		known := slices.ContainsFunc(b.input.Retained[group].Replicas, func(r topology.RetainedReplica) bool {
			return r.NodeName == id.NodeName && r.ReplicaID == id.ReplicaID
		})
		if !known {
			delete(ids, id)
		}
	}
}

func (b *deletionBaseBuilder) addReadmittedTargets(group string, ids map[storage.Identity]bool) {
	// Readmission retires host resources before their old native
	// members are removed. Preserve their routing identities while the
	// replacement
	// is not yet Active; its gated preparation covers host retirement.
	for id := range b.proofs.readmitted {
		if id.GroupID == group && b.desired.MemberID(id.NodeName) != 0 &&
			b.desired.MemberID(id.NodeName) != b.sm.active.MemberID(id.NodeName) {
			ids[id] = true
		}
	}
}

func (b *deletionBaseBuilder) removeDesiredTargets(group string, ids map[storage.Identity]bool) {
	// Desired-only identities do not establish a previously absent
	// group. Retain an historical owner only when it still requires
	// retirement.
	for id := range ids {
		if r := documentReplica(b.desired, group, id.ReplicaID); r != nil && r.NodeName == id.NodeName {
			delete(ids, id)
		}
	}
}

func (b *deletionBaseBuilder) addRetainedDesiredTargets(g *topology.Group, ids map[storage.Identity]bool) {
	for _, r := range g.Replicas {
		if wanted := documentReplica(b.desired, g.ID, r.ReplicaID); wanted != nil &&
			wanted.NodeName == r.NodeName {
			ids[b.sm.loadIdentity(g.ID, r.NodeName, r.ReplicaID)] = true
		}
	}
}

func deletionReplica(previous []topology.Replica, id storage.Identity) topology.Replica {
	r := topology.Replica{NodeName: id.NodeName, ReplicaID: id.ReplicaID, Role: topology.ReplicaVoter}
	for _, old := range previous {
		if old.NodeName == id.NodeName && old.ReplicaID == id.ReplicaID {
			r = old
			break
		}
	}
	return r
}

func (b *deletionBaseBuilder) addMetaNode(name string) {
	// An admitted member may enter native membership before Active.
	// Keep its approved node attributes for drain/health planning
	// too.
	for _, doc := range b.sm.authorityDocumentsLocked() {
		if node := doc.Node(name); node != nil {
			owned := *node
			owned.Labels = maps.Clone(node.Labels)
			b.base.Nodes = append(b.base.Nodes, owned)
			break
		}
	}
}

func (sm *topologySM) deletionCatalogLocked(group string) (map[storage.Identity]bool, error) {
	ids := make(map[storage.Identity]bool)
	proofs := sm.cleanupProofsLocked()
	if proofs.groups[group] != "" {
		return ids, nil
	}
	catalog := deletionCatalog{sm: sm, group: group, ids: ids}
	catalog.addGroup(sm.active)
	for _, op := range sm.operators {
		if err := catalog.addOperator(op); err != nil {
			return nil, err
		}
	}
	for _, record := range sm.authority.records {
		if record.Grant.Key.GroupID != group {
			continue
		}
		catalog.known = true
		for _, target := range record.Grant.Targets {
			catalog.ids[sm.loadIdentity(group, target.NodeName, target.ReplicaID)] = true
		}
	}
	if !catalog.known {
		return nil, fault.ErrHistoryUnavailable
	}
	for id := range ids {
		if proofs.covers(id) {
			delete(ids, id)
		}
	}
	return ids, nil
}

type deletionCatalog struct {
	sm    *topologySM
	group string
	ids   map[storage.Identity]bool
	known bool
}

func (catalog *deletionCatalog) addGroup(doc topology.Document) {
	if g := doc.Group(catalog.group); g != nil {
		catalog.known = true
		for _, r := range g.Replicas {
			catalog.ids[catalog.sm.loadIdentity(catalog.group, r.NodeName, r.ReplicaID)] = true
		}
	}
}

func (catalog *deletionCatalog) addOperator(op *operatorRecord) error {
	if doc, ok := catalog.sm.documentLocked(op.Version); ok {
		catalog.addGroup(doc)
	}
	for _, step := range op.Steps {
		if step.GroupID != catalog.group {
			continue
		}
		catalog.known = true
		if step.Kind == topology.StepCreateFoundingGroup && catalog.sm.active.Version < op.Version {
			doc, ok := catalog.sm.documentLocked(op.Version)
			if !ok || doc.Group(catalog.group) == nil {
				return fault.ErrHistoryUnavailable
			}
		}
		if step.NodeName != "" && step.ReplicaID != 0 {
			catalog.ids[catalog.sm.loadIdentity(catalog.group, step.NodeName, step.ReplicaID)] = true
		}
	}
	return nil
}
