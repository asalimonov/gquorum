package topology

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
)

// PlacementInput carries declarations for pure evaluation. Approved is only an
// analysis input here; service and FSM callers must fetch their own trusted
// Target.
type PlacementInput struct {
	Approved Document
	Desired  Document
	GroupID  string
}

// MatchNodeSelector requires every selector key to be present with its exact
// expected value.
func MatchNodeSelector(labels, selector map[string]string) bool {
	for key, expected := range selector {
		value, present := labels[key]
		if !present || value != expected {
			return false
		}
	}
	return true
}

// NodeReserved reports whether the node's reserved label forbids new load
// identities.
func NodeReserved(node Node) bool {
	return node.Labels[LabelReserved] == enabledNodeLabel
}

// NodeLearnerOnly reports whether the node's learner-only label forbids voter
// placement.
func NodeLearnerOnly(node Node) bool {
	return node.Labels[LabelLearnerOnly] == enabledNodeLabel
}

// Clone copies the type restriction and label selector into owned values.
func (p PlacementRule) Clone() PlacementRule {
	p.AllowedNodeTypes = slices.Clone(p.AllowedNodeTypes)
	p.NodeSelector = maps.Clone(p.NodeSelector)
	return p
}

// AffectedPlacementGroups reports groups whose effective host or voter choices
// may change. Descriptive group labels alone do not affect placement.
func AffectedPlacementGroups(before, after Document) []string {
	ids := make(map[string]bool, len(before.Groups)+len(after.Groups))
	for _, group := range before.Groups {
		ids[group.ID] = true
	}
	for _, group := range after.Groups {
		ids[group.ID] = true
	}
	var affected []string
	for _, id := range sortedMapKeys(ids) {
		old, next := before.Group(id), after.Group(id)
		if old == nil || next == nil {
			affected = append(affected, id)
			continue
		}
		if !reflect.DeepEqual(old.Replicas, next.Replicas) || !reflect.DeepEqual(old.Hosting, next.Hosting) ||
			!reflect.DeepEqual(old.VoterConfig, next.VoterConfig) ||
			!reflect.DeepEqual(before.GroupPlacement(id), after.GroupPlacement(id)) {
			affected = append(affected, id)
			continue
		}
		previous, errOld := EvaluatePlacement(PlacementInput{Approved: before, Desired: before, GroupID: id})
		proposed, errNext := EvaluatePlacement(PlacementInput{Approved: before, Desired: after, GroupID: id})
		if errOld != nil || errNext != nil || !reflect.DeepEqual(previous.Candidates, proposed.Candidates) {
			affected = append(affected, id)
		}
	}
	return affected
}

// EvaluatePlacement reports host and voter eligibility against desired and
// approved declarations.
func EvaluatePlacement(in PlacementInput) (GroupPlacementReport, error) {
	evaluation, err := newPlacementEvaluation(in)
	if err != nil {
		return GroupPlacementReport{}, err
	}
	g := evaluation.group
	r := evaluation.report()
	nodes := slices.Clone(in.Desired.Nodes)
	slices.SortFunc(nodes, func(a, b Node) int { return compareNames(a.Name, b.Name) })
	domains := make(map[string]bool)
	for _, node := range nodes {
		if err := ValidateLabels(node.Labels, NodeLabels); err != nil {
			return r, err
		}
		c := evaluation.candidate(node)
		if c.CanVote && c.Domain != "" {
			domains[c.Domain] = true
		}
		r.Candidates = append(r.Candidates, c)
	}
	for domain := range domains {
		r.EligibleDomains = append(r.EligibleDomains, domain)
	}
	slices.Sort(r.EligibleDomains)
	if g.VoterConfig != nil && g.VoterConfig.DistinctVoterLabel != "" && len(domains) < r.RequiredVoters {
		r.Issues = append(r.Issues, PlacementIssue{
			Code: "insufficient_distinct_zones", GroupID: g.ID,
			Key: g.VoterConfig.DistinctVoterLabel,
			Message: fmt.Sprintf("group %q requires %d distinct voter domains; %d are eligible", g.ID, r.RequiredVoters,
				len(domains)),
		})
	}
	return r, nil
}

// ValidateVoterPlacement checks the exact proposed voting set against node
// restrictions and an optional strict failure-domain key. It does not assert
// native liveness or quorum; those remain execution-time checks.
func ValidateVoterPlacement(doc Document, groupID string, voters []ReplicaKey) error {
	group := doc.Group(groupID)
	if group == nil {
		return invalid("unknown placement group")
	}
	report, err := EvaluatePlacement(PlacementInput{Approved: doc, Desired: doc, GroupID: groupID})
	if err != nil {
		return err
	}
	candidates := make(map[string]PlacementCandidate, len(report.Candidates))
	for _, c := range report.Candidates {
		candidates[c.NodeName] = c
	}
	seenNodes := make(map[string]bool, len(voters))
	seenDomains := make(map[string]bool, len(voters))
	for _, voter := range voters {
		replica := groupReplicaOnNode(group, voter.NodeName)
		candidate, found := candidates[voter.NodeName]
		if replica == nil || replica.ReplicaID != voter.ReplicaID || !found || seenNodes[voter.NodeName] {
			return invalid("voter set has unknown, stale or duplicate identity")
		}
		seenNodes[voter.NodeName] = true
		if !candidate.CanVote {
			return invalid(fmt.Sprintf("node %q is not eligible to vote in group %q", voter.NodeName, groupID))
		}
		if group.VoterConfig != nil && group.VoterConfig.DistinctVoterLabel != "" {
			if seenDomains[candidate.Domain] {
				return invalid(fmt.Sprintf("group %q has multiple voters in domain %q", groupID, candidate.Domain))
			}
			seenDomains[candidate.Domain] = true
		}
	}
	return nil
}

func approvedExactReplica(approved, desired Document, groupID, nodeName string) bool {
	if approved.ClusterID == "" || approved.ClusterID != desired.ClusterID {
		return false
	}
	old, next := approved.Group(groupID), desired.Group(groupID)
	if old == nil || next == nil {
		return false
	}
	a, b := groupReplicaOnNode(old, nodeName), groupReplicaOnNode(next, nodeName)
	return a != nil && b != nil && a.ReplicaID == b.ReplicaID
}

func groupReplicaOnNode(group *Group, nodeName string) *Replica {
	for i := range group.Replicas {
		if group.Replicas[i].NodeName == nodeName {
			return &group.Replicas[i]
		}
	}
	return nil
}

func addPlacementReason(c *PlacementCandidate, groupID string, issue PlacementIssue) {
	reason := issue
	reason.GroupID, reason.NodeName = groupID, c.NodeName
	c.Reasons = append(c.Reasons, reason)
}

func sortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func compareNames(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

type placementEvaluation struct {
	input PlacementInput
	group *Group
	rule  PlacementRule
}

func newPlacementEvaluation(in PlacementInput) (*placementEvaluation, error) {
	g := in.Desired.Group(in.GroupID)
	if g == nil {
		return nil, invalid("unknown placement group")
	}
	if err := ValidateLabels(g.Labels, GroupLabels); err != nil {
		return nil, err
	}
	p := in.Desired.GroupPlacement(g.ID)
	if g.ID != MetaGroupID {
		if err := ValidateSelector(p.NodeSelector); err != nil {
			return nil, err
		}
	}
	if g.VoterConfig != nil && g.VoterConfig.DistinctVoterLabel != "" {
		if err := validateLabelKey(g.VoterConfig.DistinctVoterLabel, GroupLabels); err != nil {
			return nil, err
		}
	}

	return &placementEvaluation{input: in, group: g, rule: p}, nil
}

func (e *placementEvaluation) report() GroupPlacementReport {
	g := e.group
	in := e.input
	r := GroupPlacementReport{GroupID: g.ID, Candidates: make([]PlacementCandidate, 0, len(in.Desired.Nodes))}
	if g.VoterConfig != nil {
		r.RequiredVoters = g.VoterConfig.TargetVoters
	} else {
		for _, replica := range g.Replicas {
			if replica.Role == ReplicaVoter {
				r.RequiredVoters++
			}
		}
	}
	for _, replica := range g.Replicas {
		if replica.Role == ReplicaVoter {
			r.AssignedVoters++
		}
	}

	return r
}

func (e *placementEvaluation) candidate(node Node) PlacementCandidate {
	g := e.group
	c := PlacementCandidate{NodeName: node.Name}
	if g.ID == MetaGroupID {
		c.CanAdd, c.CanRetain = node.State.Places(), node.State.Places()
		if !node.State.Places() {
			addPlacementReason(&c, g.ID, PlacementIssue{Code: "node_excluded", Message: "node is excluded"})
		}
	} else {
		e.loadCandidate(node, &c)
	}
	e.voterCandidate(node, &c)

	return c
}

func (e *placementEvaluation) loadCandidate(node Node, c *PlacementCandidate) {
	g, p := e.group, e.rule
	eligible := node.State == NodeActive
	if !eligible {
		addPlacementReason(c, g.ID, PlacementIssue{Code: "node_not_active", Message: "node is not active"})
	}
	if !p.Allows(node.Type) {
		eligible = false
		addPlacementReason(c, g.ID, PlacementIssue{Code: "node_type_mismatch",
			Message: "node type is not allowed", Actual: string(node.Type)})
	}
	if !e.matchSelector(node, c) {
		eligible = false
	}
	if g.Hosting != nil && g.Hosting.Mode == HostingExplicit && !slices.Contains(g.Hosting.Nodes, node.Name) {
		eligible = false
		addPlacementReason(c, g.ID, PlacementIssue{Code: "outside_hosting_set",
			Message: "node is outside explicit hosting"})
	}
	approved := approvedExactReplica(e.input.Approved, e.input.Desired, g.ID, node.Name)
	c.CanAdd = eligible && !NodeReserved(node)
	c.CanRetain = eligible && (approved || !NodeReserved(node))
	if NodeReserved(node) {
		addPlacementReason(c, g.ID, PlacementIssue{Code: "node_reserved",
			Message: "node blocks new load assignments", Key: LabelReserved, Actual: enabledNodeLabel})
	}
}

func (e *placementEvaluation) matchSelector(node Node, c *PlacementCandidate) bool {
	g, p := e.group, e.rule
	eligible := true
	for _, key := range sortedMapKeys(p.NodeSelector) {
		want := p.NodeSelector[key]
		actual, exists := node.Labels[key]
		if !exists {
			eligible = false
			addPlacementReason(c, g.ID, PlacementIssue{Code: "selector_key_missing",
				Message: "required node label is absent", Key: key, Expected: want})
		} else if actual != want {
			eligible = false
			addPlacementReason(c, g.ID, PlacementIssue{Code: "selector_value_mismatch",
				Message: "node label does not match", Key: key, Expected: want, Actual: actual})
		}
	}

	return eligible
}

func (e *placementEvaluation) voterCandidate(node Node, c *PlacementCandidate) {
	g := e.group
	c.CanVote = (c.CanAdd || c.CanRetain) && (node.State == NodeActive || g.ID == MetaGroupID &&
		node.State.Places())
	if NodeLearnerOnly(node) {
		c.CanVote = false
		addPlacementReason(c, g.ID, PlacementIssue{Code: "node_learner_only", Message: "node cannot vote",
			Key: LabelLearnerOnly, Actual: enabledNodeLabel})
	}
	if replica := groupReplicaOnNode(g, node.Name); replica != nil && replica.PinnedLearner {
		c.CanVote = false
		addPlacementReason(c, g.ID, PlacementIssue{Code: "replica_pinned_learner",
			Message: "replica is pinned as learner"})
	}
	if g.VoterConfig != nil && g.VoterConfig.DistinctVoterLabel != "" {
		key := g.VoterConfig.DistinctVoterLabel
		c.Domain = node.Labels[key]
		if c.Domain == "" {
			c.CanVote = false
			addPlacementReason(c, g.ID, PlacementIssue{Code: "voter_domain_missing",
				Message: "voter domain is missing or empty", Key: key, Expected: "nonempty", Actual: c.Domain})
		}
	}
}
