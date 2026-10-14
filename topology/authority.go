package topology

import (
	"cmp"
	"slices"
	"strings"
)

// AuthorityResourceKind distinguishes consensus group resources from local host
// resources.
type AuthorityResourceKind string

// AuthorityGroup and AuthorityHost identify group and host fence domains.
const (
	AuthorityGroup AuthorityResourceKind = "group"
	AuthorityHost  AuthorityResourceKind = "host"
)

// AuthorityResource identifies a group or one node's local resources for that
// group.
type AuthorityResource struct {
	Kind     AuthorityResourceKind `json:"kind"`
	GroupID  string                `json:"group_id"`
	NodeName string                `json:"node_name,omitempty"`
}

// AuthorityKey identifies a monotonic grant revision within an exact cluster
// and group.
type AuthorityKey struct {
	ClusterID string `json:"cluster_id"`
	GroupID   string `json:"group_id"`
	Revision  uint64 `json:"revision"`
}

// AuthorityEffect and the following purposes distinguish effects, fencing,
// transfer and readmission.
const (
	AuthorityEffect   AuthorityPurpose = "effect"
	AuthorityFence    AuthorityPurpose = "fence"
	AuthorityTransfer AuthorityPurpose = "transfer"
	AuthorityReadmit  AuthorityPurpose = "readmission"
)

// EffectApplied and the following outcomes distinguish applied, satisfied and
// initiated-transfer effects.
const (
	EffectApplied          EffectOutcome = "applied"
	EffectAlreadySatisfied EffectOutcome = "already_satisfied"
	EffectTransferStarted  EffectOutcome = "transfer_started"
)

// Clone returns an owned immutable-grant value suitable for an application or
// transport.
func (g AuthorityGrant) Clone() AuthorityGrant {
	g.Resources = slices.Clone(g.Resources)
	g.Targets = slices.Clone(g.Targets)
	if g.Action != nil {
		action := *g.Action
		g.Action = &action
	}
	return g
}

// Equal compares every grant field, including exact resource and target
// ordering.
func (g AuthorityGrant) Equal(other AuthorityGrant) bool {
	return g.Key == other.Key && g.Purpose == other.Purpose && g.OperatorID == other.OperatorID &&
		g.AdmissionID == other.AdmissionID && g.RequestID == other.RequestID && equalAction(g.Action,
		other.Action) && slices.Equal(g.Resources, other.Resources) &&
		slices.Equal(g.Targets, other.Targets)
}
func equalAction(a, b *AuthorizedStep) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func authorityText(s string) bool {
	return strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0)
}

// Compare orders resources canonically for replicated grants and snapshots.
func (r AuthorityResource) Compare(other AuthorityResource) int {
	return cmp.Or(cmp.Compare(r.Kind, other.Kind), cmp.Compare(r.GroupID, other.GroupID),
		cmp.Compare(r.NodeName, other.NodeName))
}

// Compare orders exact targets without conflating node names, members and
// replicas.
func (t AuthorityTarget) Compare(other AuthorityTarget) int {
	return cmp.Or(cmp.Compare(t.NodeName, other.NodeName), cmp.Compare(t.MemberID, other.MemberID),
		cmp.Compare(t.ReplicaID, other.ReplicaID))
}

// Validate checks the grant's immutable shape. It is not evidence that _meta
// committed it: receivers must separately validate its retained catalog record.
func (g AuthorityGrant) Validate() error {
	if g.Key.Revision == 0 || len(g.Resources) == 0 || len(g.Targets) == 0 ||
		!authorityText(g.Key.ClusterID) || ValidateID(g.Key.GroupID) != nil {
		return invalid("authority requires exact identity, revision, resources and targets")
	}
	if err := g.validateShape(); err != nil {
		return err
	}
	return g.validatePurpose()
}

func (g AuthorityGrant) validateShape() error {
	if err := g.validateTargets(); err != nil {
		return err
	}
	for i, r := range g.Resources {
		if r.GroupID != g.Key.GroupID || i > 0 && g.Resources[i-1].Compare(r) >= 0 {
			return invalid("authority resources must match group and be sorted and unique")
		}
		switch r.Kind {
		case AuthorityGroup:
			if r.NodeName != "" {
				return invalid("group authority cannot name a host")
			}
		case AuthorityHost:
			if !authorityText(r.NodeName) ||
				!slices.ContainsFunc(g.Targets, func(t AuthorityTarget) bool { return t.NodeName == r.NodeName }) {
				return invalid("host authority requires an exact granted target")
			}
		default:
			return invalid("unknown authority resource kind")
		}
	}
	return nil
}

func (g AuthorityGrant) validatePurpose() error {
	switch g.Purpose {
	case AuthorityFence:
		if g.Action != nil || g.AdmissionID != "" || g.RequestID != "" {
			return invalid("fence cannot authorize an effect")
		}
	case AuthorityReadmit:
		if g.Action != nil || g.RequestID != "" || !authorityText(g.AdmissionID) {
			return invalid("readmission requires its exact admission proof")
		}
		for _, r := range g.Resources {
			if r.Kind != AuthorityHost {
				return invalid("readmission fences host resources")
			}
		}
	case AuthorityEffect, AuthorityTransfer:
		return g.validateEffect()
	default:
		return invalid("unknown authority purpose")
	}
	return nil
}

func (g AuthorityGrant) validateAction(a AuthorizedStep) error {
	whole := a.Kind == StepCreateFoundingGroup || a.Kind == StepDeleteGroupState
	if whole {
		if a.NodeName != "" || a.ReplicaID != 0 {
			return invalid("group-wide action cannot name one replica")
		}
	} else if a.ReplicaID == 0 || !authorityText(a.NodeName) ||
		!slices.ContainsFunc(g.Targets, func(t AuthorityTarget) bool {
			return t.NodeName == a.NodeName && t.ReplicaID == a.ReplicaID
		}) {
		return invalid("action replica is outside granted targets")
	}
	if a.TransferReplicaID != 0 || a.TransferNodeName != "" {
		if a.Kind != StepDemoteVoter || a.TransferReplicaID == 0 || !authorityText(a.TransferNodeName) ||
			!slices.ContainsFunc(g.Targets, func(t AuthorityTarget) bool {
				return t.NodeName == a.TransferNodeName && t.ReplicaID == a.TransferReplicaID
			}) {
			return invalid("invalid demotion transfer target")
		}
	}
	return g.validateActionResources(a)
}

// Validate verifies complete evidence against the grant. The authenticated
// dispatcher still verifies that the response came from the claimed receiver.
func (e FenceEvidence) Validate(g AuthorityGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	if e.Key != g.Key || e.Receiver.ClusterID != g.Key.ClusterID || !slices.Contains(g.Resources, e.Resource) ||
		!authorityText(e.Receiver.InstanceID) ||
		!slices.ContainsFunc(g.Targets, func(t AuthorityTarget) bool {
			return t.NodeName == e.Receiver.NodeName && t.MemberID == e.Receiver.MemberID &&
				(e.Resource.Kind == AuthorityHost || t.ReplicaID == e.ReplicaID)
		}) {
		return invalid("fence evidence does not match exact grant and receiver")
	}
	if e.Resource.Kind == AuthorityHost {
		if e.Resource.NodeName != e.Receiver.NodeName || e.LocalSequence == 0 || e.AppliedIndex != 0 ||
			e.AppliedTerm != 0 || e.ReplicaID != 0 {
			return invalid("incomplete host fence evidence")
		}
	} else if e.AppliedIndex == 0 || e.AppliedTerm == 0 || e.LocalSequence != 0 {
		return invalid("incomplete group fence evidence")
	}
	return nil
}

// Clone copies the optional action so a receipt never aliases mutable caller
// data.
func (r EffectReceipt) Clone() EffectReceipt {
	if r.Action != nil {
		a := *r.Action
		r.Action = &a
	}
	return r
}

// Validate binds consumption proof to the full immutable action and resource.
func (r EffectReceipt) Validate(g AuthorityGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Purpose == AuthorityFence || r.Key != g.Key || r.AdmissionID != g.AdmissionID ||
		!slices.Contains(g.Resources, r.Resource) || !equalAction(r.Action, g.Action) {
		return invalid("effect receipt does not match exact grant")
	}
	whole := g.Action != nil && (g.Action.Kind == StepCreateFoundingGroup || g.Action.Kind == StepDeleteGroupState)
	if r.Target == (AuthorityTarget{}) {
		if r.Resource.Kind != AuthorityGroup || !whole {
			return invalid("receipt lacks its exact target")
		}
	} else if !slices.Contains(g.Targets, r.Target) {
		return invalid("receipt target is outside grant")
	}
	if err := r.validateResource(); err != nil {
		return err
	}
	if err := r.validateOutcome(g); err != nil {
		return err
	}
	if !whole && g.Action != nil && (r.Target.NodeName != g.Action.NodeName ||
		r.Target.ReplicaID != g.Action.ReplicaID) {
		return invalid("receipt names another action target")
	}
	return nil
}

// Clone copies fence obligations and their optional evidence into owned values.
func (s AuthorityStatus) Clone() AuthorityStatus {
	s.Obligations = slices.Clone(s.Obligations)
	for i := range s.Obligations {
		if s.Obligations[i].Evidence != nil {
			e := *s.Obligations[i].Evidence
			s.Obligations[i].Evidence = &e
		}
	}
	return s
}

func (g AuthorityGrant) validateEffect() error {
	if g.Action == nil || g.Action.GroupID != g.Key.GroupID || g.Action.Version == 0 || g.AdmissionID != "" {
		return invalid("effect requires its exact approved action")
	}
	a := *g.Action
	if g.Purpose == AuthorityEffect {
		if a.Attempt == 0 || a.StepSeq < 1 || g.OperatorID != a.OperatorID || g.RequestID != "" ||
			a.OperatorID != MakeOperatorID(a.Version, a.Attempt) {
			return invalid("effect does not identify its exact operator attempt and step")
		}
	} else if a.Kind != StepTransferLeader || a.OperatorID != "" || g.OperatorID != "" || a.StepSeq != 0 ||
		a.Attempt != 0 || !authorityText(g.RequestID) {
		return invalid("direct transfer requires an exact request identity")
	}
	return g.validateAction(a)
}

func (g AuthorityGrant) validateActionResources(a AuthorizedStep) error {
	counts, err := g.countActionResources(a)
	if err != nil {
		return err
	}
	group, hosts := counts.group, counts.hosts
	switch a.Kind {
	case StepAddLearner,
		StepPromoteVoter,
		StepDemoteVoter,
		StepRemoveReplica,
		StepTransferLeader:
		if group != 1 || hosts != 0 {
			return invalid("consensus action requires only the group resource")
		}
	case StepPrepareReplica, StepStopReplica, StepCleanupReplicaState:
		if group != 0 || hosts != 1 {
			return invalid("local action requires only its host resource")
		}
	case StepCreateFoundingGroup:
		if group != 1 || hosts == 0 {
			return invalid("founding requires group and founder host resources")
		}
		return g.validateTargetHosts("founder lacks host authority")
	case StepDeleteGroupState:
		if group != 0 || hosts == 0 {
			return invalid("deletion requires host resources")
		}
		return g.validateTargetHosts("deletion target lacks host authority")
	default:
		return invalid("action has no supported effect")
	}
	return nil
}

func (g AuthorityGrant) validateTargetHosts(message string) error {
	for _, t := range g.Targets {
		if !slices.Contains(g.Resources, AuthorityResource{Kind: AuthorityHost, GroupID: g.Key.GroupID,
			NodeName: t.NodeName}) {
			return invalid(message)
		}
	}
	return nil
}

func (r EffectReceipt) validateResource() error {
	if r.Resource.Kind == AuthorityHost {
		if r.Target.NodeName != r.Resource.NodeName || r.LocalSequence == 0 || r.AppliedIndex != 0 ||
			r.AppliedTerm != 0 {
			return invalid("incomplete local effect receipt")
		}
	} else if r.AppliedIndex == 0 || r.AppliedTerm == 0 || r.LocalSequence != 0 {
		return invalid("incomplete group effect receipt")
	}

	return nil
}

func (g AuthorityGrant) validateTargets() error {
	for i, t := range g.Targets {
		if t.MemberID == 0 || t.ReplicaID == 0 || !authorityText(t.NodeName) ||
			i > 0 && g.Targets[i-1].Compare(t) >= 0 {
			return invalid("authority targets must be complete, sorted and unique")
		}
	}

	return nil
}

func (r EffectReceipt) validateOutcome(g AuthorityGrant) error {
	switch r.Outcome {
	case EffectApplied, EffectAlreadySatisfied:
	case EffectTransferStarted:
		if g.Action == nil || g.Action.Kind != StepTransferLeader || r.Resource.Kind != AuthorityGroup {
			return invalid("transfer receipt for a non-transfer action")
		}
	default:
		return invalid("unknown effect outcome")
	}

	return nil
}

type actionResourceCounts struct{ group, hosts int }

func (g AuthorityGrant) countActionResources(a AuthorizedStep) (actionResourceCounts, error) {
	whole := a.Kind == StepCreateFoundingGroup || a.Kind == StepDeleteGroupState
	counts := actionResourceCounts{}
	for _, r := range g.Resources {
		if r.Kind == AuthorityGroup {
			counts.group++
		} else {
			counts.hosts++
			if !whole && r.NodeName != a.NodeName {
				return actionResourceCounts{}, invalid("local action names another host")
			}
		}
	}

	return counts, nil
}
