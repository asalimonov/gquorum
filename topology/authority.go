package topology

import (
	"cmp"
	"slices"
	"strings"
)

type AuthorityResourceKind string

const (
	AuthorityGroup AuthorityResourceKind = "group"
	AuthorityHost  AuthorityResourceKind = "host"
)

type AuthorityResource struct {
	Kind     AuthorityResourceKind `json:"kind"`
	GroupID  string                `json:"group_id"`
	NodeName string                `json:"node_name,omitempty"`
}
type AuthorityKey struct {
	ClusterID string `json:"cluster_id"`
	GroupID   string `json:"group_id"`
	Revision  uint64 `json:"revision"`
}
type AuthorityPurpose string

const (
	AuthorityEffect   AuthorityPurpose = "effect"
	AuthorityFence    AuthorityPurpose = "fence"
	AuthorityTransfer AuthorityPurpose = "transfer"
	AuthorityReadmit  AuthorityPurpose = "readmission"
)

type AuthorityTarget struct {
	NodeName  string `json:"node_name"`
	MemberID  uint64 `json:"member_id"`
	ReplicaID uint64 `json:"replica_id"`
}
type AuthorityGrant struct {
	Key         AuthorityKey        `json:"key"`
	Purpose     AuthorityPurpose    `json:"purpose"`
	OperatorID  OperatorID          `json:"operator_id,omitempty"`
	Action      *AuthorizedStep     `json:"action,omitempty"`
	AdmissionID string              `json:"admission_id,omitempty"`
	RequestID   string              `json:"request_id,omitempty"`
	Resources   []AuthorityResource `json:"resources"`
	Targets     []AuthorityTarget   `json:"targets"`
}
type FenceEvidence struct {
	Key           AuthorityKey      `json:"key"`
	Resource      AuthorityResource `json:"resource"`
	Receiver      Identity          `json:"receiver"`
	ReplicaID     uint64            `json:"replica_id,omitempty"`
	AppliedIndex  uint64            `json:"applied_index,omitempty"`
	AppliedTerm   uint64            `json:"applied_term,omitempty"`
	LocalSequence uint64            `json:"local_sequence,omitempty"`
}
type EffectOutcome string

const (
	EffectApplied          EffectOutcome = "applied"
	EffectAlreadySatisfied EffectOutcome = "already_satisfied"
	EffectTransferStarted  EffectOutcome = "transfer_started"
)

type EffectReceipt struct {
	Key           AuthorityKey      `json:"key"`
	Resource      AuthorityResource `json:"resource"`
	Action        *AuthorizedStep   `json:"action,omitempty"`
	AdmissionID   string            `json:"admission_id,omitempty"`
	Target        AuthorityTarget   `json:"target"`
	Outcome       EffectOutcome     `json:"outcome"`
	AppliedIndex  uint64            `json:"applied_index,omitempty"`
	AppliedTerm   uint64            `json:"applied_term,omitempty"`
	LocalSequence uint64            `json:"local_sequence,omitempty"`
}
type FenceObligation struct {
	Key      AuthorityKey      `json:"key"`
	Resource AuthorityResource `json:"resource"`
	Evidence *FenceEvidence    `json:"evidence,omitempty"`
}
type AuthorityStatus struct {
	OperatorID          OperatorID        `json:"operator_id"`
	RevocationRequested bool              `json:"revocation_requested"`
	RevocationComplete  bool              `json:"revocation_complete"`
	Obligations         []FenceObligation `json:"obligations"`
}

// Clone returns an owned immutable-grant value suitable for an application or transport.
func (g AuthorityGrant) Clone() AuthorityGrant {
	g.Resources = slices.Clone(g.Resources)
	g.Targets = slices.Clone(g.Targets)
	if g.Action != nil {
		action := *g.Action
		g.Action = &action
	}
	return g
}

// Equal compares every grant field, including exact resource and target ordering.
func (g AuthorityGrant) Equal(other AuthorityGrant) bool {
	return g.Key == other.Key && g.Purpose == other.Purpose && g.OperatorID == other.OperatorID &&
		g.AdmissionID == other.AdmissionID && g.RequestID == other.RequestID &&
		equalAction(g.Action, other.Action) && slices.Equal(g.Resources, other.Resources) && slices.Equal(g.Targets, other.Targets)
}
func equalAction(a, b *AuthorizedStep) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
func authorityText(s string) bool { return strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0) }

// Compare orders resources canonically for replicated grants and snapshots.
func (r AuthorityResource) Compare(other AuthorityResource) int {
	return cmp.Or(cmp.Compare(r.Kind, other.Kind), cmp.Compare(r.GroupID, other.GroupID), cmp.Compare(r.NodeName, other.NodeName))
}

// Compare orders exact targets without conflating node names, members and replicas.
func (t AuthorityTarget) Compare(other AuthorityTarget) int {
	return cmp.Or(cmp.Compare(t.NodeName, other.NodeName), cmp.Compare(t.MemberID, other.MemberID), cmp.Compare(t.ReplicaID, other.ReplicaID))
}

// Validate checks the grant's immutable shape. It is not evidence that _meta
// committed it: receivers must separately validate its retained catalog record.
func (g AuthorityGrant) Validate() error {
	if !authorityText(g.Key.ClusterID) || ValidateID(g.Key.GroupID) != nil || g.Key.Revision == 0 || len(g.Resources) == 0 || len(g.Targets) == 0 {
		return invalid("authority requires exact identity, revision, resources and targets")
	}
	for i, t := range g.Targets {
		if !authorityText(t.NodeName) || t.MemberID == 0 || t.ReplicaID == 0 || i > 0 && g.Targets[i-1].Compare(t) >= 0 {
			return invalid("authority targets must be complete, sorted and unique")
		}
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
			if !authorityText(r.NodeName) || !slices.ContainsFunc(g.Targets, func(t AuthorityTarget) bool { return t.NodeName == r.NodeName }) {
				return invalid("host authority requires an exact granted target")
			}
		default:
			return invalid("unknown authority resource kind")
		}
	}
	switch g.Purpose {
	case AuthorityFence:
		if g.Action != nil || g.AdmissionID != "" || g.RequestID != "" {
			return invalid("fence cannot authorize an effect")
		}
	case AuthorityReadmit:
		if g.Action != nil || !authorityText(g.AdmissionID) || g.RequestID != "" {
			return invalid("readmission requires its exact admission proof")
		}
		for _, r := range g.Resources {
			if r.Kind != AuthorityHost {
				return invalid("readmission fences host resources")
			}
		}
	case AuthorityEffect, AuthorityTransfer:
		if g.Action == nil || g.Action.GroupID != g.Key.GroupID || g.Action.Version == 0 || g.AdmissionID != "" {
			return invalid("effect requires its exact approved action")
		}
		a := *g.Action
		if g.Purpose == AuthorityEffect {
			if a.Attempt == 0 || a.StepSeq < 1 || a.OperatorID != MakeOperatorID(a.Version, a.Attempt) || g.OperatorID != a.OperatorID || g.RequestID != "" {
				return invalid("effect does not identify its exact operator attempt and step")
			}
		} else if a.Kind != StepTransferLeader || a.OperatorID != "" || g.OperatorID != "" || a.StepSeq != 0 || a.Attempt != 0 || !authorityText(g.RequestID) {
			return invalid("direct transfer requires an exact request identity")
		}
		return g.validateAction(a)
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
	} else if !authorityText(a.NodeName) || a.ReplicaID == 0 || !slices.ContainsFunc(g.Targets, func(t AuthorityTarget) bool { return t.NodeName == a.NodeName && t.ReplicaID == a.ReplicaID }) {
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
	group, hosts := 0, 0
	for _, r := range g.Resources {
		if r.Kind == AuthorityGroup {
			group++
		} else {
			hosts++
			if !whole && r.NodeName != a.NodeName {
				return invalid("local action names another host")
			}
		}
	}
	switch a.Kind {
	case StepAddLearner, StepPromoteVoter, StepDemoteVoter, StepRemoveReplica, StepTransferLeader:
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
		for _, t := range g.Targets {
			if !slices.Contains(g.Resources, AuthorityResource{Kind: AuthorityHost, GroupID: g.Key.GroupID, NodeName: t.NodeName}) {
				return invalid("founder lacks host authority")
			}
		}
	case StepDeleteGroupState:
		if group != 0 || hosts == 0 {
			return invalid("deletion requires host resources")
		}
		for _, t := range g.Targets {
			if !slices.Contains(g.Resources, AuthorityResource{Kind: AuthorityHost, GroupID: g.Key.GroupID, NodeName: t.NodeName}) {
				return invalid("deletion target lacks host authority")
			}
		}
	default:
		return invalid("action has no supported effect")
	}
	return nil
}

// Validate verifies complete evidence against the grant. The authenticated
// dispatcher still verifies that the response came from the claimed receiver.
func (e FenceEvidence) Validate(g AuthorityGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	if e.Key != g.Key || !slices.Contains(g.Resources, e.Resource) || e.Receiver.ClusterID != g.Key.ClusterID ||
		!authorityText(e.Receiver.InstanceID) || !slices.ContainsFunc(g.Targets, func(t AuthorityTarget) bool {
		return t.NodeName == e.Receiver.NodeName && t.MemberID == e.Receiver.MemberID && (e.Resource.Kind == AuthorityHost || t.ReplicaID == e.ReplicaID)
	}) {
		return invalid("fence evidence does not match exact grant and receiver")
	}
	if e.Resource.Kind == AuthorityHost {
		if e.Resource.NodeName != e.Receiver.NodeName || e.LocalSequence == 0 || e.AppliedIndex != 0 || e.AppliedTerm != 0 || e.ReplicaID != 0 {
			return invalid("incomplete host fence evidence")
		}
	} else if e.AppliedIndex == 0 || e.AppliedTerm == 0 || e.LocalSequence != 0 {
		return invalid("incomplete group fence evidence")
	}
	return nil
}

// Clone copies the optional action so a receipt never aliases mutable caller data.
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
	if g.Purpose == AuthorityFence || r.Key != g.Key || !slices.Contains(g.Resources, r.Resource) || !equalAction(r.Action, g.Action) || r.AdmissionID != g.AdmissionID {
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
	if r.Resource.Kind == AuthorityHost {
		if r.Target.NodeName != r.Resource.NodeName || r.LocalSequence == 0 || r.AppliedIndex != 0 || r.AppliedTerm != 0 {
			return invalid("incomplete local effect receipt")
		}
	} else if r.AppliedIndex == 0 || r.AppliedTerm == 0 || r.LocalSequence != 0 {
		return invalid("incomplete group effect receipt")
	}
	switch r.Outcome {
	case EffectApplied, EffectAlreadySatisfied:
	case EffectTransferStarted:
		if g.Action == nil || g.Action.Kind != StepTransferLeader || r.Resource.Kind != AuthorityGroup {
			return invalid("transfer receipt for a non-transfer action")
		}
	default:
		return invalid("unknown effect outcome")
	}
	if !whole && g.Action != nil && (r.Target.NodeName != g.Action.NodeName || r.Target.ReplicaID != g.Action.ReplicaID) {
		return invalid("receipt names another action target")
	}
	return nil
}

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
