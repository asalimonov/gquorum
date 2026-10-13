//go:build !functional

package topology

import (
	"testing"
)

func testAuthorityGrant() AuthorityGrant {
	return AuthorityGrant{
		Key: AuthorityKey{ClusterID: "cluster", GroupID: "orders", Revision: 41}, Purpose: AuthorityEffect,
		OperatorID: MakeOperatorID(3, 2),
		Action:     &AuthorizedStep{OperatorID: MakeOperatorID(3, 2), Version: 3, Attempt: 2, StepSeq: 5, Kind: StepPromoteVoter, GroupID: "orders", NodeName: "b", ReplicaID: 12},
		Resources:  []AuthorityResource{{Kind: AuthorityGroup, GroupID: "orders"}},
		Targets:    []AuthorityTarget{{NodeName: "a", MemberID: 1, ReplicaID: 11}, {NodeName: "b", MemberID: 2, ReplicaID: 12}},
	}
}

func TestAuthorityGrantRejectsIncompleteOrReboundApproval(t *testing.T) {
	t.Parallel()
	g := testAuthorityGrant()
	if err := g.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*AuthorityGrant){
		"zero revision":         func(g *AuthorityGrant) { g.Key.Revision = 0 },
		"foreign group":         func(g *AuthorityGrant) { g.Key.GroupID = "other" },
		"unsafe group":          func(g *AuthorityGrant) { g.Key.GroupID = "../orders" },
		"foreign operator":      func(g *AuthorityGrant) { g.OperatorID = MakeOperatorID(4, 2) },
		"foreign attempt":       func(g *AuthorityGrant) { g.Action.Attempt++ },
		"missing step":          func(g *AuthorityGrant) { g.Action.StepSeq = 0 },
		"ungiven target":        func(g *AuthorityGrant) { g.Action.ReplicaID++ },
		"observation as effect": func(g *AuthorityGrant) { g.Action.Kind = StepWaitCaughtUp },
		"host instead of group": func(g *AuthorityGrant) {
			g.Resources[0] = AuthorityResource{Kind: AuthorityHost, GroupID: "orders", NodeName: "b"}
		},
		"duplicate resource":      func(g *AuthorityGrant) { g.Resources = append(g.Resources, g.Resources[0]) },
		"duplicate target":        func(g *AuthorityGrant) { g.Targets = append(g.Targets, g.Targets[1]) },
		"unsorted targets":        func(g *AuthorityGrant) { g.Targets[0], g.Targets[1] = g.Targets[1], g.Targets[0] },
		"missing member identity": func(g *AuthorityGrant) { g.Targets[1].MemberID = 0 },
		"transfer smuggling":      func(g *AuthorityGrant) { g.Action.TransferNodeName = "a"; g.Action.TransferReplicaID = 11 },
		"action on pure fence":    func(g *AuthorityGrant) { g.Purpose = AuthorityFence },
		"admission on effect":     func(g *AuthorityGrant) { g.AdmissionID = "admission" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bad := g.Clone()
			mutate(&bad)
			if err := bad.Validate(); err == nil {
				t.Fatal("invalid grant accepted")
			}
		})
	}
}

func TestAuthorityGrantVariantsAndOwnership(t *testing.T) {
	t.Parallel()
	g := testAuthorityGrant()
	other := g.Clone()
	if !g.Equal(other) {
		t.Fatal("clone differs")
	}
	other.Action.StepSeq++
	other.Resources[0].GroupID = "changed"
	other.Targets[0].ReplicaID++
	if g.Equal(other) || g.Action.StepSeq != 5 || g.Resources[0].GroupID != "orders" || g.Targets[0].ReplicaID != 11 {
		t.Fatal("grant aliases caller data")
	}
	fence := g.Clone()
	fence.Purpose = AuthorityFence
	fence.Action = nil
	if err := fence.Validate(); err != nil {
		t.Fatal(err)
	}
	transfer := g.Clone()
	transfer.Purpose = AuthorityTransfer
	transfer.OperatorID = ""
	transfer.RequestID = "request"
	transfer.Action.OperatorID = ""
	transfer.Action.Attempt = 0
	transfer.Action.StepSeq = 0
	transfer.Action.Kind = StepTransferLeader
	if err := transfer.Validate(); err != nil {
		t.Fatal(err)
	}
	readmit := fence.Clone()
	readmit.Purpose = AuthorityReadmit
	readmit.AdmissionID = "admission"
	readmit.Resources = []AuthorityResource{{Kind: AuthorityHost, GroupID: "orders", NodeName: "b"}}
	if err := readmit.Validate(); err != nil {
		t.Fatal(err)
	}
	founding := g.Clone()
	founding.Action.Kind = StepCreateFoundingGroup
	founding.Action.NodeName = ""
	founding.Action.ReplicaID = 0
	founding.Resources = append(founding.Resources, AuthorityResource{Kind: AuthorityHost, GroupID: "orders", NodeName: "a"}, AuthorityResource{Kind: AuthorityHost, GroupID: "orders", NodeName: "b"})
	if err := founding.Validate(); err != nil {
		t.Fatal(err)
	}
	founding.Resources = founding.Resources[:2]
	if err := founding.Validate(); err == nil {
		t.Fatal("founder without host fence accepted")
	}
}

func TestFenceAndEffectProofBindExactResource(t *testing.T) {
	t.Parallel()
	g := testAuthorityGrant()
	e := FenceEvidence{Key: g.Key, Resource: g.Resources[0], Receiver: Identity{ClusterID: "cluster", NodeName: "a", MemberID: 1, InstanceID: "run"}, ReplicaID: 11, AppliedIndex: 7, AppliedTerm: 2}
	if err := e.Validate(g); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*FenceEvidence){
		"successor":        func(e *FenceEvidence) { e.ReplicaID++ },
		"new member":       func(e *FenceEvidence) { e.Receiver.MemberID++ },
		"different group":  func(e *FenceEvidence) { e.Resource.GroupID = "other" },
		"no durable index": func(e *FenceEvidence) { e.AppliedIndex = 0 },
		"mixed proof":      func(e *FenceEvidence) { e.LocalSequence = 1 },
		"other revision":   func(e *FenceEvidence) { e.Key.Revision++ },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bad := e
			mutate(&bad)
			if bad.Validate(g) == nil {
				t.Fatal("foreign fence accepted")
			}
		})
	}
	r := EffectReceipt{Key: g.Key, Resource: g.Resources[0], Action: g.Clone().Action, Target: g.Targets[1], Outcome: EffectApplied, AppliedIndex: 8, AppliedTerm: 2}
	if err := r.Validate(g); err != nil {
		t.Fatal(err)
	}
	bad := r.Clone()
	bad.Action.StepSeq++
	if bad.Validate(g) == nil || r.Action.StepSeq != 5 {
		t.Fatal("receipt action rebound or aliased")
	}
	bad = r.Clone()
	bad.Target = g.Targets[0]
	if bad.Validate(g) == nil {
		t.Fatal("another replica's receipt accepted")
	}
	bad = r.Clone()
	bad.Outcome = EffectTransferStarted
	if bad.Validate(g) == nil {
		t.Fatal("transfer proof completed promotion")
	}
}

func TestHostProofAndRevocationStatusOwnership(t *testing.T) {
	t.Parallel()
	g := testAuthorityGrant()
	g.Action.Kind = StepCleanupReplicaState
	g.Resources = []AuthorityResource{{Kind: AuthorityHost, GroupID: "orders", NodeName: "b"}}
	e := FenceEvidence{Key: g.Key, Resource: g.Resources[0], Receiver: Identity{ClusterID: "cluster", NodeName: "b", MemberID: 2, InstanceID: "run"}, LocalSequence: 9}
	if err := e.Validate(g); err != nil {
		t.Fatal(err)
	}
	r := EffectReceipt{Key: g.Key, Resource: g.Resources[0], Action: g.Clone().Action, Target: g.Targets[1], Outcome: EffectApplied, LocalSequence: 10}
	if err := r.Validate(g); err != nil {
		t.Fatal(err)
	}
	r.LocalSequence = 0
	if r.Validate(g) == nil {
		t.Fatal("host receipt without durability accepted")
	}
	s := AuthorityStatus{OperatorID: g.OperatorID, RevocationRequested: true, Obligations: []FenceObligation{{Key: g.Key, Resource: e.Resource, Evidence: &e}}}
	copy := s.Clone()
	copy.Obligations[0].Evidence.LocalSequence++
	copy.Obligations[0].Key.Revision++
	if s.Obligations[0].Evidence.LocalSequence != 9 || s.Obligations[0].Key.Revision != 41 {
		t.Fatal("status evidence aliases")
	}
}
