//go:build !functional

package topology

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func testDocument() Document {
	return Document{Version: 1, ClusterID: "cluster", Placement: PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Nodes: []Node{
		{Name: "a", Type: "worker", State: NodeActive, Labels: map[string]string{"rack": "1"}},
		{Name: "b", Type: "observer", State: NodeActive},
		{Name: "c", Type: "worker", State: NodeActive},
	}, Groups: []Group{
		{ID: MetaGroupID, NextReplicaID: 4, Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: ReplicaVoter}, {NodeName: "b", ReplicaID: 2, Role: ReplicaVoter}, {NodeName: "c", ReplicaID: 3, Role: ReplicaVoter}}},
		{ID: "load", NextReplicaID: 3, Replicas: []Replica{{NodeName: "a", ReplicaID: 1, Role: ReplicaVoter}, {NodeName: "c", ReplicaID: 2, Role: ReplicaLearner}}, Placement: &PlacementRule{MinReplicas: 1, MaxReplicas: 2, AllowedNodeTypes: []NodeType{"worker"}}},
	}}
}
func testRules() Rules { return Rules{SchemaVersion: 1, Limits: (Limits{}).WithDefaults()} }

func TestDocumentLookupHelpers(t *testing.T) {
	t.Parallel()
	d := testDocument()
	if d.Node("a") == nil || d.Node("missing") != nil || d.Group("load") == nil || d.Group("missing") != nil || d.MemberID("a") != 1 || d.MemberID("missing") != 0 || len(d.LoadGroups()) != 1 {
		t.Fatal("document lookup failure")
	}
	if len(d.VotersOf("load")) != 1 || len(d.VotersOf("missing")) != 0 || QuorumOf(5) != 3 {
		t.Fatal("voter/quorum lookup failure")
	}
}

func TestDocumentCloneIsolation(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.Audit = &ChangeAudit{Reason: "original"}
	d.Placement.AllowedNodeTypes = []NodeType{"worker"}
	before := d.Clone()
	c := d.Clone()
	c.Nodes[0].Labels["rack"] = "2"
	c.Groups[0].Replicas[0].ReplicaID = 7
	c.Groups[1].Placement.AllowedNodeTypes[0] = "other"
	c.Placement.AllowedNodeTypes[0] = "other"
	c.Audit.Reason = "changed"
	if !reflect.DeepEqual(d, before) {
		t.Fatal("clone mutated source")
	}
	if (Document{}).Clone().Nodes != nil {
		t.Fatal("clone allocated nil slice")
	}
}

func TestGroupBounds(t *testing.T) {
	t.Parallel()
	d := testDocument()
	if a, b := d.GroupBounds("load"); a != 1 || b != 2 {
		t.Fatal(a, b)
	}
	if a, b := d.GroupBounds("unknown"); a != 1 || b != 3 {
		t.Fatal(a, b)
	}
}

func TestEffectiveNodeStateDoesNotMutate(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.Nodes[1].State = NodeDraining
	if EffectiveNodeState(d, "b") != NodeDrained || d.Node("b").State != NodeDraining {
		t.Fatal("drain modified declaration or counted meta")
	}
	d.Nodes[0].State = NodeDraining
	if EffectiveNodeState(d, "a") != NodeDraining || EffectiveNodeState(d, "missing") != "" {
		t.Fatal("incorrect effective state")
	}
}

func TestAllocateMemberID(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.MetaGroup().NextReplicaID = 0
	a, err := d.AllocateMemberID()
	if err != nil || a != 4 {
		t.Fatal(a, err)
	}
	b, err := d.AllocateMemberID()
	if err != nil || b != 5 {
		t.Fatal(b, err)
	}
	d.MetaGroup().NextReplicaID = math.MaxUint64
	if _, err := d.AllocateMemberID(); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("allocator wrapped", err)
	}
	if err := d.SetMemberID("a", math.MaxUint64, time.Time{}); err == nil {
		t.Fatal("overflow identity accepted")
	}
}

func TestDocumentValidate(t *testing.T) {
	t.Parallel()
	if err := testDocumentPointer().Validate(testRules()); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Document){
		"duplicate node":      func(d *Document) { d.Nodes[1].Name = "a" },
		"null node name":      func(d *Document) { d.Nodes[1].Name = "b\x00other" },
		"null cluster ID":     func(d *Document) { d.ClusterID = "cluster\x00other" },
		"empty type":          func(d *Document) { d.Nodes[1].Type = "" },
		"unknown state":       func(d *Document) { d.Nodes[1].State = "other" },
		"duplicate meta":      func(d *Document) { d.Groups = append(d.Groups, d.Groups[0]) },
		"duplicate meta host": func(d *Document) { d.Groups[0].Replicas[1].NodeName = "a" },
		"zero identity":       func(d *Document) { d.Groups[0].Replicas[0].ReplicaID = 0 },
		"counter floor":       func(d *Document) { d.Groups[0].NextReplicaID = 3 },
		"unknown host":        func(d *Document) { d.Groups[1].Replicas[0].NodeName = "missing" },
		"duplicate load host": func(d *Document) { d.Groups[1].Replicas[1].NodeName = "a" },
		"ineligible host":     func(d *Document) { d.Groups[1].Replicas[1].NodeName = "b" },
		"excluded load":       func(d *Document) { d.Nodes[0].State = NodeExcluded },
		"invalid role":        func(d *Document) { d.Groups[1].Replicas[0].Role = "witness" },
		"missing meta":        func(d *Document) { d.Groups = d.Groups[1:] },
		"missing member":      func(d *Document) { d.Groups[0].Replicas = d.Groups[0].Replicas[:2] },
		"no meta voters": func(d *Document) {
			for i := range d.Groups[0].Replicas {
				d.Groups[0].Replicas[i].Role = ReplicaLearner
			}
		},
		"meta placement":  func(d *Document) { d.Groups[0].Placement = &d.Placement },
		"unsafe group ID": func(d *Document) { d.Groups[1].ID = "../escape" },
		"invalid bounds":  func(d *Document) { d.Placement.MaxReplicas = 6 },
		"unsorted types":  func(d *Document) { d.Groups[1].Placement.AllowedNodeTypes = []NodeType{"worker", "observer"} },
		"forced audit":    func(d *Document) { d.Audit = &ChangeAudit{Forced: true} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			d := testDocument()
			mutate(&d)
			if err := d.Validate(testRules()); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal("invalid topology accepted", err)
			}
		})
	}
}
func testDocumentPointer() *Document { d := testDocument(); return &d }

func TestLimitsCapacityAndExcludedTombstones(t *testing.T) {
	t.Parallel()
	l := (Limits{}).WithDefaults()
	if l.MaxNodes != 3 || l.MaxReplicas != 5 || l.MaxPendingAuthorityRecords != 1024 {
		t.Fatal(l)
	}
	for _, v := range []Limits{{MaxNodes: 1, MaxReplicas: 1, MaxPendingAuthorityRecords: -1}, {MaxNodes: -1, MaxReplicas: 1}, {MaxNodes: 1, MaxReplicas: -1}, {}} {
		if v.Validate() == nil {
			t.Fatal("invalid limits", v)
		}
	}
	d := testDocument()
	d.Nodes = append(d.Nodes, Node{Name: "retired", Type: "worker", State: NodeExcluded})
	if err := d.Validate(testRules()); err != nil {
		t.Fatal("tombstone consumed capacity", err)
	}
	d.Nodes[3].State = NodeActive
	d.MetaGroup().Replicas = append(d.MetaGroup().Replicas, Replica{NodeName: "retired", ReplicaID: 4, Role: ReplicaLearner})
	d.MetaGroup().NextReplicaID = 5
	if err := d.Validate(testRules()); err == nil {
		t.Fatal("capacity exceeded")
	}
	r := testRules()
	r.Limits.MaxNodes = 7
	if err := d.Validate(r); err != nil {
		t.Fatal("larger caller limit ignored", err)
	}
}

func TestMetaIgnoresNodeTypeEligibility(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.Placement.AllowedNodeTypes = []NodeType{"worker"}
	if err := d.Validate(testRules()); err != nil {
		t.Fatal("observer was excluded from meta", err)
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var round Document
	if err := json.Unmarshal(raw, &round); err != nil || !reflect.DeepEqual(round, d) {
		t.Fatal("document roundtrip", err)
	}
}

func TestValidateTransitionRetiredIdentities(t *testing.T) {
	t.Parallel()
	before := testDocument()
	after := before.Clone()
	after.MetaGroup().Replicas[0].ReplicaID = 2
	if err := ValidateTransition(before, after, testRules(), nil); err == nil {
		t.Fatal("identity reused")
	}
	after = before.Clone()
	after.Nodes[0].Type = "observer"
	after.Groups = after.Groups[:1]
	if err := ValidateTransition(before, after, testRules(), nil); err == nil {
		t.Fatal("node type changed")
	}
	after = before.Clone()
	after.Group("load").NextReplicaID = 2
	if err := ValidateTransition(before, after, testRules(), nil); err == nil {
		t.Fatal("counter rewound")
	}
	after = before.Clone()
	if err := ValidateTransition(before, after, testRules(), map[string]bool{"load": true}); err == nil {
		t.Fatal("retired group accepted")
	}
}

func TestAuthorizedStepMatchesExactScope(t *testing.T) {
	t.Parallel()
	step := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "load", NodeName: "a", ReplicaID: 7}
	a := AuthorizedStep{OperatorID: "v2-a1", Version: 2, Attempt: 1, StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID}
	if !a.Matches("v2-a1", 1, 2, step) {
		t.Fatal("exact identity rejected")
	}
	for _, mutate := range []func(*AuthorizedStep){func(a *AuthorizedStep) { a.OperatorID = "v3-a1" }, func(a *AuthorizedStep) { a.Version++ }, func(a *AuthorizedStep) { a.Attempt++ }, func(a *AuthorizedStep) { a.StepSeq++ }, func(a *AuthorizedStep) { a.Kind = StepStopReplica }, func(a *AuthorizedStep) { a.GroupID = "other" }, func(a *AuthorizedStep) { a.NodeName = "b" }, func(a *AuthorizedStep) { a.ReplicaID++ }} {
		bad := a
		mutate(&bad)
		if bad.Matches(a.OperatorID, a.Attempt, a.Version, step) {
			t.Fatal("conflicting authorization accepted", bad)
		}
	}
}
