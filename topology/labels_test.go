//go:build !functional

package topology

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestLabelReplacementOwnsInputsAndOutputs(t *testing.T) {
	t.Parallel()
	var n Node
	labels := n.GetLabels()
	if labels == nil || len(labels) != 0 {
		t.Fatalf("zero value labels = %#v", labels)
	}
	labels["network"] = "public_ip"
	if err := n.SetLabels(labels); err != nil {
		t.Fatal(err)
	}
	labels["network"] = "private"
	if got := n.GetLabels()["network"]; got != "public_ip" {
		t.Fatalf("setter retained input map: %q", got)
	}
	copyOfNode := n
	changed := copyOfNode.GetLabels()
	delete(changed, "network")
	if err := copyOfNode.SetLabels(changed); err != nil {
		t.Fatal(err)
	}
	if copyOfNode.Labels != nil || n.Labels["network"] != "public_ip" {
		t.Fatalf("clear or shallow copy changed original: %#v %#v", copyOfNode.Labels, n.Labels)
	}
	if err := n.SetLabels(nil); err != nil || n.Labels != nil || n.GetLabels() == nil {
		t.Fatalf("nil replacement did not clear: %#v %v", n.Labels, err)
	}
	var missing *Node
	if !errors.Is(missing.SetLabels(nil), fault.ErrInvalid) {
		t.Fatal("nil receiver accepted")
	}
}

func TestGroupLabelsAreIndependentOfPlacement(t *testing.T) {
	t.Parallel()
	d := testDocument()
	d.Placement.NodeSelector = map[string]string{"network": "public_ip"}
	g := d.Group("load")
	if err := g.SetLabels(map[string]string{"purpose": "accounting"}); err != nil {
		t.Fatal(err)
	}
	if got := d.GroupPlacement("load").NodeSelector; got != nil {
		t.Fatalf("override inherited document selector: %#v", got)
	}
	g.Placement = nil
	if got := d.GroupPlacement("load").NodeSelector["network"]; got != "public_ip" {
		t.Fatalf("nil override did not inherit selector: %q", got)
	}
	if err := d.Validate(testRules()); err != nil {
		t.Fatal(err)
	}
	clone := d.Clone()
	clone.Placement.NodeSelector["network"] = "private"
	clone.Group("load").Labels["purpose"] = "other"
	if d.Placement.NodeSelector["network"] != "public_ip" || g.Labels["purpose"] != "accounting" {
		t.Fatal("document clone shared label or selector map")
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var round Document
	if err := json.Unmarshal(raw, &round); err != nil || !reflect.DeepEqual(round, d) {
		t.Fatalf("round trip changed labels or selector: %v", err)
	}
}

func TestLabelValidationAppliesToDirectMapWrites(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		labels map[string]string
		scope  LabelScope
	}{
		{"reserved prefix", map[string]string{"gquorum.io/unknown": "x"}, NodeLabels},
		{"group restriction", map[string]string{LabelReserved: "true"}, GroupLabels},
		{"false restriction", map[string]string{LabelLearnerOnly: "false"}, NodeLabels},
		{"invalid utf8", map[string]string{"key": "\xff"}, NodeLabels},
		{"key whitespace", map[string]string{"a b": "x"}, NodeLabels},
		{"value control", map[string]string{"key": "a\x00b"}, NodeLabels},
		{"long key", map[string]string{strings.Repeat("k", 254): "x"}, NodeLabels},
		{"long value", map[string]string{"key": strings.Repeat("v", 1025)}, NodeLabels},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateLabels(tc.labels, tc.scope); !errors.Is(err, fault.ErrInvalid) {
				t.Fatalf("invalid labels accepted: %v", err)
			}
		})
	}
	tooMany := make(map[string]string)
	for i := 0; i < 65; i++ {
		tooMany[string(rune('a'+i))] = ""
	}
	if ValidateLabels(tooMany, NodeLabels) == nil {
		t.Fatal("too many labels accepted")
	}
	full := make(map[string]string)
	for i := 0; i < 17; i++ {
		full[strings.Repeat(string(rune('a'+i)), 30)] = strings.Repeat("v", 1000)
	}
	if ValidateLabels(full, NodeLabels) == nil {
		t.Fatal("oversized map accepted")
	}
	d := testDocument()
	d.Nodes[0].Labels[LabelReserved] = "false"
	if err := d.Validate(testRules()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("direct invalid write bypassed validation: %v", err)
	}
	bad := map[string]string{"gquorum.io/private": "x"}
	if ValidateSelector(bad) == nil {
		t.Fatal("selector accepted reserved namespace")
	}
}

func TestSetLabelsFailureIsAtomic(t *testing.T) {
	t.Parallel()
	n := Node{Labels: map[string]string{"valid": "old"}}
	before := maps.Clone(n.Labels)
	if err := n.SetLabels(map[string]string{"valid": "new", LabelReserved: ""}); err == nil {
		t.Fatal("invalid replacement accepted")
	}
	if !maps.Equal(n.Labels, before) {
		t.Fatalf("failed replacement changed labels: %#v", n.Labels)
	}
	g := Group{Labels: map[string]string{"purpose": "accounting"}}
	if err := g.SetLabels(map[string]string{LabelLearnerOnly: "true"}); err == nil || g.Labels["purpose"] != "accounting" {
		t.Fatal("group accepted restriction or changed labels")
	}
}
