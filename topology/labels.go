package topology

import (
	"maps"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Reserved node-label keys and internal settings for label validation.
const (
	LabelReserved    = "gquorum.io/reserved"
	LabelLearnerOnly = "gquorum.io/learner-only"

	maxLabels          = 64
	maxLabelBytes      = 16 * 1024
	maxLabelValueBytes = 1024
	maxLabelKeyBytes   = 253
	enabledNodeLabel   = "true"
)

// LabelScope distinguishes application labels from node restriction labels.
type LabelScope uint8

const (
	// GroupLabels excludes library-owned node restriction keys.
	GroupLabels LabelScope = iota
	// NodeLabels permits the library-owned node restriction keys.
	NodeLabels
)

// GetLabels returns a writable, owned copy of the node's labels.
func (n Node) GetLabels() map[string]string { return ownedLabels(n.Labels) }

// SetLabels validates and replaces the complete node label set.
func (n *Node) SetLabels(labels map[string]string) error {
	if n == nil {
		return invalid("nil node")
	}
	next := maps.Clone(labels)
	if err := ValidateLabels(next, NodeLabels); err != nil {
		return err
	}
	if len(next) == 0 {
		next = nil
	}
	n.Labels = next
	return nil
}

// GetLabels returns a writable, owned copy of the group's descriptive labels.
func (g Group) GetLabels() map[string]string { return ownedLabels(g.Labels) }

// SetLabels validates and replaces the complete group label set.
func (g *Group) SetLabels(labels map[string]string) error {
	if g == nil {
		return invalid("nil group")
	}
	next := maps.Clone(labels)
	if err := ValidateLabels(next, GroupLabels); err != nil {
		return err
	}
	if len(next) == 0 {
		next = nil
	}
	g.Labels = next
	return nil
}

func ownedLabels(labels map[string]string) map[string]string {
	out := maps.Clone(labels)
	if out == nil {
		return make(map[string]string)
	}
	return out
}

// ValidateLabels applies the bounded label schema. Only nodes may carry the two
// library-owned restriction keys, and their only accepted value is "true".
func ValidateLabels(labels map[string]string, scope LabelScope) error {
	if len(labels) > maxLabels {
		return invalid("too many labels")
	}
	total := 0
	for key, value := range labels {
		if err := validateLabelKey(key, scope); err != nil {
			return err
		}
		if err := validateLabelValue(key, value); err != nil {
			return err
		}
		total += len(key) + len(value)
		if total > maxLabelBytes {
			return invalid("label map exceeds byte limit")
		}
	}
	return nil
}

// ValidateSelector uses the same size and value bounds as labels, but never
// permits library-owned restriction keys in placement requirements.
func ValidateSelector(selector map[string]string) error {
	return ValidateLabels(selector, GroupLabels)
}

func validateLabelKey(key string, scope LabelScope) error {
	if len(key) == 0 || len(key) > maxLabelKeyBytes || !utf8.ValidString(key) {
		return invalid("invalid label key")
	}
	for _, r := range key {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return invalid("invalid label key")
		}
	}
	if (scope != NodeLabels || key != LabelReserved && key != LabelLearnerOnly) &&
		strings.HasPrefix(key, "gquorum.io/") {
		return invalid("reserved label key")
	}
	return nil
}

func validateLabelValue(key, value string) error {
	if len(value) > maxLabelValueBytes || !utf8.ValidString(value) {
		return invalid("invalid label value")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return invalid("invalid label value")
		}
	}
	if key == LabelReserved || key == LabelLearnerOnly {
		if value != "true" {
			return invalid("invalid node restriction value")
		}
	}

	return nil
}
