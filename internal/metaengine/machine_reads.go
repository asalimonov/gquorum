package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

// HasActivated reports retained activation evidence for a node name.
func (m *Machine) HasActivated(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Contains(m.scoped.Activated, name)
}

// Latest returns an owned copy of staged intent.
func (m *Machine) Latest() topology.Document {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.scoped.Latest.Clone()
}

// Target returns an owned copy of approved intent.
func (m *Machine) Target() topology.Document {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.scoped.Target.Clone()
}

// History returns retained documents newest first. Nonpositive limits include
// all retained entries after the offset; negative offsets start at the newest.
func (m *Machine) History(offset, limit int) []topology.Document {
	m.mu.RLock()
	defer m.mu.RUnlock()
	history := m.scoped.History
	position := max(offset, 0)
	n := len(history) - position
	if n <= 0 {
		return nil
	}
	if limit > 0 {
		n = min(n, limit)
	}
	out := make([]topology.Document, 0, n)
	for i := range n {
		out = append(out, history[len(history)-1-position-i].Clone())
	}
	return out
}

// Get returns an owned retained document at the exact requested version.
func (m *Machine) Get(version uint64) (topology.Document, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	doc, ok := m.scoped.indexes.documents[version]
	return doc.Clone(), ok
}

// AppliedState returns the locally persisted metadata boundary.
func (m *Machine) AppliedState() (index, term uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.scoped.AppliedIndex, m.scoped.AppliedTerm
}

// Sessions returns an owned snapshot for local expiration processing.
func (m *Machine) Sessions() []SessionRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Clone(m.scoped.Sessions)
}

// Admission returns the exact retained process record, including terminal ones.
func (m *Machine) Admission(name, instance string) (AdmissionRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, record := range m.scoped.Admissions {
		if record.NodeName == name && record.InstanceID == instance {
			return record, true
		}
	}
	return AdmissionRecord{}, false
}
