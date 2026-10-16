package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Approval returns the original approval, or an empty ID for current unapproved
// intent. Missing proof is never treated as permission to allocate a new attempt.
func (m *Machine) Approval(version uint64) (topology.OperationID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Task78 replaces this schema3 read adapter with the scoped indexes below.
	id := topology.MakeOperatorID(version, 1)
	if operation := m.operators[id]; operation != nil {
		return topology.OperationID(id), nil
	}
	index := &scopedIndexes{documents: make(map[uint64]topology.Document), rebases: make(map[uint64]uint64)}
	for _, doc := range m.history {
		index.documents[doc.Version] = doc
		if doc.RebasedFrom != 0 {
			index.rebases[doc.RebasedFrom] = doc.Version
		}
	}
	index.documents[m.latest.Version] = m.latest
	return retainedApproval(version, topology.ReadView{Latest: m.latest, Target: m.target}, index)
}

func retainedApproval(version uint64, view topology.ReadView, index *scopedIndexes) (topology.OperationID, error) {
	if version == 0 {
		return "", fault.ErrInvalid
	}
	if id := index.first[version]; id != "" {
		return id, nil
	}
	successor := rebasedSuccessor(version, index)
	if successor != 0 {
		return "", &fault.Error{Code: fault.VersionConflict, ExpectedVersion: version,
			CurrentVersion: view.Latest.Version, RebasedTo: successor}
	}
	if version <= view.Latest.Version && (index.documents[version].Version == 0 ||
		index.latest[version] != "" || version <= view.Target.Version) {
		return "", fault.New(fault.HistoryUnavailable, "original approval evidence is not retained", nil)
	}
	if version != view.Latest.Version {
		return "", &fault.Error{Code: fault.VersionConflict, ExpectedVersion: version,
			CurrentVersion: view.Latest.Version}
	}
	return "", nil
}

func rebasedSuccessor(version uint64, index *scopedIndexes) uint64 {
	var successor uint64
	for next := index.rebases[version]; next != 0; next = index.rebases[next] {
		successor = next
	}
	return successor
}
