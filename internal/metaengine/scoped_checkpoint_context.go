package metaengine

import (
	"reflect"

	"github.com/asalimonov/gquorum/topology"
)

// A checkpoint can outlive its whole source document. Resolve only an approved
// identical declaration, with unambiguous durable member bindings. Pruning must
// compare those bindings to the original source before releasing that source.
func checkpointDocument(state *scopedState, index *scopedIndexes,
	row groupConvergence) (topology.Document, bool) {
	if source, found := index.documents[row.SourceVersion]; found {
		return source, true
	}
	if row.Declaration == nil {
		return topology.Document{}, false
	}
	var selected topology.Document
	for _, doc := range index.documents {
		if !scopedApprovedVersion(state, doc.Version) || !reflect.DeepEqual(doc.Group(row.GroupID), row.Declaration) {
			continue
		}
		if !sameGroupMembers(doc, doc, row.Declaration) ||
			selected.Version != 0 && !sameGroupMembers(selected, doc, row.Declaration) {
			return topology.Document{}, false
		}
		if doc.Version > selected.Version {
			selected = doc
		}
	}
	return selected, selected.Version != 0
}

func sameGroupMembers(a, b topology.Document, group *topology.Group) bool {
	for _, replica := range group.Replicas {
		member := a.MemberID(replica.NodeName)
		if member == 0 || member != b.MemberID(replica.NodeName) {
			return false
		}
	}
	return true
}
