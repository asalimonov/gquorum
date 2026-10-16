package metaengine

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Indexes refer only to owned decoded rows. They are rebuilt, never serialized,
// and confer no authority before the complete restore graph/proof validation.
type scopedIndexes struct {
	groups       map[string]*groupConvergence
	nodes        map[string]*nodeConvergence
	operations   map[topology.OperationID]*topology.Operation
	first        map[uint64]topology.OperationID
	latest       map[uint64]topology.OperationID
	retries      map[topology.OperationID]topology.OperationID
	cleanup      map[storage.Identity]*cleanupRecord
	cleanupOrder []*cleanupRecord
	intents      map[intentKey]*Intent
	documents    map[uint64]topology.Document
	rebases      map[uint64]uint64
	grants       map[topology.AuthorityKey]*scopedAuthorityRecord
	obligations  map[topology.AuthorityResource][]topology.AuthorityKey
}

type intentKey struct {
	ExpectedVersion uint64
	Kind            string
	Digest          string
}

func (s *scopedState) buildIndexes() (*scopedIndexes, error) {
	index := &scopedIndexes{
		first: map[uint64]topology.OperationID{}, latest: map[uint64]topology.OperationID{},
		retries:   map[topology.OperationID]topology.OperationID{},
		documents: map[uint64]topology.Document{}, rebases: map[uint64]uint64{},
	}
	err := errors.Join(
		indexRows(s.Groups, &index.groups, func(r *groupConvergence) string { return r.GroupID }),
		indexRows(s.Nodes, &index.nodes, func(r *nodeConvergence) string { return r.NodeName }),
		indexRows(s.Operations, &index.operations, func(r *topology.Operation) topology.OperationID {
			return r.Status.ID
		}),
		indexRows(s.Cleanup, &index.cleanup, func(r *cleanupRecord) storage.Identity { return r.Identity }),
		indexRows(s.Intents, &index.intents, func(r *Intent) intentKey {
			return intentKey{r.ExpectedVersion, r.Kind, r.Digest}
		}),
		indexRows(s.AuthorityRecords, &index.grants, func(r *scopedAuthorityRecord) topology.AuthorityKey {
			return r.Grant.Key
		}),
	)
	if err != nil {
		return nil, err
	}
	if err := index.indexAttempts(s.Operations); err != nil {
		return nil, err
	}
	if err := index.indexDocuments(s); err != nil {
		return nil, err
	}
	index.obligations = scopedResourceObligations(s)
	index.cleanupOrder = slices.SortedFunc(maps.Values(index.cleanup), func(a, b *cleanupRecord) int {
		return compareReplicaIdentity(a.Identity, b.Identity)
	})
	return index, nil
}

func indexRows[T any, K comparable](rows []T, into *map[K]*T, key func(*T) K) error {
	indexed := make(map[K]*T, len(rows))
	for i := range rows {
		k := key(&rows[i])
		if _, exists := indexed[k]; exists {
			return fault.New(fault.Unreadable, fmt.Sprintf("duplicate %T lookup key %v", rows[i], k), nil)
		}
		indexed[k] = &rows[i]
	}
	*into = indexed
	return nil
}

func (index *scopedIndexes) indexAttempts(operations []topology.Operation) error {
	for _, operation := range operations {
		status := operation.Status
		if err := index.indexOriginal(operation); err != nil {
			return err
		}
		latest := index.operations[index.latest[status.Version]]
		if latest == nil || latest.Status.Attempt < status.Attempt {
			index.latest[status.Version] = status.ID
		}
		if operation.Previous == "" {
			continue
		}
		if _, exists := index.retries[operation.Previous]; exists {
			return fault.New(fault.Unreadable, "multiple retries of one operation", nil)
		}
		index.retries[operation.Previous] = status.ID
	}
	return nil
}

func (index *scopedIndexes) indexOriginal(operation topology.Operation) error {
	status := operation.Status
	if status.Version == 0 || status.Attempt == 0 ||
		status.ID != topology.MakeOperationID(status.Version, status.Attempt) {
		return fault.New(fault.Unreadable, "invalid operation index identity", nil)
	}
	if operation.Previous == "" {
		if status.Attempt != 1 || index.first[status.Version] != "" {
			return fault.New(fault.Unreadable, "ambiguous original approval", nil)
		}
		index.first[status.Version] = status.ID
	}
	return nil
}

func (index *scopedIndexes) indexDocuments(state *scopedState) error {
	if err := index.indexDocument(state.Latest); err != nil {
		return err
	}
	if err := index.indexDocument(state.Target); err != nil {
		return err
	}
	for i := range state.History {
		if err := index.indexDocument(state.History[i]); err != nil {
			return err
		}
	}
	return nil
}

func (index *scopedIndexes) indexDocument(doc topology.Document) error {
	if doc.Version == 0 {
		return nil // Empty founding views have no durable version key.
	}
	if existing, found := index.documents[doc.Version]; found && !reflect.DeepEqual(existing, doc) {
		return fault.New(fault.Unreadable, "conflicting declarations for one document version", nil)
	}
	// Documents are immutable values, independent of a subsequently copied state.
	index.documents[doc.Version] = doc.Clone()
	if doc.RebasedFrom == 0 {
		return nil
	}
	if successor := index.rebases[doc.RebasedFrom]; doc.RebasedFrom >= doc.Version ||
		successor != 0 && successor != doc.Version {
		return fault.New(fault.Unreadable, "conflicting rebase successor", nil)
	}
	index.rebases[doc.RebasedFrom] = doc.Version
	return nil
}
