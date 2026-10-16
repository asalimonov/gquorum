package metaengine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"sort"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// CleanupPage owns internal proof for the cleanup executor; ordinary public
// progress never exposes these receipts or collection tokens.
type CleanupPage struct {
	Records []cleanupRecord
	Next    string
}

const defaultCleanupPageLimit = 100

type cleanupCursor struct {
	Version uint32           `json:"version"`
	Last    storage.Identity `json:"last"`
}

// CleanupPage returns exact debt in stable identity order. It gains the active
// scoped state at the Task 78 schema cutover; no legacy debt is synthesized.
func (m *Machine) CleanupPage(after string, limit int) (CleanupPage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryScopedCleanupPage(nil, after, limit)
}

func queryScopedCleanupPage(s *scopedState, after string, limit int) (CleanupPage, error) {
	if err := (topology.ProgressQuery{Limit: limit}).Validate(); err != nil {
		return CleanupPage{}, err
	}
	if s == nil {
		return CleanupPage{}, fault.ErrUnsupported
	}
	pageLimit := limit
	if pageLimit == 0 {
		pageLimit = defaultCleanupPageLimit
	}
	last, err := decodeCleanupCursor(after, s.Target.ClusterID)
	if err != nil {
		return CleanupPage{}, err
	}
	rows := s.indexes.cleanupOrder
	start := sort.Search(len(rows), func(i int) bool { return compareReplicaIdentity(rows[i].Identity, last) > 0 })
	end := start + min(pageLimit, len(rows)-start)
	page := CleanupPage{Records: make([]cleanupRecord, 0, end-start)}
	for _, row := range rows[start:end] {
		page.Records = append(page.Records, row.clone())
	}
	if end < len(rows) {
		encoded, err := json.Marshal(cleanupCursor{Version: 1, Last: rows[end-1].Identity})
		if err != nil {
			return CleanupPage{}, err
		}
		page.Next = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return page, nil
}

func decodeCleanupCursor(after, cluster string) (storage.Identity, error) {
	if after == "" {
		return storage.Identity{}, nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(after)
	if err != nil {
		return storage.Identity{}, fault.ErrInvalid
	}
	var cursor cleanupCursor
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return storage.Identity{}, fault.ErrInvalid
	}
	if cursor.Version != 1 || cursor.Last.ClusterID != cluster || cursor.Last.NodeName == "" ||
		cursor.Last.ReplicaID == 0 || cursor.Last.GroupID == "" || decoder.Decode(new(any)) != io.EOF ||
		(topology.ProgressQuery{GroupID: cursor.Last.GroupID, NodeName: cursor.Last.NodeName}).Validate() != nil {
		return storage.Identity{}, fault.ErrInvalid
	}
	return cursor.Last, nil
}
