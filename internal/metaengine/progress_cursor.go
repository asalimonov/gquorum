package metaengine

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

const (
	progressGroup = iota + 1
	progressNode
	progressOperation
	progressCleanup
)

// Separate tuple components avoid separator collisions and order replica IDs numerically.
type progressKey struct {
	Kind      int    `json:"kind"`
	ID        string `json:"id"`
	Scope     string `json:"scope,omitempty"`
	ReplicaID uint64 `json:"replica_id,omitempty"`
}

type progressCursor struct {
	Version uint32      `json:"version"`
	Last    progressKey `json:"last"`
	Filters string      `json:"filters"`
}

func compareProgressKeys(a, b progressKey) int {
	return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.ID, b.ID),
		cmp.Compare(a.Scope, b.Scope), cmp.Compare(a.ReplicaID, b.ReplicaID))
}

func progressFilterHash(cluster string, q topology.ProgressQuery) string {
	// Limits may change between pages; filters and cluster identity may not.
	data, _ := json.Marshal([]string{cluster, q.GroupID, q.NodeName, string(q.OperationID)})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func decodeProgressCursor(after, filters string) (progressKey, error) {
	if after == "" {
		return progressKey{}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(after)
	if err != nil {
		return progressKey{}, fault.ErrInvalid
	}
	var cursor progressCursor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return progressKey{}, fault.ErrInvalid
	}
	if cursor.Version != 1 || cursor.Filters != filters || decoder.Decode(new(any)) != io.EOF ||
		!validProgressKey(cursor.Last) {
		return progressKey{}, fault.ErrInvalid
	}
	return cursor.Last, nil
}

func validProgressKey(key progressKey) bool {
	if key.ID == "" {
		return false
	}
	var query topology.ProgressQuery
	switch key.Kind {
	case progressGroup:
		query.GroupID = key.ID
	case progressNode:
		query.NodeName = key.ID
	case progressOperation:
		query.OperationID, query.GroupID = topology.OperationID(key.ID), key.Scope
	case progressCleanup:
		query.GroupID, query.NodeName = key.ID, key.Scope
	default:
		return false
	}
	return (key.Kind < progressOperation && key.Scope == "" || key.Kind >= progressOperation && key.Scope != "") &&
		(key.Kind == progressCleanup && key.ReplicaID != 0 || key.Kind != progressCleanup && key.ReplicaID == 0) &&
		query.Validate() == nil
}

func encodeProgressCursor(last progressKey, filters string) string {
	data, _ := json.Marshal(progressCursor{Version: 1, Last: last, Filters: filters})
	return base64.RawURLEncoding.EncodeToString(data)
}
