package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// CollectionCheck is a quorum-read absence predicate, never historical acceptance.
type CollectionCheck struct {
	Untracked    bool   `json:"untracked"`
	AppliedIndex uint64 `json:"applied_index"`
}

func (q QueryRequest) validateScopedFields() error {
	if q.OperationID != "" {
		// Remove with the old OperatorID query consumers at Task 66.
		return fault.ErrUnsupported
	}
	if q.Progress != nil || q.Kind == "progress" {
		if q.Kind != "progress" || q.Progress == nil || q.CollectionIdentity != nil ||
			q.Key != "" || q.OperatorID != "" || q.AuthorityKey != nil {
			return fault.ErrInvalid
		}
		return q.Progress.Validate()
	}
	if q.CollectionIdentity != nil || q.Kind == "collection_check" {
		id := q.CollectionIdentity
		if q.Kind != "collection_check" || id == nil || q.Key != "" || q.OperatorID != "" ||
			q.AuthorityKey != nil || id.ClusterID == "" || id.NodeName == "" || id.ReplicaID == 0 ||
			topology.ValidateID(id.GroupID) != nil {
			return fault.ErrInvalid
		}
	}
	return nil
}
