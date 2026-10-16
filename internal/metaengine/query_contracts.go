package metaengine

import (
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// CollectionCheck is a quorum-read absence predicate, never historical acceptance.
type CollectionCheck struct {
	Untracked    bool   `json:"untracked"`
	AppliedIndex uint64 `json:"applied_index"`
}

func (q QueryRequest) validateScopedFields() error {
	if q.Version != 0 || q.Desired != nil || q.Native != nil || scopedPlanQuery(q.Kind) {
		return q.validatePlanFields()
	}
	if q.OperationID != "" {
		return q.validateScopedAuthority()
	}
	if q.Progress != nil || q.Kind == "progress" {
		if q.Kind != "progress" || q.Progress == nil || q.CollectionIdentity != nil ||
			q.Key != "" || q.OperatorID != "" || q.AuthorityKey != nil {
			return fault.ErrInvalid
		}
		return q.Progress.Validate()
	}
	if q.CollectionIdentity != nil || q.Kind == "collection_check" {
		return q.validateCollectionFields()
	}
	return nil
}

func (q QueryRequest) validateScopedAuthority() error {
	if q.Kind != "authority" || q.OperationID == "" || q.Key != "" || q.OperatorID != "" ||
		q.AuthorityKey != nil || q.Progress != nil || q.CollectionIdentity != nil {
		return fault.ErrInvalid
	}
	return nil
}

func (q QueryRequest) validateCollectionFields() error {
	id := q.CollectionIdentity
	if q.Kind != "collection_check" || id == nil || q.Key != "" || q.OperatorID != "" ||
		q.OperationID != "" || q.Progress != nil || q.AuthorityKey != nil || id.ReplicaID == 0 {
		return fault.ErrInvalid
	}
	if strings.TrimSpace(id.ClusterID) == "" || strings.ContainsRune(id.ClusterID, 0) ||
		strings.TrimSpace(id.NodeName) == "" || strings.ContainsRune(id.NodeName, 0) ||
		topology.ValidateID(id.GroupID) != nil {
		return fault.ErrInvalid
	}
	return nil
}
