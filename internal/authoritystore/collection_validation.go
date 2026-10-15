package authoritystore

import (
	"encoding/binary"
	"math"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func collectionKey(id storage.Identity) ([]byte, error) {
	if len(id.GroupID) > math.MaxUint32 || len(id.NodeName) > math.MaxUint32 || !validCollectionIdentity(id) {
		return nil, fault.ErrInvalid
	}
	key := appendUint32Length(nil, len(id.GroupID))
	key = append(key, id.GroupID...)
	key = appendUint32Length(key, len(id.NodeName))
	key = append(key, id.NodeName...)
	return binary.BigEndian.AppendUint64(key, id.ReplicaID), nil
}

func validCollectionText(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.ContainsRune(value, 0)
}

func validCollectionIdentity(id storage.Identity) bool {
	return id.ReplicaID != 0 && validCollectionText(id.ClusterID) && validCollectionText(id.NodeName) &&
		topology.ValidateID(id.GroupID) == nil
}

// Validate checks receipt shape, not whether the claimed deletion happened.
func (r CollectionReceipt) Validate() error {
	if r.MemberID == 0 || r.LocalSequence == 0 || r.RetirementKey.Revision == 0 ||
		r.RetirementKey.ClusterID != r.Identity.ClusterID || r.RetirementKey.GroupID != r.Identity.GroupID ||
		r.CollectedAt.IsZero() || !validCollectionIdentity(r.Identity) || !validCollectionText(r.Token) {
		return fault.ErrInvalid
	}
	return nil
}

// Validate checks the complete retained record against its host identity pin.
// Former members remain valid after rebinding; this never authorizes a new effect.
func (r CollectionRecord) Validate(identity topology.Identity) error {
	pin := pin(identity)
	id := r.Retired.Identity
	if id.ClusterID != pin.ClusterID || id.NodeName != pin.NodeName || r.MemberID > pin.MemberID {
		return fault.ErrIdentityMismatch
	}
	if r.MemberID == 0 || r.Sequence == 0 ||
		!validCollectionText(r.Retired.Token) || r.Retired.QuarantinedAt.IsZero() {
		return fault.ErrInvalid
	}
	if _, err := collectionKey(id); err != nil {
		return err
	}
	if err := validateCollectionRetirement(r); err != nil {
		return err
	}
	switch r.State {
	case collectionPrepared:
		if r.CollectedAt != nil {
			return fault.ErrUnreadable
		}
	case collectionCollected:
		if r.CollectedAt == nil || r.CollectedAt.IsZero() {
			return fault.ErrUnreadable
		}
	default:
		return fault.ErrUnreadable
	}
	return nil
}

func validateCollectionRetirement(r CollectionRecord) error {
	id, receipt := r.Retired.Identity, r.Retirement
	target := topology.AuthorityTarget{NodeName: id.NodeName, MemberID: r.MemberID, ReplicaID: id.ReplicaID}
	if r.Grant.Key.ClusterID != id.ClusterID || r.Grant.Key.GroupID != id.GroupID ||
		receipt.Target != target ||
		receipt.Resource != resource(id.GroupID, id.NodeName) {
		return fault.ErrIdentityMismatch
	}
	if err := receipt.Validate(r.Grant); err != nil {
		return err
	}
	if r.Grant.Purpose == topology.AuthorityReadmit {
		return nil
	}
	if r.Grant.Purpose != topology.AuthorityEffect || r.Grant.Action == nil {
		return fault.ErrInvalid
	}
	if r.Grant.Action.Kind != topology.StepCleanupReplicaState && r.Grant.Action.Kind != topology.StepDeleteGroupState {
		return fault.ErrInvalid
	}
	return nil
}
