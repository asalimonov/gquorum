package metaengine

import (
	"cmp"
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (s scopedState) canonical() (scopedState, error) {
	if s.SchemaVersion != scopedStateSchemaVersion {
		return scopedState{}, fault.ErrUnsupported
	}
	for _, record := range s.AuthorityRecords {
		if record.Class != "execution" && record.Class != "cleanup" {
			return scopedState{}, fault.New(fault.Unreadable, "invalid authority class", nil)
		}
	}
	err := errors.Join(s.sortProgress(), s.sortEvidence(), s.sortUnchanged())
	if s.Recovery != nil {
		owned := model.CloneImport(*s.Recovery)
		err = errors.Join(err, sortStateRows(&owned.Groups, func(a,
			b model.ImportGroup) int {
			return cmp.Compare(a.GroupID, b.GroupID)
		}),
			sortStateRows(&owned.Policies, func(a, b model.PolicyState) int {
				return cmp.Compare(a.GroupID,
					b.GroupID)
			}))
		s.Recovery = &owned
	}
	return s, err
}

func (s *scopedState) sortProgress() error {
	return errors.Join(
		sortStateRows(&s.History, func(a, b topology.Document) int { return cmp.Compare(a.Version, b.Version) }),
		sortStateRows(&s.Operations, func(a, b topology.Operation) int {
			return cmp.Or(cmp.Compare(a.Status.Version, b.Status.Version),
				cmp.Compare(a.Status.Attempt, b.Status.Attempt))
		}),
		sortStateRows(&s.Groups, func(a, b groupConvergence) int { return cmp.Compare(a.GroupID, b.GroupID) }),
		sortStateRows(&s.Nodes, func(a, b nodeConvergence) int { return cmp.Compare(a.NodeName, b.NodeName) }),
		sortStateRows(&s.Cleanup, func(a, b cleanupRecord) int {
			return compareReplicaIdentity(a.Identity, b.Identity)
		}),
		sortStateRows(&s.Intents, func(a, b Intent) int { return cmp.Compare(a.Version, b.Version) }),
	)
}

func (s *scopedState) sortEvidence() error {
	return errors.Join(
		sortStateRows(&s.AuthorityRecords, func(a, b scopedAuthorityRecord) int {
			return compareAuthorityKey(a.Grant.Key, b.Grant.Key)
		}),
		sortStateRows(&s.AuthorityFloors, func(a, b resourceFloor) int {
			return a.Resource.Compare(b.Resource)
		}),
		sortStateRows(&s.Revocations, func(a, b scopedRevocationRecord) int {
			return cmp.Or(cmp.Compare(a.OperationID, b.OperationID), cmp.Compare(a.GroupID, b.GroupID))
		}),
		sortStateRows(&s.RetiredGroups, func(a, b retiredGroup) int { return cmp.Compare(a.ID, b.ID) }),
		sortStateRows(&s.Allocations, func(a, b allocationRecord) int { return cmp.Compare(a.GroupID, b.GroupID) }),
	)
}

func (s *scopedState) sortUnchanged() error {
	return errors.Join(
		sortStateRows(&s.Admissions, func(a, b AdmissionRecord) int {
			return cmp.Or(cmp.Compare(a.NodeName, b.NodeName), cmp.Compare(a.InstanceID, b.InstanceID))
		}),
		sortStateRows(&s.Sessions, func(a, b sessionState) int { return cmp.Compare(a.SessionID, b.SessionID) }),
		sortStateRows(&s.Locks, func(a, b lockEntry) int { return cmp.Compare(a.Key, b.Key) }),
		sortStateRows(&s.Activated, cmp.Compare[string]),
		sortStateRows(&s.VoterSwitches, func(a, b topology.VoterSwitchState) int {
			return cmp.Compare(a.GroupID, b.GroupID)
		}),
	)
}

// Clone before sorting so snapshot serialization never mutates owned live state.
// Duplicate keys are corrupt evidence, not a compaction opportunity.
func sortStateRows[T any](rows *[]T, compare func(T, T) int) error {
	owned := slices.Clone(*rows)
	slices.SortFunc(owned, compare)
	for i := 1; i < len(owned); i++ {
		if compare(owned[i-1], owned[i]) == 0 {
			return fault.New(fault.Unreadable, "duplicate metadata snapshot key", nil)
		}
	}
	*rows = owned
	return nil
}

func compareReplicaIdentity(a, b storage.Identity) int {
	return cmp.Or(cmp.Compare(a.ClusterID, b.ClusterID), cmp.Compare(a.GroupID, b.GroupID),
		cmp.Compare(a.NodeName, b.NodeName), cmp.Compare(a.ReplicaID, b.ReplicaID))
}
