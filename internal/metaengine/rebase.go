package metaengine

import (
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type repairDocuments struct {
	Repair topology.Document
	Latest topology.Document
}

// FI_REBASE: generated times and declaration order do not claim ownership of
// another scope. Allocators and exact IDs remain part of the dependency check.
func rebaseSemanticDocument(doc topology.Document) topology.Document {
	owned := doc.Clone()
	owned.Version, owned.ParentVersion, owned.RebasedFrom, owned.CreatedAt = 0, 0, 0, time.Time{}
	if owned.Audit != nil {
		owned.Audit.StagedAt = time.Time{}
	}
	for i := range owned.Nodes {
		owned.Nodes[i].CreatedAt, owned.Nodes[i].UpdatedAt = time.Time{}, time.Time{}
	}
	for i := range owned.Groups {
		group := &owned.Groups[i]
		group.CreatedAt, group.UpdatedAt = time.Time{}, time.Time{}
		for j := range group.Replicas {
			group.Replicas[j].CreatedAt, group.Replicas[j].UpdatedAt = time.Time{}, time.Time{}
		}
	}
	canonicalizeDocument(&owned)
	return owned
}

// Reapply the complete staged semantic delta, including additions/deletions.
// Existing scope/read-set checks prohibit conflicting repair writes first.
func rebaseStaged(base, staged, repair topology.Document) (topology.Document, error) {
	if err := ValidateRepairIndependence(base, staged, repair); err != nil {
		return topology.Document{}, err
	}
	before, intent := rebaseSemanticDocument(base), rebaseSemanticDocument(staged)
	merged, edits := repair.Clone(), staged.Clone()
	if !reflect.DeepEqual(before.Placement, intent.Placement) {
		merged.Placement = edits.Placement
	}
	merged.Nodes = slices.DeleteFunc(merged.Nodes, func(n topology.Node) bool {
		return before.Node(n.Name) != nil && intent.Node(n.Name) == nil
	})
	for _, node := range edits.Nodes {
		if reflect.DeepEqual(before.Node(node.Name), intent.Node(node.Name)) {
			continue
		}
		if current := merged.Node(node.Name); current != nil {
			*current = node
		} else {
			merged.Nodes = append(merged.Nodes, node)
		}
	}
	merged.Groups = slices.DeleteFunc(merged.Groups, func(g topology.Group) bool {
		return before.Group(g.ID) != nil && intent.Group(g.ID) == nil
	})
	for _, group := range edits.Groups {
		if reflect.DeepEqual(before.Group(group.ID), intent.Group(group.ID)) {
			continue
		}
		if current := merged.Group(group.ID); current != nil {
			*current = group
		} else {
			merged.Groups = append(merged.Groups, group)
		}
	}
	merged.ChangedBy, merged.Origin = edits.ChangedBy, edits.Origin
	merged.Comment, merged.Audit = edits.Comment, edits.Audit
	canonicalizeDocument(&merged)
	return merged, nil
}

func (s *scopedState) prepareRepairDocuments(candidate topology.Document, at time.Time) (repairDocuments, error) {
	if s.Target.Version == 0 || s.Latest.Version < s.Target.Version || s.Latest.Version == math.MaxUint64 ||
		at.IsZero() || at.Before(s.Latest.CreatedAt) || at.Before(s.Target.CreatedAt) {
		return repairDocuments{}, fault.ErrInvalid
	}
	repair := candidate.Clone()
	repair.Version, repair.ParentVersion, repair.RebasedFrom = s.Latest.Version+1, s.Target.Version, 0
	repair.CreatedAt = at
	canonicalizeDocument(&repair)
	if err := s.validateRepairCandidate(s.Target, repair); err != nil {
		return repairDocuments{}, err
	}
	pair := repairDocuments{Repair: repair, Latest: repair.Clone()}
	if reflect.DeepEqual(rebaseSemanticDocument(s.Target), rebaseSemanticDocument(s.Latest)) {
		return pair, s.validateRebasedFloors(pair.Latest)
	}
	if repair.Version == math.MaxUint64 {
		return repairDocuments{}, fault.ErrInvalid
	}
	staged, err := rebaseStaged(s.Target, s.Latest, repair)
	if err != nil {
		return repairDocuments{}, err
	}
	staged.Version, staged.ParentVersion, staged.RebasedFrom = repair.Version+1, repair.Version, s.Latest.Version
	staged.CreatedAt = at.Add(time.Nanosecond)
	if err := s.validateRepairCandidate(repair, staged); err != nil {
		return repairDocuments{}, err
	}
	pair.Latest = staged
	return pair, s.validateRebasedFloors(pair.Latest)
}

func (s *scopedState) validateRepairCandidate(base, doc topology.Document) error {
	retired := make(map[string]bool, len(s.RetiredGroups))
	for _, group := range s.RetiredGroups {
		retired[group.ID] = true
	}
	if err := topology.ValidateTransition(base, doc, s.Rules, retired); err != nil {
		return err
	}
	if err := topology.ValidateHostingCapacityFor(base, doc, s.Rules); err != nil {
		return err
	}
	if err := topology.ValidatePlacementTransition(base, base, doc); err != nil {
		return err
	}
	return s.validateRepairAllocations(doc)
}

func (s *scopedState) validateRepairAllocations(doc topology.Document) error {
	for _, allocation := range s.Allocations {
		group := doc.Group(allocation.GroupID)
		if group == nil {
			continue
		}
		for _, replica := range group.Replicas {
			if replica.ReplicaID >= allocation.NextReplicaID {
				continue
			}
			retained := func(view topology.Document) bool {
				old := documentReplica(view, group.ID, replica.ReplicaID)
				return old != nil && old.NodeName == replica.NodeName
			}
			if !retained(s.Target) && !retained(s.Latest) {
				return fault.New(fault.VersionConflict, "repair/rebase reuses a historical replica identity", nil)
			}
		}
	}
	return nil
}

func (s *scopedState) validateRebasedFloors(latest topology.Document) error {
	for _, floor := range s.Allocations {
		if group := latest.Group(floor.GroupID); group != nil && group.NextReplicaID < floor.NextReplicaID {
			return fault.New(fault.Invalid, "rebased Latest regresses an accepted allocation", nil)
		}
	}
	return nil
}
