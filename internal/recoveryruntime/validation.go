package recoveryruntime

import (
	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func validateImportedGroups(record *model.ImportState, manifest model.LocalManifest) error {
	if len(record.Groups) != len(manifest.Approved.LoadGroups()) {
		return fault.ErrIdentityMismatch
	}
	sources := make(map[string]model.SeedSource)
	for _, source := range manifest.Sources {
		if source.GroupID != topology.MetaGroupID {
			sources[source.GroupID] = source
		}
	}
	floors := make(map[string]uint64)
	for _, floor := range manifest.Floors {
		floors[floor.GroupID] = floor.NextID
	}
	for _, group := range record.Groups {
		source, exists := sources[group.GroupID]
		expected := storage.Identity{NodeName: manifest.Identity.NodeName, ClusterID: manifest.Identity.ClusterID,
			GroupID: group.GroupID, ReplicaID: floors[group.GroupID]}
		if !exists || source != group.Source || expected != group.Destination {
			return fault.ErrIdentityMismatch
		}
		delete(sources, group.GroupID)
	}
	if len(sources) != 0 {
		return fault.ErrIdentityMismatch
	}
	return nil
}
