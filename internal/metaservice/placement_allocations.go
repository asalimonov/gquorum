package metaservice

import (
	"math"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Placement computes tentative fresh IDs from one. Rebase only newly allocated
// groups onto the retained authoritative floor before any staging or publication.
// Existing identities and expert Stage payloads are never rewritten here.
func raisePreparedGroupFloors(before topology.Document, prepared *topology.Document,
	floors map[string]uint64) error {
	for i := range prepared.Groups {
		group := &prepared.Groups[i]
		floor := floors[group.ID]
		if floor <= 1 || before.Group(group.ID) != nil {
			continue
		}
		offset := floor - 1
		if group.NextReplicaID == 0 || group.NextReplicaID > math.MaxUint64-offset {
			return fault.ErrInvalid
		}
		for j := range group.Replicas {
			replica := &group.Replicas[j]
			if replica.ReplicaID == 0 || replica.ReplicaID >= group.NextReplicaID {
				return fault.ErrInvalid
			}
			replica.ReplicaID += offset
		}
		group.NextReplicaID += offset
	}
	return nil
}
