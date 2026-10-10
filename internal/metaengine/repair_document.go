package metaengine

import (
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ReplicaRepairDocument is shared by operator reseating and automatic repair.
// It replaces one exact Active voter while preserving placement and audit.
func ReplicaRepairDocument(active, latest topology.Document, groupID, nodeName string, replicaID uint64, at time.Time, origin topology.Origin, actor string) (topology.Document, uint64, error) {
	if groupID == topology.MetaGroupID || at.IsZero() || latest.Version == math.MaxUint64 || (origin != topology.OriginOperator && origin != topology.OriginReconciler) {
		return topology.Document{}, 0, fault.ErrInvalid
	}
	repair := active.Clone()
	group := repair.Group(groupID)
	if group == nil {
		return topology.Document{}, 0, fault.ErrExecutionAuthorization
	}
	for i := range group.Replicas {
		r := &group.Replicas[i]
		if r.NodeName != nodeName || r.ReplicaID != replicaID || r.Role != topology.ReplicaVoter {
			continue
		}
		if newer := latest.Group(groupID); newer != nil {
			group.NextReplicaID = max(group.NextReplicaID, newer.NextReplicaID)
		}
		fresh, err := group.AllocateReplicaID()
		if err != nil {
			return topology.Document{}, 0, err
		}
		r.ReplicaID = fresh
		r.UpdatedAt = at
		group.UpdatedAt = at
		if node := repair.Node(nodeName); node != nil {
			node.UpdatedAt = at
		}
		repair.Version = latest.Version + 1
		repair.ParentVersion = active.Version
		repair.Origin = origin
		repair.ChangedBy = actor
		if err = validateRepairDocument(active, repair); err != nil {
			return topology.Document{}, 0, fault.New(fault.Invalid, "invalid replica repair", err)
		}
		return repair, fresh, nil
	}
	return topology.Document{}, 0, fault.ErrExecutionAuthorization
}
