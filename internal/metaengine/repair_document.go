package metaengine

import (
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ReplicaRepairOptions identifies the Active voter and the repair audit details.
type ReplicaRepairOptions struct {
	GroupID, NodeName string
	ReplicaID         uint64
	At                time.Time
	Origin            topology.Origin
	Actor             string
}

// ReplicaRepairResult contains the owned repair document and its new replica ID.
type ReplicaRepairResult struct {
	Doc       topology.Document
	ReplicaID uint64
}

// ReplicaRepairDocument is shared by operator reseating and automatic repair.
// It replaces one exact Active voter while preserving placement and audit.
func ReplicaRepairDocument(active, latest topology.Document,
	options ReplicaRepairOptions) (ReplicaRepairResult, error) {
	if !options.valid(latest.Version) {
		return ReplicaRepairResult{}, fault.ErrInvalid
	}
	repair := active.Clone()
	fresh, err := replaceRepairVoter(&repair, latest, options)
	if err != nil {
		return ReplicaRepairResult{}, err
	}
	finishRepair(&repair, active.Version, latest.Version, options)
	if err = validateRepairDocument(active, repair); err != nil {
		return ReplicaRepairResult{}, fault.New(fault.Invalid, "invalid replica repair", err)
	}
	return ReplicaRepairResult{Doc: repair, ReplicaID: fresh}, nil
}

func (o ReplicaRepairOptions) valid(latestVersion uint64) bool {
	if o.GroupID == topology.MetaGroupID {
		return false
	}
	if o.At.IsZero() {
		return false
	}
	if latestVersion == math.MaxUint64 {
		return false
	}
	return o.Origin == topology.OriginOperator || o.Origin == topology.OriginReconciler
}

func findRepairVoter(repair *topology.Document, options ReplicaRepairOptions) (*topology.Group, *topology.Replica) {
	group := repair.Group(options.GroupID)
	if group == nil {
		return nil, nil
	}
	for i := range group.Replicas {
		r := &group.Replicas[i]
		if r.NodeName == options.NodeName && r.ReplicaID == options.ReplicaID && r.Role == topology.ReplicaVoter {
			return group, r
		}
	}
	return group, nil
}

func replaceRepairVoter(repair *topology.Document, latest topology.Document,
	options ReplicaRepairOptions) (uint64, error) {
	group, replica := findRepairVoter(repair, options)
	if replica == nil {
		return 0, fault.ErrOperatorAuthorization
	}
	fresh, err := allocateRepairID(group, latest, options.GroupID)
	if err != nil {
		return 0, err
	}
	replica.ReplicaID = fresh
	stampRepairVoter(repair, group, replica, options)
	return fresh, nil
}

func allocateRepairID(group *topology.Group, latest topology.Document, groupID string) (uint64, error) {
	if newer := latest.Group(groupID); newer != nil {
		group.NextReplicaID = max(group.NextReplicaID, newer.NextReplicaID)
	}
	return group.AllocateReplicaID()
}

func stampRepairVoter(repair *topology.Document, group *topology.Group, replica *topology.Replica,
	options ReplicaRepairOptions) {
	replica.UpdatedAt = options.At
	group.UpdatedAt = options.At
	if node := repair.Node(options.NodeName); node != nil {
		node.UpdatedAt = options.At
	}
}

func finishRepair(repair *topology.Document, activeVersion, latestVersion uint64, options ReplicaRepairOptions) {
	repair.Version = latestVersion + 1
	repair.ParentVersion = activeVersion
	repair.Origin = options.Origin
	repair.ChangedBy = options.Actor
}
