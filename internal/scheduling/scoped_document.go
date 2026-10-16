package scheduling

import (
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

func scopedRepairDocument(read metaengine.PlanRead, action schedulingapi.ScheduleAction,
	at time.Time) (topology.Document, error) {
	if read.Latest.Version == math.MaxUint64 {
		return topology.Document{}, fault.ErrInvalid
	}
	var doc topology.Document
	if action.Kind == schedulingapi.ReconfigureVoters {
		var err error
		doc, err = metaengine.VoterChangeDocument(read.Target, *action.VoterChange, at, "auto-voter-config")
		if err != nil {
			return topology.Document{}, err
		}
	} else {
		id := action.Identity
		result, err := metaengine.ReplicaRepairDocument(read.Target, read.Latest, metaengine.ReplicaRepairOptions{
			GroupID: id.GroupID, NodeName: id.NodeName, ReplicaID: id.ReplicaID, At: at,
			Origin: topology.OriginReconciler, Actor: "auto-repair"})
		if err != nil {
			return topology.Document{}, err
		}
		doc = result.Doc
		// The node itself did not change. Stamping its timestamp would spuriously
		// own _meta, coupling otherwise independent repairs to node admission.
		doc.Nodes = read.Target.Clone().Nodes
	}
	doc.Version, doc.ParentVersion, doc.CreatedAt, doc.RebasedFrom = read.Latest.Version+1, read.Target.Version, at, 0
	return doc, nil
}
