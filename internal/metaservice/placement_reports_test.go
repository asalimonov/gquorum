//go:build !functional

package metaservice

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestUnsafePlacementPlanCarriesCandidateAndSequenceReason(t *testing.T) {
	t.Parallel()
	base := topology.Document{Version: 1, ClusterID: "cluster", Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2},
		Nodes: []topology.Node{{Name: "n1", Type: "worker", State: topology.NodeActive}, {Name: "n2", Type: "worker", State: topology.NodeActive}},
		Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 3, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaVoter}}},
			{ID: "work", NextReplicaID: 3, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaVoter}}}}}
	desired := base.Clone()
	desired.Version = 2
	desired.Group("work").Replicas[1].Role = topology.ReplicaLearner
	cause := fault.New(fault.UnsafeMembership, "group work has no safe transition", nil)
	got := explainUnsafePlacementPlan(base, base, desired, cause)
	var placement *topology.PlacementError
	if !errors.As(got, &placement) || !errors.Is(got, fault.ErrUnsafeMembership) || placement.Report.Version != 2 || len(placement.Report.Groups) != 1 ||
		len(placement.Report.Groups[0].Candidates) != 2 || placement.Report.Groups[0].Issues[0].Code != "no_safe_transition" {
		t.Fatal("unsafe plan lost structured placement explanation", got)
	}
}
