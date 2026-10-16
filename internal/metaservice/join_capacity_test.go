//go:build !functional

package metaservice

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestJoinGrantAllowsBoundedDecommissionMembershipHints(t *testing.T) {
	t.Parallel()
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{}.WithDefaults()}
	previous := topology.Document{Version: 1, ClusterID: "cluster", Origin: topology.OriginFounding,
		CreatedAt: time.Now().UTC(), Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 4}}}
	var members []joinMember
	for i, name := range []string{"one", "two", "three"} {
		previous.Nodes = append(previous.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		previous.MetaGroup().Replicas = append(previous.MetaGroup().Replicas, topology.Replica{
			NodeName: name, ReplicaID: uint64(i + 1), Role: topology.ReplicaVoter})
		members = append(members, joinMember{Name: name, MemberID: uint64(i + 1)})
	}
	granted := previous.Clone()
	granted.Version, granted.ParentVersion, granted.Origin = 2, 1, topology.OriginAPI
	granted.Node("three").State = topology.NodeExcluded
	granted.Nodes = append(granted.Nodes, topology.Node{Name: "four", Type: "worker", State: topology.NodeActive})
	granted.MetaGroup().NextReplicaID = 5
	granted.MetaGroup().Replicas[2] = topology.Replica{NodeName: "four", ReplicaID: 4, Role: topology.ReplicaLearner}
	members = append(members, joinMember{Name: "four", MemberID: 4})
	identity := topology.Identity{ClusterID: "cluster", NodeName: "four", InstanceID: "process"}
	result := joinResponse{Rules: rules, MemberID: 4, Granted: granted, Previous: previous, Members: members,
		LeaderAddress: "127.0.0.1:1234", Admission: metaengine.AdmissionRecord{
			NodeName: "four", InstanceID: "process", MemberID: 4, OperatorID: topology.MakeOperatorID(2, 1)}}
	if err := validateJoinGrant(identity, rules, result); err != nil {
		t.Fatal("temporary native membership hints were confused with document capacity", err)
	}
	result.Members[2].Name = "foreign"
	if err := validateJoinGrant(identity, rules, result); err == nil {
		t.Fatal("surge bypassed exact old member identity")
	}
}
