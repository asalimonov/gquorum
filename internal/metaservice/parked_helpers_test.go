package metaservice

import (
	"github.com/asalimonov/gquorum/topology"
)

func parkedRequest(name, instance string) joinRequest {
	return joinRequest{Identity: topology.Identity{NodeName: name, InstanceID: instance, ClusterID: "cluster", MemberID: 7}, Node: topology.Node{Name: name, Type: "worker"}, Address: "127.0.0.1:1234", LastDocVersion: 3}
}
