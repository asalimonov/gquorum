//go:build !functional

package metaservice

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestProgressObservationRequiresExactProvenance(t *testing.T) {
	t.Parallel()
	process := topology.Identity{ClusterID: "c", NodeName: "n", MemberID: 7, InstanceID: "current"}
	group := topology.Group{ID: "work", Replicas: []topology.Replica{{NodeName: "n", ReplicaID: 9}}}
	row := topology.GroupProgress{GroupID: "work", ConvergedVersion: 4, Converged: &group, Approved: &group}
	observed := topology.GroupStatus{GroupID: "work", Found: true, Reporter: process, ReporterReplicaID: 9,
		SourceVersion: 4, ReceivedAt: time.Unix(100, 0), HostNode: "n"}
	if !matchingProgressObservation(row, observed, process) {
		t.Fatal("exact cached observation refused")
	}
	for name, change := range map[string]func(*topology.GroupStatus){
		"group":           func(s *topology.GroupStatus) { s.GroupID = "other" },
		"source":          func(s *topology.GroupStatus) { s.SourceVersion++ },
		"unreceived":      func(s *topology.GroupStatus) { s.ReceivedAt = time.Time{} },
		"absent":          func(s *topology.GroupStatus) { s.Found = false },
		"cluster":         func(s *topology.GroupStatus) { s.Reporter.ClusterID = "other" },
		"node":            func(s *topology.GroupStatus) { s.Reporter.NodeName = "other" },
		"member":          func(s *topology.GroupStatus) { s.Reporter.MemberID-- },
		"instance":        func(s *topology.GroupStatus) { s.Reporter.InstanceID = "predecessor" },
		"replica":         func(s *topology.GroupStatus) { s.ReporterReplicaID-- },
		"missing replica": func(s *topology.GroupStatus) { s.ReporterReplicaID = 0 },
		"host":            func(s *topology.GroupStatus) { s.HostNode = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			expected, bad := row.Clone(), observed
			t.Parallel()
			change(&bad)
			if matchingProgressObservation(expected, bad, process) {
				t.Fatal("mismatched sample accepted", bad)
			}
		})
	}
	row.Desired, row.Converged, row.Approved = &group, nil, nil
	if matchingProgressObservation(row, observed, process) {
		t.Fatal("staged assignment authorized observation")
	}
	row.Approved, row.ConvergedVersion, observed.SourceVersion = &group, 0, 0
	if matchingProgressObservation(row, observed, process) {
		t.Fatal("missing source provenance was assumed current before first convergence")
	}
	row.GroupID, group.ID, observed.GroupID = topology.MetaGroupID, topology.MetaGroupID, topology.MetaGroupID
	if matchingProgressObservation(row, observed, process) {
		t.Fatal("metadata replica not bound to reporter member")
	}
}

func TestProgressLeaderNameUsesExactApprovedOrProvenReplica(t *testing.T) {
	t.Parallel()
	row := topology.GroupProgress{
		Converged: &topology.Group{Replicas: []topology.Replica{{NodeName: "old", ReplicaID: 1}}},
		Approved:  &topology.Group{Replicas: []topology.Replica{{NodeName: "new", ReplicaID: 2}}},
		Desired:   &topology.Group{Replicas: []topology.Replica{{NodeName: "staged", ReplicaID: 3}}}}
	for id, want := range map[uint64]string{0: "", 1: "old", 2: "new", 3: "", 99: ""} {
		if got := progressLeaderName(row, id); got != want {
			t.Fatal(id, got, want)
		}
	}
}
