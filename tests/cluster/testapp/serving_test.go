//go:build !functional

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type servingProgressCluster struct {
	gquorum.Cluster
	status topology.ClusterStatus
	target topology.Document
}

func (c servingProgressCluster) Status(context.Context) (topology.ClusterStatus, error) {
	panic("serving attempted live cluster probes")
}
func (servingProgressCluster) Get(context.Context, uint64) (topology.Document, error) {
	panic("serving required a compactable source document")
}
func (c servingProgressCluster) Target(context.Context) (topology.Document, error) {
	return c.target.Clone(), nil
}
func (servingProgressCluster) ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error) {
	return topology.ProgressPage{}, fault.ErrUnavailable
}

func TestServingUsesCompactedConvergenceAndExactApprovedMember(t *testing.T) {
	t.Parallel()
	identity := topology.Identity{NodeName: "one", MemberID: 7}
	cluster := servingProgressCluster{
		status: topology.ClusterStatus{TargetVersion: 9, FullyConverged: topology.FullyConverged{Version: 2},
			Groups: []topology.GroupProgress{{GroupID: "work", ConvergedVersion: 8, Converged: &topology.Group{ID: "work",
				Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 1}}}}}},
		target: topology.Document{Version: 9, Groups: []topology.Group{{ID: topology.MetaGroupID,
			Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 7}}}}},
	}
	if group, err := servingGroup(t.Context(), cluster, cluster.status.Groups[0], identity); err != nil || group == nil {
		t.Fatal("historical marker or metadata quorum blocked independent group", group, err)
	}
	cluster.target.Groups[0].Replicas[0].ReplicaID++
	if _, err := servingGroup(t.Context(), cluster, cluster.status.Groups[0], identity); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("successor member authorized stale process", err)
	}
	cluster.target.Groups[0].Replicas[0].ReplicaID = identity.MemberID
	if _, err := servingGroup(t.Context(), cluster, cluster.status.Groups[0], topology.Identity{NodeName: "joining", MemberID: 8}); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("pending admission was classified as a foreign converged owner", err)
	}
	row := &cluster.status.Groups[0]
	row.Approved, row.Converged = row.Converged, nil
	if _, err := servingGroup(t.Context(), cluster, cluster.status.Groups[0], identity); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("approval was treated as convergence", err)
	}
}
