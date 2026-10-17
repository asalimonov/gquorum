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
	page topology.ProgressPage
}

func (c servingProgressCluster) ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error) {
	return c.page, nil
}

func TestServingUsesIndependentConvergenceAndExactMember(t *testing.T) {
	t.Parallel()
	identity := topology.Identity{NodeName: "one", MemberID: 7}
	page := topology.ProgressPage{TargetVersion: 9, FullyConverged: topology.FullyConverged{Version: 2},
		Groups: []topology.GroupProgress{{GroupID: "work", ConvergedVersion: 8, Converged: &topology.Group{ID: "work"}}},
		Nodes:  []topology.NodeProgress{{NodeName: "one", MemberID: 7}}}
	if group, err := servingGroup(t.Context(), servingProgressCluster{page: page}, "work", identity); err != nil || group == nil {
		t.Fatal("historical marker blocked independent group", group, err)
	}
	page.Nodes[0].MemberID++
	if _, err := servingGroup(t.Context(), servingProgressCluster{page: page}, "work", identity); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("successor member authorized stale process", err)
	}
	page.Nodes[0].MemberID = identity.MemberID
	page.Groups[0].Approved, page.Groups[0].Converged = page.Groups[0].Converged, nil
	if _, err := servingGroup(t.Context(), servingProgressCluster{page: page}, "work", identity); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("approval was treated as convergence", err)
	}
}
