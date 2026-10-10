//go:build cluster

package cluster

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

func TestStagedTopologyDoesNotChangeApprovedMembershipOrStartGroups(t *testing.T) {
	spec := matrixSpec(3)
	spec.Limits.MaxNodes = 4 // Room for an admitted node whose process never starts.
	lease := acquire(t, spec)
	c := lease.Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	base := created.Document
	baseline, err := groupStatus(ctx, c, "node2", "work")
	if err != nil || !baseline.Found || len(baseline.ConfState.Voters) != 3 {
		t.Fatal("baseline membership", baseline, err)
	}
	assertInert := func(latestVersion uint64) {
		t.Helper()
		if err := Poll(ctx, func() (bool, error) {
			for _, node := range c.Nodes() {
				latest, err := c.Document(ctx, node.Spec.Name, "latest")
				if err != nil || latest.Version != latestVersion {
					return false, err
				}
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, node := range c.Nodes() {
			status, err := c.Status(ctx, node.Spec.Name)
			if err != nil || status.LatestVersion != latestVersion || status.TargetVersion != base.Version || status.ActiveVersion != base.Version || status.Execution == nil || status.Execution.ID != created.ExecutionID || status.Execution.State != topology.ExecutionSucceeded {
				t.Fatal("staging changed an approved view or started an execution", node.Spec.Name, status, err)
			}
			for _, view := range []string{"target", "active"} {
				doc, err := c.Document(ctx, node.Spec.Name, view)
				if err != nil || doc.Version != base.Version || len(doc.Nodes) != 3 || len(doc.LoadGroups()) != 1 {
					t.Fatal("approved document changed", node.Spec.Name, view, doc, err)
				}
			}
			observed, err := groupStatus(ctx, c, node.Spec.Name, "work")
			if err != nil || !slices.Equal(observed.ConfState.Voters, baseline.ConfState.Voters) || !slices.Equal(observed.ConfState.Learners, baseline.ConfState.Learners) {
				t.Fatal("staging changed actual membership", node.Spec.Name, observed, err)
			}
		}
	}
	role := base.Clone()
	role.Group("work").Replicas[2].Role = topology.ReplicaLearner
	staged, err := c.Stage(ctx, "node2", role)
	if err != nil {
		t.Fatal(err)
	}
	assertInert(staged.Document.Version)
	response, err := c.Request(ctx, "node3", "PUT", "/api/groups/work/placement", map[string]any{
		"placement": topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2},
		"mutation":  meta.MutationOptions{Actor: "cluster-test"},
	}, staged.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := response.Decode(&staged); err != nil {
		t.Fatal(err)
	}
	assertInert(staged.Document.Version)
	response, err = c.Request(ctx, "node1", "POST", "/api/nodes", map[string]any{
		"node":     topology.Node{Name: "staged-no-process", Type: "worker"},
		"mutation": meta.MutationOptions{Actor: "cluster-test"},
	}, staged.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := response.Decode(&staged); err != nil {
		t.Fatal(err)
	}
	assertInert(staged.Document.Version)
	response, err = c.Request(ctx, "node3", "POST", "/api/groups", map[string]any{
		"group":    meta.GroupSpec{ID: "staged-only"},
		"mutation": meta.MutationOptions{Actor: "cluster-test"},
	}, staged.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := response.Decode(&staged); err != nil {
		t.Fatal(err)
	}
	if staged.Document.Version != base.Version+4 || staged.Document.Node("staged-no-process") == nil || staged.Document.Group("staged-only") == nil {
		t.Fatal("staged edits missing", staged)
	}
	assertInert(staged.Document.Version)
	for _, node := range c.Nodes() {
		response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/load/staged-only/keys/key", nil, 0)
		if err := requireStatus(response, err, 503); err != nil {
			t.Fatal("staged-only group has a local runtime", err)
		}
		if _, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), client.ContainerStatPathOptions{Path: "/data/load/staged-only"}); !errdefs.IsNotFound(err) {
			t.Fatal("staged-only group created local storage", node.Spec.Name, err)
		}
	}
	if err := c.Put(ctx, "node1", "work", "staged-traffic", "staged-traffic", "acknowledged"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.WaitValue(ctx, node.Spec.Name, "work", "staged-traffic", "acknowledged"); err != nil {
			t.Fatal("approved group stopped during staging", err)
		}
	}
}
