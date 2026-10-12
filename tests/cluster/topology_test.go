//go:build clustertest

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/asalimonov/gquorum"
	"maps"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

func TestStagedTopologyDoesNotChangeApprovedMembershipOrStartGroups(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.Limits.MaxNodes = 4 // Room for an admitted node whose process never starts.
	lease := acquire(t, spec)
	c := lease.Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}})
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
			if err != nil || status.LatestVersion != latestVersion || status.TargetVersion != base.Version || status.ActiveVersion != base.Version || status.Operator == nil || status.Operator.ID != created.OperatorID || status.Operator.State != topology.OperatorSucceeded {
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
	role.Group("work").VoterPolicy.TargetVoters = 2
	staged, err := c.Stage(ctx, "node2", role)
	if err != nil {
		t.Fatal(err)
	}
	assertInert(staged.Document.Version)
	response, err := c.Request(ctx, "node3", "PUT", "/api/groups/work/placement", map[string]any{
		"placement": topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2},
		"mutation":  gquorum.MutationOptions{Actor: "cluster-test"},
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
		"mutation": gquorum.MutationOptions{Actor: "cluster-test"},
	}, staged.Document.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := response.Decode(&staged); err != nil {
		t.Fatal(err)
	}
	assertInert(staged.Document.Version)
	response, err = c.Request(ctx, "node3", "POST", "/api/groups", map[string]any{
		"group":    gquorum.GroupSpec{ID: "staged-only"},
		"mutation": gquorum.MutationOptions{Actor: "cluster-test"},
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

func TestStagedReplicaRepairRequiresExplicitApplyThreeNodes(t *testing.T) {
	t.Parallel()
	stagedReplicaRepair(t, 3)
}

func stagedReplicaRepair(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.Scheduling = true
	spec.SchedulingInterval, spec.SchedulingTimeout = 200*time.Millisecond, 150*time.Millisecond
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"before-stage": "retained"}
	if err := c.Put(ctx, "node3", "work", "before-stage", "before-stage", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	oldID := replicaID(created.Document, "work", "node3")
	candidate := created.Document.Clone()
	candidate.Group("work").Replicas = slices.DeleteFunc(candidate.Group("work").Replicas, func(r topology.Replica) bool { return r.NodeName == "node3" })
	staged, err := c.Stage(ctx, "node2", candidate)
	if err != nil {
		t.Fatal(err)
	}
	waitLatest := func(version uint64) {
		t.Helper()
		if err := Poll(ctx, func() (bool, error) {
			for _, node := range c.Nodes() {
				doc, err := c.Document(ctx, node.Spec.Name, "latest")
				if err != nil || doc.Version < version {
					return false, err
				}
				if doc.Version != version {
					return true, fmt.Errorf("unexpected Latest version %d, want %d", doc.Version, version)
				}
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitLatest(staged.Document.Version)
	// Cross fifteen configured policy ticks. The active controller must report
	// staged_latest; merely observing an unchanged document once is insufficient.
	until := time.Now().Add(3 * time.Second)
	blocked, samples := false, 0
	for time.Now().Before(until) {
		for _, node := range c.Nodes() {
			status, err := c.Status(ctx, node.Spec.Name)
			if err != nil || status.LatestVersion != staged.Document.Version || status.TargetVersion != created.Document.Version || status.ActiveVersion != created.Document.Version {
				t.Fatal("reconciler overwrote unapproved operator intent", node.Spec.Name, status, err)
			}
			r := status.Scheduling
			blocked = blocked || r.Enabled && r.IsLeader && r.State == topology.SchedulingBlocked && r.Reason == "staged_latest"
		}
		local, err := c.Local(ctx, "node3", "work")
		if err != nil || local.ReplicaID != oldID || !slices.Contains(local.Status.ConfState.Voters, oldID) || !maps.Equal(local.Values, expected) {
			t.Fatal("staged disassociation changed real membership", local, err)
		}
		samples++
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
	if !blocked || samples < 10 {
		t.Fatal("automatic controller never observed staged protection", blocked, samples)
	}
	removeID, err := c.Approve(ctx, "node2", staged.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := c.Local(ctx, "node3", "work")
	if err != nil || removed.Found || removed.StorageIdentity != nil {
		t.Fatal("approved removal retained local voting handle", removed, err)
	}
	repaired := staged.Document.Clone()
	freshID, err := repaired.Group("work").AllocateReplicaID()
	if err != nil {
		t.Fatal(err)
	}
	oldReplica := created.Document.Group("work").Replicas[2]
	for _, r := range created.Document.Group("work").Replicas {
		if r.NodeName == "node3" {
			oldReplica = r
		}
	}
	oldReplica.ReplicaID = freshID
	repaired.Group("work").Replicas = append(repaired.Group("work").Replicas, oldReplica)
	staged, err = c.Stage(ctx, "node2", repaired)
	if err != nil {
		t.Fatal(err)
	}
	waitLatest(staged.Document.Version)
	response, err := c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/plan", staged.Document.Version), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil {
		t.Fatal(err)
	}
	want := []topology.StepKind{topology.StepPrepareReplica, topology.StepAddLearner, topology.StepWaitCaughtUp, topology.StepPromoteVoter}
	var actual []topology.StepKind
	for _, step := range plan.Steps {
		if step.GroupID == "work" && step.NodeName == "node3" {
			actual = append(actual, step.Kind)
		}
	}
	if !slices.Equal(actual, want) || freshID <= oldID {
		t.Fatal("repair did not plan fresh learner before voter", plan, oldID, freshID)
	}
	repairID, err := c.Approve(ctx, "node2", staged.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	if repairID == removeID {
		t.Fatal("repair reused removal execution", repairID)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, staged.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	recovered, err := c.Local(ctx, "node3", "work")
	if err != nil || recovered.ReplicaID != freshID || !slices.Contains(recovered.Status.ConfState.Voters, freshID) || slices.Contains(recovered.Status.ConfState.Voters, oldID) {
		t.Fatal("operator repair did not install exact new voter", recovered, err)
	}
	t.Logf("METRIC nodes=%d staged_samples=%d retired_replica=%d repaired_replica=%d", size, samples, oldID, freshID)
}

func TestAddressFreeTopologyRoutesMetaAndLoadThreeNodes(t *testing.T) {
	t.Parallel()
	addressFreeRouting(t, 3)
}

func addressFreeRouting(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.AddressFree = true
	ctx, c := performanceClusterSpec(t, spec)
	assertAddressFree := func(view string) {
		t.Helper()
		response, err := c.Request(ctx, "node2", "GET", "/api/topology/"+view, nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(strings.ToLower(string(response.Data)), "addr") {
			t.Fatal("caller resolver location leaked into replicated document", view, string(response.Data))
		}
		var doc topology.Document
		if err := response.Decode(&doc); err != nil {
			t.Fatal(err)
		}
		if len(doc.Nodes) != size || len(doc.MetaGroup().Replicas) != size {
			t.Fatal("address-free topology dropped identities", doc)
		}
		for _, node := range doc.Nodes {
			raw, err := json.Marshal(node)
			if err != nil || len(node.Labels) != 0 || strings.Contains(strings.ToLower(string(raw)), "addr") {
				t.Fatal("node persisted discovery location", node, err)
			}
		}
	}
	for _, view := range []string{"latest", "target", "active"} {
		assertAddressFree(view)
	}
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal("meta proposal could not use external caller resolver", err)
	}
	expected := map[string]string{}
	for _, node := range c.Nodes() {
		status, err := c.Status(ctx, node.Spec.Name)
		if err != nil || len(status.Nodes) != size {
			t.Fatal("discovery status lost nodes", status, err)
		}
		for _, observed := range status.Nodes {
			host, port, err := net.SplitHostPort(observed.Address)
			if err != nil || host != observed.Node.Name || port != "9000" || created.Document.MemberID(observed.Node.Name) == 0 {
				t.Fatal("status did not enrich exact identity using live resolver", observed, err)
			}
		}
		key := "from-" + node.Spec.Name
		if err := c.Put(ctx, node.Spec.Name, "work", key, key, key); err != nil {
			t.Fatal("load routing requires replicated address", err)
		}
		expected[key] = key
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, view := range []string{"latest", "target", "active"} {
		assertAddressFree(view)
	}
}
