//go:build clustertest

package cluster

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

func TestStoppedFollowerHealthUsesActivePlacementAndRetainsStagingThreeNodes(t *testing.T) {
	t.Parallel()
	stoppedFollowerPlacement(t, 3)
}

// Source stopped-follower health/readiness, including its staged-placement blocker
// and normal survivor restart under Latest/Active drift. Meta health covers every
// admitted type; the data plane remains a caller-defined three-worker subset.
func stoppedFollowerPlacement(t *testing.T, size int) {
	spec := matrixSpec(size)
	spec.Scheduling = true
	spec.SchedulingInterval, spec.SchedulingTimeout = 200*time.Millisecond, 150*time.Millisecond
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	base := created.Document
	if err := transferMeta(ctx, c, "node2", "node1", base.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	if err := requestLeaderTransfer(ctx, c, "node2", "work", "node1"); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := groupStatus(ctx, c, "node1", "work")
		return status.LeaderID == replicaID(base, "work", "node1"), err
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node2", "work", "before-stop", "before-stop", "retained"); err != nil {
		t.Fatal(err)
	}
	rule := base.Placement
	rule.MinReplicas, rule.MaxReplicas = 3, 3
	response, err := c.Request(ctx, "node2", "PUT", "/api/placement", globalPlacementBody(rule, gquorum.MutationOptions{Actor: "operator"}), base.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var staged gquorum.MutationResult
	if err := response.Decode(&staged); err != nil || staged.Document.Version != base.Version+1 || staged.Document.Placement.MinReplicas != 3 || staged.OperationID != "" {
		t.Fatal(staged, err)
	}
	assertBlocked := func() {
		t.Helper()
		for _, node := range c.Nodes() {
			if !node.running.Load() {
				continue
			}
			if err := Poll(ctx, func() (bool, error) {
				status, err := c.Status(ctx, node.Spec.Name)
				return status.LatestVersion == staged.Document.Version && status.TargetVersion == base.Version && status.FullyConverged.Version == base.Version && status.Scheduling.State == topology.SchedulingBlocked && status.Scheduling.Reason == "staged_latest", err
			}); err != nil {
				t.Fatal("durable staged placement did not block repair", node.Spec.Name, err)
			}
			latest, err := c.Document(ctx, node.Spec.Name, "latest")
			if err != nil || !reflect.DeepEqual(latest, staged.Document) {
				t.Fatal("staged placement changed", node.Spec.Name, latest, err)
			}
			active, err := c.Document(ctx, node.Spec.Name, "active")
			if err != nil || !reflect.DeepEqual(active, base) {
				t.Fatal("unapproved placement changed Active", active, err)
			}
		}
	}
	assertBlocked()
	response, err = c.Request(ctx, "node2", "POST", fmt.Sprintf("/api/topology/%d/plan", staged.Document.Version), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil || !plan.IsEmpty() {
		t.Fatal("placement-only staging has a runtime effect", plan, err)
	}
	if err := c.Node("node3").Stop(ctx); err != nil {
		t.Fatal(err)
	}
	inspectHealth := func(alive bool) {
		t.Helper()
		for _, node := range c.Nodes() {
			if !node.running.Load() {
				continue
			}
			if err := Poll(ctx, func() (bool, error) {
				status, err := c.Status(ctx, node.Spec.Name)
				if err != nil {
					return false, err
				}
				if status.Observation.Version != base.Version || status.Observation.At.IsZero() || len(status.Observation.Groups) != 2 {
					return false, nil
				}
				expectedNodeHealth := topology.HealthDead
				if alive {
					expectedNodeHealth = topology.HealthAlive
				}
				if status.Observation.Nodes["node3"] != expectedNodeHealth {
					return false, nil
				}
				for _, group := range status.Observation.Groups {
					voters := 3
					if group.GroupID == topology.MetaGroupID {
						voters = size
					}
					healthy := voters
					if !alive {
						healthy--
					}
					state := topology.GroupHealthy
					if !alive {
						state = topology.GroupDegraded
					}
					h := group.ObservedHealth
					if group.Voters != voters || group.Quorum != topology.QuorumOf(voters) || h.State != state || h.Source != "observed" || h.HealthyVoters == nil || *h.HealthyVoters != healthy || h.FailureTolerance == nil || *h.FailureTolerance != healthy-topology.QuorumOf(voters) {
						return false, nil
					}
					if group.GroupID == topology.MetaGroupID && h.BelowMinReplicas != nil {
						return true, fmt.Errorf("meta acquired load placement rules")
					}
					if group.GroupID == "work" && (h.BelowMinReplicas == nil || *h.BelowMinReplicas) {
						return true, fmt.Errorf("health used staged minimum instead of Active minimum %d", base.Placement.MinReplicas)
					}
				}
				return true, nil
			}); err != nil {
				t.Fatal("health did not report both planes from the exact Active view", node.Spec.Name, err)
			}
			response, err := c.Request(ctx, node.Spec.Name, "GET", "/health/ready", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				t.Fatal("retained majority stopped serving", node.Spec.Name, err)
			}
		}
	}
	inspectHealth(false)
	assertBlocked()
	if err := c.Put(ctx, "node2", "work", "during-stop", "during-stop", "acknowledged"); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	inspectHealth(true)
	assertBlocked()
	expected := map[string]string{"before-stop": "retained", "during-stop": "acknowledged"}
	if err := exactOwnedValues(ctx, c, base, "work", expected); err != nil {
		t.Fatal("returning follower lost acknowledged commands", err)
	}
	// Restart the original survivor/leader with a retained store and unapplied
	// staged lifecycle. Neither founding nor reconciliation may overwrite its views.
	if err := c.Node("node1").Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node1").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal("retained survivor failed under staged drift", err)
	}
	inspectHealth(true)
	assertBlocked()
	if err := exactOwnedValues(ctx, c, base, "work", expected); err != nil {
		t.Fatal(err)
	}
	assertActualAPIMembership(ctx, t, c, base, 3)
}
