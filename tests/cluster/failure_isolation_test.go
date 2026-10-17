//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func stageIsolationGroup(ctx context.Context, t *testing.T, c *Cluster, group string, expected uint64) gquorum.MutationResult {
	t.Helper()
	response, err := c.Request(ctx, "node1", "POST", "/api/groups", map[string]any{
		"group":    gquorum.GroupSpec{ID: group, Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}},
		"mutation": gquorum.MutationOptions{Actor: "isolation-test"},
	}, expected)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var result gquorum.MutationResult
	if err := response.Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func reseatIsolationReplica(ctx context.Context, t *testing.T, c *Cluster, group string, replica, expected uint64) gquorum.MutationResult {
	t.Helper()
	response, err := c.Request(ctx, "node1", "POST", "/api/groups/"+group+"/replicas/node3/reseat",
		map[string]any{"replica_id": replica, "actor": "isolation-test", "comment": "explicit exact repair"}, expected)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var result gquorum.MutationResult
	if err := response.Decode(&result); err != nil || result.OperationID == "" {
		t.Fatal("repair omitted its accepted operation", result, err)
	}
	return result
}

func TestFailureIsolation_BlockedGroup(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	base, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range c.Nodes() {
		if err := c.SetBarrier(ctx, n.Spec.Name, "blocked", topology.StepCreateFoundingGroup, true); err != nil {
			t.Fatal(err)
		}
	}
	staged := stageIsolationGroup(ctx, t, c, "blocked", base.Version)
	blocked := startApply(ctx, t, c, "node1", staged.Document.Version)
	if err := Poll(ctx, func() (bool, error) {
		for _, n := range c.Nodes() {
			controls, err := c.Controls(ctx, n.Spec.Name)
			if err != nil {
				return false, err
			}
			for _, barrier := range controls.Barriers {
				if barrier.Group == "blocked" && barrier.Hits > 0 {
					if barrier.Last.OperationID != blocked || barrier.Last.Version != staged.Document.Version || barrier.Last.Attempt != 1 {
						return true, fmt.Errorf("barrier reached with foreign authority: %+v", barrier)
					}
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("blocked group never reached the authenticated barrier", err)
	}
	healthy := stageIsolationGroup(ctx, t, c, "healthy", staged.Document.Version)
	healthy.OperationID = startApply(ctx, t, c, "node1", healthy.Document.Version)
	if err := c.WaitOperation(ctx, "node1", healthy.OperationID); err != nil {
		t.Fatal("unrelated group could not converge", err)
	}
	// Exercise real native membership, cleanup and same-host replacement while
	// the original founding operation remains held. No global wait helper here.
	repaired := reseatIsolationReplica(ctx, t, c, "healthy", replicaID(healthy.Document, "healthy", "node3"), healthy.Document.Version)
	if err := c.WaitOperation(ctx, "node1", repaired.OperationID); err != nil {
		t.Fatal("independent repair was gated by founding", err)
	}
	page, err := c.Progress(ctx, "node1", topology.ProgressQuery{})
	if err != nil || page.TargetVersion != repaired.Document.Version || page.FullyConverged.Version != base.Version {
		t.Fatal("incorrect aggregate convergence while founding is held", page, err)
	}
	for _, want := range []struct {
		group   string
		version uint64
		owner   topology.OperationID
	}{{"blocked", 0, blocked}, {"healthy", repaired.Document.Version, ""}} {
		found := false
		for _, row := range page.Groups {
			if row.GroupID == want.group {
				found = true
				if row.ConvergedVersion != want.version || row.OperationID != want.owner {
					t.Fatal("unrelated approval changed scoped provenance", row, want)
				}
			}
		}
		if !found {
			t.Fatal("missing group progress", want.group, page)
		}
	}
	status, err := c.Operation(ctx, "node1", blocked)
	if err != nil || status.Version != staged.Document.Version || status.State.Terminal() {
		t.Fatal("unrelated work replaced or completed held operation", status, err)
	}
	if err := c.Put(ctx, "node1", "healthy", "independent", "independent", "served"); err != nil {
		t.Fatal("converged group cannot serve behind historical marker", err)
	}
	if err := exactOwnedValues(ctx, c, repaired.Document, "healthy", map[string]string{"independent": "served"}); err != nil {
		t.Fatal(err)
	}
	for _, n := range c.Nodes() {
		if err := c.SetBarrier(ctx, n.Spec.Name, "blocked", topology.StepCreateFoundingGroup, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitOperation(ctx, "node1", blocked); err != nil {
		t.Fatal("original operation did not resume", err)
	}
	if err := c.WaitFullyConverged(ctx, repaired.Document.Version); err != nil {
		t.Fatal(err)
	}
}

func TestFailureIsolation_StagedRebase(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	placement := &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}
	if _, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "intent", Placement: placement}); err != nil {
		t.Fatal(err)
	}
	base, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "repair", Placement: placement})
	if err != nil {
		t.Fatal(err)
	}
	candidate := base.Document.Clone()
	candidate.Group("intent").Labels = map[string]string{"pending": "must-not-be-approved"}
	candidate.ChangedBy, candidate.Comment, candidate.Origin = "staged-editor", "retain unrelated intent", topology.OriginOperator
	staged, err := c.Stage(ctx, "node1", candidate)
	if err != nil {
		t.Fatal(err)
	}
	repaired := reseatIsolationReplica(ctx, t, c, "repair", replicaID(base.Document, "repair", "node3"), staged.Document.Version)
	// The mutation may have been forwarded. Local document observations need
	// their explicit barrier before asserting the atomically committed pair.
	page, err := c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: "repair"})
	if err != nil || page.TargetVersion != repaired.Document.Version || page.LatestVersion != repaired.Document.Version+1 {
		t.Fatal("repair/rebase pair not visible after quorum read", page, err)
	}
	latest, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	r, s := repaired.Document, latest
	if r.Version != staged.Document.Version+1 || r.ParentVersion != base.Document.Version || r.RebasedFrom != 0 ||
		s.Version != r.Version+1 || s.ParentVersion != r.Version || s.RebasedFrom != staged.Document.Version ||
		!reflect.DeepEqual(r.Group("intent"), base.Document.Group("intent")) ||
		!reflect.DeepEqual(s.Group("intent"), staged.Document.Group("intent")) || !reflect.DeepEqual(s.Group("repair"), r.Group("repair")) ||
		s.ChangedBy != staged.Document.ChangedBy || s.Comment != staged.Document.Comment || s.Origin != staged.Document.Origin {
		t.Fatal("repair lost staged intent or atomic provenance", base.Document, staged.Document, r, s)
	}
	if err := c.WaitOperation(ctx, "node1", repaired.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitFullyConverged(ctx, r.Version); err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node1", "node2", r.MemberID("node2")); err != nil {
		t.Fatal(err)
	}
	// Stop the known leader, force election on retained replicas, then reopen
	// its volume. No restaging or guessed successor operation is allowed.
	if err := c.Node("node2").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		page, err := c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: "repair"})
		return err == nil && page.TargetVersion == r.Version && page.LatestVersion == s.Version, err
	}); err != nil {
		t.Fatal("rebase did not survive leader loss", err)
	}
	if err := c.Node("node2").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, n := range c.Nodes() {
		latest, err := c.Document(ctx, n.Spec.Name, "latest")
		if err != nil || !reflect.DeepEqual(latest, s) {
			t.Fatal("retained restart changed rebased intent", n.Spec.Name, latest, err)
		}
		target, err := c.Document(ctx, n.Spec.Name, "target")
		if err != nil || !reflect.DeepEqual(target, r) {
			t.Fatal("staged successor was implicitly approved", n.Spec.Name, target, err)
		}
		op, err := c.Operation(ctx, n.Spec.Name, repaired.OperationID)
		if err != nil || op.Version != r.Version || op.State != topology.OperationSucceeded {
			t.Fatal("retained repair operation lost", n.Spec.Name, op, err)
		}
	}
	response, err := c.Request(ctx, "node1", "POST", fmt.Sprintf("/api/topology/%d/approve", staged.Document.Version), nil, 0)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal(err)
	}
	var conflict struct {
		Code      fault.Code `json:"code"`
		RebasedTo uint64     `json:"rebased_to"`
	}
	if err := response.Decode(&conflict); err != nil || conflict.Code != fault.VersionConflict || conflict.RebasedTo != s.Version {
		t.Fatal("old approval did not identify preserved successor", conflict, err)
	}
	page, err = c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: "intent"})
	if err != nil || page.TargetVersion != r.Version || page.LatestVersion != s.Version || len(page.Groups) != 1 ||
		!reflect.DeepEqual(page.Groups[0].Desired, s.Group("intent")) || !reflect.DeepEqual(page.Groups[0].Approved, r.Group("intent")) || page.Groups[0].OperationID != "" {
		t.Fatal("old approval changed the approved intent", page, err)
	}
}
