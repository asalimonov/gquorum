//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

func placementEdit(ctx context.Context, t *testing.T, c *Cluster, edit func(*topology.Document)) topology.Document {
	t.Helper()
	latest, err := c.Document(ctx, "node2", "latest")
	if err != nil {
		t.Fatal(err)
	}
	target, err := c.Document(ctx, "node2", "target")
	if err != nil {
		t.Fatal(err)
	}
	edited := latest.Clone()
	edit(&edited)
	edited.ChangedBy = "placement-process-test"
	preparation, err := topology.PreparePlacement(topology.ReadView{Latest: latest, Target: target}, edited,
		topology.Rules{SchemaVersion: 1, Limits: c.Spec.Limits}, time.Now().UTC())
	next, report := preparation.Document, preparation.Report
	if err != nil {
		t.Fatalf("prepare placement: %v, report=%+v", err, report)
	}
	response, err := c.Request(ctx, "node2", "PUT", "/api/topology", next, latest.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal("stage prepared placement", err)
	}
	var lastPlanResponse string
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node2", "POST", "/api/topology/preview/plan", next, 0)
		lastPlanResponse = fmt.Sprintf("status=%d body=%s error=%v", response.Status, response.Data, err)
		return err == nil && response.Status == 200, err
	}); err != nil {
		t.Fatal("prepared placement did not become plannable", err, lastPlanResponse)
	}
	if _, err := c.Approve(ctx, "node2", next.Version); err != nil {
		t.Fatal("approve prepared placement", err)
	}
	return next
}

func TestPlacementSelectorReservationAndStrictShortageThreeNodes(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 95*time.Second)
	defer cancel()
	labeled := placementEdit(ctx, t, c, func(doc *topology.Document) {
		for i, name := range []string{"node1", "node2", "node3"} {
			labels := doc.Node(name).GetLabels()
			labels["az"] = fmt.Sprintf("az-%d", i+1)
			labels["network"] = "private"
			if i < 2 {
				labels["network"] = "public_ip"
			}
			if err := doc.Node(name).SetLabels(labels); err != nil {
				t.Fatal(err)
			}
		}
	})

	strict := voterConfig(3)
	strict.DistinctVoterLabel = "az"
	response, err := c.Request(ctx, "node2", "POST", "/api/groups", map[string]any{
		"group": gquorum.GroupSpec{ID: "short", VoterConfig: strict,
			Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3,
				NodeSelector: map[string]string{"network": "public_ip"}}},
		"mutation": gquorum.MutationOptions{Approve: true},
	}, labeled.Version)
	if err := requireStatus(response, err, 400); err != nil || !strings.Contains(string(response.Data), "distinct voter domains") {
		t.Fatal("strict shortage did not explain refusal", err, string(response.Data))
	}
	before, err := c.Document(ctx, "node2", "latest")
	if err != nil || before.Group("short") != nil {
		t.Fatal("rejected strict group changed topology", before, err)
	}

	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "paid", Labels: map[string]string{"purpose": "accounting"},
		Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3,
			NodeSelector: map[string]string{"network": "public_ip"}}, VoterConfig: voterConfig(2)})
	if err != nil {
		t.Fatal(err)
	}
	if got := created.Document.Group("paid"); got == nil || len(got.Replicas) != 2 || got.Replicas[0].NodeName != "node1" || got.Replicas[1].NodeName != "node2" {
		t.Fatal("selector admitted an unlabelled host", got)
	}
	if err := c.Put(ctx, "node2", "paid", "invoice", "invoice", "retained"); err != nil {
		t.Fatal(err)
	}
	originalID := replicaID(created.Document, "paid", "node1")
	placementEdit(ctx, t, c, func(doc *topology.Document) {
		labels := doc.Node("node1").GetLabels()
		labels[topology.LabelReserved] = "true"
		if err := doc.Node("node1").SetLabels(labels); err != nil {
			t.Fatal(err)
		}
	})
	reserved, err := c.Document(ctx, "node2", "active")
	if err != nil || replicaID(reserved, "paid", "node1") != originalID {
		t.Fatal("reservation moved existing replica", reserved, err)
	}
	response, err = c.Request(ctx, "node2", "POST", "/api/groups", map[string]any{
		"group": gquorum.GroupSpec{ID: "new-on-reserved", Nodes: []string{"node1"}, VoterConfig: voterConfig(1),
			Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1}},
		"mutation": gquorum.MutationOptions{Approve: true},
	}, reserved.Version)
	if err := requireStatus(response, err, 400); err != nil || !strings.Contains(string(response.Data), "reserved") {
		t.Fatal("reservation allowed a fresh replica identity", err, string(response.Data))
	}
	if err := c.Node("node1").Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node1").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node1", "paid")
		return err == nil && local.Found && local.ReplicaID == originalID, err
	}); err != nil {
		t.Fatal("reserved exact replica did not recover", err)
	}
	if err := c.WaitValue(ctx, "node1", "paid", "invoice", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal("cluster did not regain meta readiness after reserved restart", err)
	}
	placementEdit(ctx, t, c, func(doc *topology.Document) {
		labels := doc.Node("node1").GetLabels()
		delete(labels, topology.LabelReserved)
		if err := doc.Node("node1").SetLabels(labels); err != nil {
			t.Fatal(err)
		}
	})
	placementEdit(ctx, t, c, func(doc *topology.Document) {
		for name, network := range map[string]string{"node1": "private", "node3": "public_ip"} {
			labels := doc.Node(name).GetLabels()
			labels["network"] = network
			if err := doc.Node(name).SetLabels(labels); err != nil {
				t.Fatal(err)
			}
		}
	})
	relocated, err := c.Document(ctx, "node2", "active")
	if err != nil || len(relocated.Group("paid").Replicas) != 2 || replicaID(relocated, "paid", "node3") == 0 || replicaID(relocated, "paid", "node1") != 0 {
		t.Fatal("selector edit did not relocate paid group", relocated, err)
	}
	if err := c.WaitValue(ctx, "node3", "paid", "invoice", "retained"); err != nil {
		t.Fatal(err)
	}
}

func TestPlacementLearnerOnlyIncludesMetaThreeNodes(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(3)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	placementEdit(ctx, t, c, func(doc *topology.Document) {
		labels := doc.Node("node3").GetLabels()
		labels[topology.LabelLearnerOnly] = "true"
		if err := doc.Node("node3").SetLabels(labels); err != nil {
			t.Fatal(err)
		}
	})
	active, err := c.Document(ctx, "node2", "active")
	if err != nil || slices.ContainsFunc(active.MetaGroup().Replicas, func(r topology.Replica) bool {
		return r.NodeName == "node3" && r.Role == topology.ReplicaVoter
	}) {
		t.Fatal("learner-only node still votes in meta", active, err)
	}
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "restricted", VoterConfig: voterConfig(2)})
	if err != nil {
		t.Fatal(err)
	}
	if r := created.Document.Group("restricted"); r == nil || len(r.Replicas) != 3 || slices.ContainsFunc(r.Replicas, func(replica topology.Replica) bool {
		return replica.NodeName == "node3" && replica.Role == topology.ReplicaVoter
	}) {
		t.Fatal("learner-only node voted in load group", r)
	}
	local, err := c.Local(ctx, "node3", "restricted")
	if err != nil || !local.Found || !slices.Contains(local.Status.ConfState.Learners, replicaID(created.Document, "restricted", "node3")) {
		t.Fatal("learner-only load replica did not converge", local, err)
	}
}

// The fourth process is necessary to keep a standby in the same AZ as an
// existing voter while the other two AZs each retain one voter.
func TestPlacementStrictStandbySwitchAndSurvivorLossFourNodes(t *testing.T) {
	t.Parallel()
	c := acquire(t, matrixSpec(4)).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	placementEdit(ctx, t, c, func(doc *topology.Document) {
		for i, name := range []string{"node1", "node2", "node3", "node4"} {
			labels := doc.Node(name).GetLabels()
			labels["az"] = []string{"a", "b", "c", "c"}[i]
			if err := doc.Node(name).SetLabels(labels); err != nil {
				t.Fatal(err)
			}
		}
	})
	voterConfig := voterConfig(3)
	voterConfig.DistinctVoterLabel = "az"
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "zoned", VoterConfig: voterConfig,
		Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if got := created.Document.Group("zoned"); got == nil || len(got.Replicas) != 4 || len(created.Document.VotersOf("zoned")) != 3 ||
		slices.ContainsFunc(got.Replicas, func(r topology.Replica) bool { return r.NodeName == "node4" && r.Role == topology.ReplicaVoter }) {
		t.Fatal("strict founding did not retain same-AZ standby", got)
	}
	if err := c.Put(ctx, "node4", "zoned", "before", "before", "retained"); err != nil {
		t.Fatal(err)
	}
	desired := created.Document.Clone()
	for i := range desired.Group("zoned").Replicas {
		r := &desired.Group("zoned").Replicas[i]
		if r.NodeName == "node3" {
			r.Role = topology.ReplicaLearner
		}
		if r.NodeName == "node4" {
			r.Role = topology.ReplicaVoter
		}
	}
	staged, err := c.Stage(ctx, "node2", desired)
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/topology/preview/plan", staged.Document, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var plan topology.Plan
	if err := response.Decode(&plan); err != nil {
		t.Fatal(err)
	}
	var roles []topology.StepKind
	for _, step := range plan.Steps {
		if step.GroupID == "zoned" && (step.Kind == topology.StepDemoteVoter || step.Kind == topology.StepPromoteVoter) {
			roles = append(roles, step.Kind)
		}
	}
	if !slices.Equal(roles, []topology.StepKind{topology.StepDemoteVoter, topology.StepPromoteVoter}) {
		t.Fatal("same-AZ switch did not use guarded 3 -> 2 -> 3 order", plan)
	}
	if err := c.Node("node2").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node1", "POST", "/api/topology/preview/plan", staged.Document, 0)
	if err != nil || response.Status == 200 {
		t.Fatal("survivor loss did not block demotion", err, response.Status, string(response.Data))
	}
	if err := c.Node("node2").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node2", "zoned")
		return err == nil && local.Found, err
	}); err != nil {
		t.Fatal("survivor did not recover", err)
	}
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node1", "POST", "/api/topology/preview/plan", staged.Document, 0)
		return err == nil && response.Status == 200, err
	}); err != nil {
		t.Fatal("safe plan did not become available after survivor recovery", err)
	}
	if _, err := c.Approve(ctx, "node1", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	active, err := c.Document(ctx, "node1", "active")
	if err != nil || len(active.VotersOf("zoned")) != 3 ||
		slices.ContainsFunc(active.VotersOf("zoned"), func(r topology.Replica) bool { return r.NodeName == "node3" }) {
		t.Fatal("strict standby replacement did not converge", active, err)
	}
	if err := c.WaitValue(ctx, "node4", "zoned", "before", "retained"); err != nil {
		t.Fatal(err)
	}
}
