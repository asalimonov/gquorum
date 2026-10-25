//go:build clustertest

package cluster

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func waitLossJobFailure(ctx context.Context, t *testing.T, c *Cluster, node, id string) RecoveryJob {
	t.Helper()
	var job RecoveryJob
	if err := Poll(ctx, func() (bool, error) {
		var err error
		job, err = c.RecoveryJob(ctx, node, id)
		return job.Error != "", err
	}); err != nil {
		t.Fatal(err)
	}
	return job
}
func TestPermanentMajorityLossIncompleteSurvivorRefuses(t *testing.T) {
	t.Parallel()
	ctx, c, _ := permanentLossCluster(t)
	voters := topology.DefaultVoterConfig()
	voters.TargetVoters = 1
	if _, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "missing-on-survivor", Nodes: []string{"node2"},
		VoterConfig: &voters, Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1}}); err != nil {
		t.Fatal(err)
	}
	stopLostPeers(ctx, t, c, "node1")
	if _, err := c.PrepareRecovery(ctx, "node1", "job", recovery.Request{RecoveryID: "incomplete", Mode: recovery.Rebuild,
		Actor: "operator", Comment: "incomplete survivor"}); err != nil {
		t.Fatal(err)
	}
	job := waitLossJobFailure(ctx, t, c, "node1", "job")
	if !job.Maintenance || job.Plan == nil || !strings.Contains(job.Error, "missing") {
		t.Fatal("incomplete source was accepted", job)
	}
	missing := false
	for _, issue := range job.Plan.Issues {
		if issue.GroupID == "missing-on-survivor" && issue.Blocking {
			missing = true
		}
	}
	if !missing {
		t.Fatal("missing group was not named", job.Plan)
	}
	response, err := c.Request(ctx, "node1", "GET", "/internal/test/recovery", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var list struct {
		Operations []recovery.Status `json:"operations"`
	}
	if err := response.Decode(&list); err != nil || len(list.Operations) != 0 {
		t.Fatal("inspection mutated library recovery state", list, err)
	}
}
func TestPermanentMajorityLossApprovedInventoryArchivesOldWork(t *testing.T) {
	t.Parallel()
	ctx, c, _ := permanentLossCluster(t)
	debt, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "cleanup-debt",
		Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetBarrier(ctx, "node3", "cleanup-debt", topology.StepStopReplica, true); err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node1", "DELETE", "/api/groups/cleanup-debt",
		gquorum.MutationOptions{Actor: "operator", Approve: true}, debt.Document.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var deletion gquorum.MutationResult
	if err := response.Decode(&deletion); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		controls, err := c.Controls(ctx, "node3")
		for _, barrier := range controls.Barriers {
			if barrier.Group == "cleanup-debt" && barrier.Kind == topology.StepStopReplica && barrier.Hits > 0 {
				return true, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	latest, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	stagedOnly := *latest.Group("work")
	stagedOnly.ID = "unapproved"
	latest.Groups = append(latest.Groups, stagedOnly)
	latest.ChangedBy = "operator"
	staged, err := c.Stage(ctx, "node1", latest)
	if err != nil {
		t.Fatal(err)
	}
	// Confirm the exact survivor applied both the approval and staged intent before
	// losing peers. Latest cannot become recovery authorization accidentally.
	if err := Poll(ctx, func() (bool, error) {
		got, err := c.Document(ctx, "node1", "latest")
		return got.Version == staged.Document.Version, err
	}); err != nil {
		t.Fatal(err)
	}
	stopLostPeers(ctx, t, c, "node1")
	job := preparePermanentLoss(ctx, t, c, "node1")
	if job.Plan.Archived.ApprovedVersion != deletion.Document.Version || job.Plan.Archived.LatestVersion != staged.Document.Version ||
		!slices.Contains(job.Plan.Archived.OperationsNotResumed, deletion.OperationID) || len(job.Plan.Archived.UnresolvedCleanup) == 0 {
		t.Fatal("old work evidence was dropped or relabelled complete", job.Plan.Archived)
	}
	for _, group := range job.Plan.Groups {
		if group.GroupID == "unapproved" || group.GroupID == "cleanup-debt" {
			t.Fatal("recovered unapproved/deleted group", group)
		}
	}
	confirmPermanentLoss(ctx, t, c, "node1", job)
	target, err := c.Document(ctx, "node1", "target")
	if err != nil || target.Group("work") == nil || target.Group("unapproved") != nil || target.Group("cleanup-debt") != nil {
		t.Fatal(target, err)
	}
}
