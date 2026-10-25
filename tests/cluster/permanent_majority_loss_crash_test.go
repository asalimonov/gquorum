//go:build clustertest

package cluster

import (
	"context"
	"testing"

	"github.com/asalimonov/gquorum/recovery"
)

func recoveryControls(ctx context.Context, t *testing.T, c *Cluster, node string, controls map[string]bool) {
	t.Helper()
	response, err := c.Request(ctx, node, "PUT", "/internal/test/recovery/controls", controls, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
}
func waitRecoveryMetric(ctx context.Context, t *testing.T, c *Cluster, node, metric string) {
	t.Helper()
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, node, "GET", "/internal/test/recovery/metrics", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			return false, err
		}
		var counters map[string]uint64
		if err := response.Decode(&counters); err != nil {
			return false, err
		}
		return counters[metric] > 0, nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestPermanentMajorityLossRemoteCrashResume(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"prepared", "backed_up", "readiness"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			ctx, c, _ := permanentLossCluster(t)
			stopLostPeers(ctx, t, c, "node1")
			job := preparePermanentLoss(ctx, t, c, "node1")
			acceptance := recovery.Acceptance{PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
			if boundary != "prepared" {
				controls := map[string]bool{"hold_after_backup": boundary == "backed_up", "hold_readiness": boundary == "readiness"}
				recoveryControls(ctx, t, c, "node1", controls)
				if _, err := c.RecoveryCommand(ctx, "node1", "job", "confirm", acceptance); err != nil {
					t.Fatal(err)
				}
				waitRecoveryMetric(ctx, t, c, "node1", boundary)
				if boundary == "readiness" {
					response, err := c.Request(ctx, "node1", "GET", "/internal/test/recovery/metrics", nil, 0)
					if err := requireStatus(response, err, 200); err != nil {
						t.Fatal(err)
					}
					var counters map[string]uint64
					if err := response.Decode(&counters); err != nil {
						t.Fatal(err)
					}
					if counters["runs"] != 1 {
						t.Fatal("Run escaped validation", counters)
					} // only A's original factory ran
				}
			}
			if err := c.Node("node1").Kill(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.Node("node1").Start(ctx); err != nil {
				t.Fatal(err)
			}
			if boundary == "prepared" {
				resumed, err := c.RecoveryJob(ctx, "node1", "job")
				if err != nil {
					t.Fatal(err)
				}
				if !resumed.Maintenance || resumed.Plan.Digest != job.Plan.Digest || resumed.Acceptance != nil {
					t.Fatal("lost preparation", resumed)
				}
				if _, err := c.RecoveryCommand(ctx, "node1", "job", "confirm", acceptance); err != nil {
					t.Fatal(err)
				}
			}
			complete, err := c.WaitRecoveryJob(ctx, "node1", "job", "complete")
			if err != nil {
				t.Fatal(err)
			}
			if complete.Request.RecoveryID != job.Request.RecoveryID || complete.Acceptance == nil || *complete.Acceptance != acceptance ||
				complete.Status == nil || complete.Status.Phase != recovery.Complete {
				t.Fatal("crash changed accepted identity", complete)
			}
			if err := c.WaitValue(ctx, "node1", "work", "key", "before"); err != nil {
				t.Fatal(err)
			}
			response, err := c.Request(ctx, "node1", "POST", "/internal/test/recovery/job/cancel", RecoveryCancellation{Actor: "operator", Reason: "too late"}, 0)
			if err != nil || response.Status != 409 {
				t.Fatal("post-archive cancellation was accepted", response, err)
			}
		})
	}
}
func TestPermanentMajorityLossRemoteCancelUndoRetry(t *testing.T) {
	t.Parallel()
	ctx, c, _ := permanentLossCluster(t)
	stopLostPeers(ctx, t, c, "node1")
	job := preparePermanentLoss(ctx, t, c, "node1")
	recoveryControls(ctx, t, c, "node1", map[string]bool{"hold_after_backup": true, "fail_undo_once": true})
	if _, err := c.RecoveryCommand(ctx, "node1", "job", "confirm", recovery.Acceptance{
		PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}); err != nil {
		t.Fatal(err)
	}
	waitRecoveryMetric(ctx, t, c, "node1", "backed_up")
	audit := RecoveryCancellation{Actor: "operator", Reason: "operator cancelled before archive"}
	if _, err := c.RecoveryCommand(ctx, "node1", "job", "cancel", audit); err != nil {
		t.Fatal(err)
	}
	failed := waitLossJobFailure(ctx, t, c, "node1", "job")
	if !failed.Maintenance || failed.Phase != "cancelling" {
		t.Fatal("Undo failure released maintenance", failed)
	}
	recoveryControls(ctx, t, c, "node1", map[string]bool{})
	if _, err := c.RecoveryCommand(ctx, "node1", "job", "retry", nil); err != nil {
		t.Fatal(err)
	}
	cancelled, err := c.WaitRecoveryJob(ctx, "node1", "job", "cancelled")
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Maintenance || cancelled.Status == nil || cancelled.Status.Phase != recovery.Cancelled {
		t.Fatal(cancelled)
	}
	// Restore A quorum using the same old volumes. Cancellation did not change
	// cluster/replica identities and never fabricated a B import.
	for _, name := range []string{"node2", "node3"} {
		if err := c.Node(name).Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitValue(ctx, "node1", "work", "key", "before"); err != nil {
		t.Fatal(err)
	}
	if local, err := c.Local(ctx, "node1", "work"); err != nil || local.Identity.ClusterID != c.ID {
		t.Fatal(local, err)
	}
	response, err := c.Request(ctx, "node1", "POST", "/internal/test/recovery/job/cancel", RecoveryCancellation{Actor: "different", Reason: "changed"}, 0)
	if err != nil || response.Status != 409 {
		t.Fatal("changed cancellation audit accepted", response, err)
	}
}

func TestPermanentMajorityLossRemotePreparationCancel(t *testing.T) {
	t.Parallel()
	ctx, c, _ := permanentLossCluster(t)
	stopLostPeers(ctx, t, c, "node1")
	job := preparePermanentLoss(ctx, t, c, "node1")
	for _, acceptance := range []recovery.Acceptance{
		{PlanDigest: "stale", AcknowledgeDataLoss: true, OldDeploymentFenced: true},
		{PlanDigest: job.Plan.Digest, OldDeploymentFenced: true},
		{PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true},
	} {
		response, err := c.Request(ctx, "node1", "POST", "/internal/test/recovery/job/confirm", acceptance, 0)
		if err != nil || response.Status != 400 {
			t.Fatal("changed/incomplete acceptance was accepted", response, err)
		}
	}
	changed := job.Request
	changed.ClusterID = "different-destination"
	response, err := c.Request(ctx, "node1", "POST", "/internal/test/recovery/prepare", map[string]any{"id": "job", "request": changed}, 0)
	if err != nil || response.Status != 409 {
		t.Fatal("saved destination was replaced", response, err)
	}
	audit := RecoveryCancellation{Actor: "operator", Reason: "cancel before approval"}
	if _, err := c.RecoveryCommand(ctx, "node1", "job", "cancel", audit); err != nil {
		t.Fatal(err)
	}
	cancelled, err := c.WaitRecoveryJob(ctx, "node1", "job", "cancelled")
	if err != nil || cancelled.Maintenance || cancelled.Operation != nil || cancelled.Status != nil {
		t.Fatal("preparation became library recovery", cancelled, err)
	}
	response, err = c.Request(ctx, "node1", "POST", "/internal/test/recovery/job/confirm", recovery.Acceptance{
		PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}, 0)
	if err != nil || response.Status != 400 {
		t.Fatal("cancelled job was rearmed", response, err)
	}
	for _, name := range []string{"node2", "node3"} {
		if err := c.Node(name).Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		err := c.Put(ctx, "node1", "work", "resumed", "after-cancel", "A")
		return err == nil, err
	}); err != nil {
		t.Fatal(err)
	}
}
