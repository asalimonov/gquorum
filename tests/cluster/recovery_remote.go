//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"net/url"

	"github.com/asalimonov/gquorum/recovery"
)

// RecoveryJob is caller-owned maintenance state, returned by the authenticated
// fixture while its embedded Node is stopped. Status is historical observation.
type RecoveryJob struct {
	ID            string                `json:"id"`
	SourceCluster string                `json:"source_cluster"`
	Maintenance   bool                  `json:"maintenance"`
	Phase         string                `json:"phase"`
	Request       recovery.Request      `json:"request"`
	Plan          *recovery.Plan        `json:"plan,omitempty"`
	Acceptance    *recovery.Acceptance  `json:"acceptance,omitempty"`
	Operation     *recovery.Operation   `json:"operation,omitempty"`
	Status        *recovery.Status      `json:"status,omitempty"`
	Cancellation  *RecoveryCancellation `json:"cancellation,omitempty"`
	Error         string                `json:"error,omitempty"`
}

// RecoveryCancellation is a stable audit intent for remote cancellation.
type RecoveryCancellation struct {
	Actor  string `json:"actor"`
	Reason string `json:"reason"`
}

// PrepareRecovery persists caller maintenance and starts inspection independently
// of this HTTP waiter. Repeating id/request returns the same generated plan.
func (c *Cluster) PrepareRecovery(ctx context.Context, node, id string, request recovery.Request) (RecoveryJob, error) {
	response, err := c.Request(ctx, node, "POST", "/internal/test/recovery/prepare", map[string]any{"id": id, "request": request}, 0)
	if err := requireStatus(response, err, 202); err != nil {
		return RecoveryJob{}, err
	}
	var job RecoveryJob
	err = response.Decode(&job)
	return job, err
}

// RecoveryJob reads public library progress and the application job, without quorum.
func (c *Cluster) RecoveryJob(ctx context.Context, node, id string) (RecoveryJob, error) {
	response, err := c.Request(ctx, node, "GET", "/internal/test/recovery/"+url.PathEscape(id), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return RecoveryJob{}, err
	}
	var job RecoveryJob
	err = response.Decode(&job)
	return job, err
}

// RecoveryCommand confirms, retries or cancels one saved fixture job.
func (c *Cluster) RecoveryCommand(ctx context.Context, node, id, action string, body any) (RecoveryJob, error) {
	response, err := c.Request(ctx, node, "POST", "/internal/test/recovery/"+url.PathEscape(id)+"/"+url.PathEscape(action), body, 0)
	if err := requireStatus(response, err, 202); err != nil {
		return RecoveryJob{}, err
	}
	var job RecoveryJob
	err = response.Decode(&job)
	return job, err
}

// WaitRecoveryJob waits for an observable caller phase, surfacing saved failures.
func (c *Cluster) WaitRecoveryJob(ctx context.Context, node, id, phase string) (RecoveryJob, error) {
	var job RecoveryJob
	err := Poll(ctx, func() (bool, error) {
		var err error
		job, err = c.RecoveryJob(ctx, node, id)
		if err != nil {
			return false, err
		}
		if job.Error != "" {
			return false, fmt.Errorf("recovery %s: %s", job.Phase, job.Error)
		}
		return job.Phase == phase, nil
	})
	return job, err
}

// RequireRecoveryTail refuses a false-positive REC-04 setup. Proposal timeouts,
// live status and drop counters alone cannot establish a retained uncommitted tail.
func RequireRecoveryTail(job RecoveryJob, group string) error {
	if job.Plan == nil {
		return fmt.Errorf("recovery has no inspected plan")
	}
	for _, row := range job.Plan.Groups {
		if row.GroupID == group && row.Usable && row.LastLog.Index > row.Commit.Index {
			return nil
		}
	}
	return fmt.Errorf("inspection did not prove a retained tail for %s", group)
}
