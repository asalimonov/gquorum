//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/topology"
)

func TestIntentionalLearnerSurvivesReconciliationUntilApprovedPromotion(t *testing.T) {
	spec := matrixSpec(3)
	spec.Reconciliation = true
	spec.ReconciliationInterval, spec.ReconciliationTimeout = time.Second, 500*time.Millisecond
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node2", metaservice.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "before-demote", "before-demote", "retained"); err != nil {
		t.Fatal(err)
	}
	demoted := created.Document.Clone()
	id := replicaID(demoted, "work", "node3")
	for i := range demoted.Group("work").Replicas {
		if demoted.Group("work").Replicas[i].NodeName == "node3" {
			demoted.Group("work").Replicas[i].Role = topology.ReplicaLearner
		}
	}
	staged, err := c.Stage(ctx, "node2", demoted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Apply(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	assertLearner := func() error {
		own, err := c.Local(ctx, "node3", "work")
		if err != nil {
			return err
		}
		if !own.Found || own.ReplicaID != id || !slices.Contains(own.Status.ConfState.Learners, id) || slices.Contains(own.Status.ConfState.Voters, id) {
			return fmt.Errorf("intentional learner changed membership: %+v", own.Status)
		}
		return nil
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			status, err := c.Status(ctx, node.Spec.Name)
			if err != nil {
				return false, err
			}
			r := status.Reconciliation
			if r.Enabled && r.IsLeader && r.State == topology.ReconciliationIdle && r.Reason == "converged" {
				return true, assertLearner()
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("active reconciler never recognized the approved intentional learner", err)
	}
	// Four seconds spans several real one-second policy ticks. Check repeatedly,
	// rather than assuming a sleeping controller preserved the membership.
	until := time.NewTimer(4 * time.Second)
	defer until.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
observe:
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-until.C:
			break observe
		case <-ticker.C:
			if err := assertLearner(); err != nil {
				t.Fatal(err)
			}
		}
	}
	promoted, err := c.Document(ctx, "node2", "latest")
	if err != nil {
		t.Fatal(err)
	}
	for i := range promoted.Group("work").Replicas {
		if promoted.Group("work").Replicas[i].NodeName == "node3" {
			promoted.Group("work").Replicas[i].Role = topology.ReplicaVoter
		}
	}
	staged, err = c.Stage(ctx, "node2", promoted)
	if err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			status, err := c.Status(ctx, node.Spec.Name)
			if err != nil {
				return false, err
			}
			r := status.Reconciliation
			if r.Enabled && r.IsLeader && r.State == topology.ReconciliationBlocked && r.Reason == "staged_latest" {
				return true, assertLearner()
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("staged promotion did not leave the learner intact and block repair", err)
	}
	if _, err := c.Apply(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		own, err := c.Local(ctx, "node3", "work")
		return err == nil && own.Found && own.ReplicaID == id && slices.Contains(own.Status.ConfState.Voters, id) && !slices.Contains(own.Status.ConfState.Learners, id), err
	}); err != nil {
		t.Fatal("explicit approved promotion did not retain the exact replica", err)
	}
	if err := c.WaitValue(ctx, "node3", "work", "before-demote", "retained"); err != nil {
		t.Fatal(err)
	}
}
