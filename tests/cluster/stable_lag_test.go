//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// Four replicas distinguish an alive but non-replicating voter from its ready
// standby. This also exercises per-group threshold/window overrides end to end.
func TestStableLearnerReplacesReplicationLagWhileNodeStaysAliveFourNodes(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(4)
	spec.Scheduling = true
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	p := topology.DefaultVoterPolicy()
	p.FailureWindow, p.StandbyWindow = 5*time.Second, 5*time.Second
	p.FailureLagEntries, p.StandbyLagEntries = 10, 1
	p.ReplaceNoProgress = false
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "lag", VoterPolicy: &p})
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node2", "POST", "/api/groups/lag/leader/node1", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.SetBarrier(ctx, node.Spec.Name, "lag", topology.StepPromoteVoter, true); err != nil {
			t.Fatal(err)
		}
	}
	var collector string
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			snapshot, err := clusterMetrics(ctx, c, node.Spec.Name)
			if err != nil {
				return false, err
			}
			for _, group := range snapshot.Groups {
				if snapshot.IsCollectorLeader && group.GroupID == "lag" && group.EligibleStandbys == 1 {
					collector = node.Spec.Name
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("standby did not qualify", err)
	}
	victim := replicaID(created.Document, "lag", "node3")
	t.Cleanup(func() {
		cleanup, finish := context.WithTimeout(context.Background(), 5*time.Second)
		defer finish()
		if err := c.WALFault(cleanup, "node3", "lag", victim, false, 0, true); err != nil {
			t.Error(err)
		}
	})
	if err := c.WALFault(ctx, "node3", "lag", victim, true, 0, false); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		key := fmt.Sprintf("lag-%d", i)
		if err := c.Put(ctx, "node1", "lag", key, key, "retained"); err != nil {
			t.Fatal(err)
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		snapshot, err := clusterMetrics(ctx, c, collector)
		if err != nil {
			return false, err
		}
		for _, group := range snapshot.Groups {
			if group.GroupID != "lag" {
				continue
			}
			for _, r := range group.Replicas {
				if r.Sample.Identity.ReplicaID == victim && r.Reachability == "alive" && r.ReplicationLag.State == metrics.Current && r.ReplicationLag.Value > p.FailureLagEntries && r.Evidence.ReplicationLagFor.State == metrics.Current && r.Evidence.ReplicationLagFor.Value >= p.FailureWindow && r.Evidence.UnavailableFor.Value == 0 {
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("live voter's replication lag did not qualify independently", err)
	}
	var operator topology.OperatorID
	if err := Poll(ctx, func() (bool, error) {
		controls, err := c.Controls(ctx, "node1")
		if err != nil {
			return false, err
		}
		for _, b := range controls.Barriers {
			if b.Hits > 0 && b.Last.Kind == topology.StepPromoteVoter && b.Last.GroupID == "lag" && b.Last.ReplicaID == replicaID(created.Document, "lag", "node4") {
				operator = b.Last.OperatorID
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("lag did not approve exact learner promotion", err)
	}
	status, err := c.Status(ctx, "node3")
	if err != nil || !status.SessionLive {
		t.Fatal("replication failure incorrectly depended on node death", status, err)
	}
	for _, node := range c.Nodes() {
		if err := c.SetBarrier(ctx, node.Spec.Name, "lag", topology.StepPromoteVoter, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitOperator(ctx, collector, operator); err != nil {
		t.Fatal(err)
	}
	if err := c.WALFault(ctx, "node3", "lag", victim, false, 0, true); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitActive(ctx, created.Document.Version+1); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := Poll(ctx, func() (bool, error) {
			local, err := c.Local(ctx, node.Spec.Name, "lag")
			return local.Found && len(local.Status.ConfState.Voters) == 3 && len(local.Status.ConfState.Learners) == 1 && slices.Contains(local.Status.ConfState.Learners, victim) && local.Values["lag-19"] == "retained", err
		}); err != nil {
			t.Fatal("lag replacement did not retain data and learner identity", node.Spec.Name, err)
		}
	}
}
