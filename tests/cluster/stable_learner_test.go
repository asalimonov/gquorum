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

func clusterMetrics(ctx context.Context, c *Cluster, node string) (metrics.ClusterSnapshot, error) {
	response, err := c.Request(ctx, node, "GET", "/internal/test/metrics", nil, 0)
	var sample metrics.ClusterSnapshot
	if err := requireStatus(response, err, 200); err != nil {
		return sample, err
	}
	return sample, response.Decode(&sample)
}

// Five nodes are required here to keep a second promotable standby and prove
// that one failed voter causes exactly one promotion in each affected group.
// The second standby also permits a second failure during the first cooldown,
// while the retained former voter remains ineligible for re-promotion.
func TestStableLearnerFiveNodesAutomaticPromotionAndRetainedReturn(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(5)
	spec.Scheduling = true
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 360*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node5", gquorum.GroupSpec{ID: "work"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{topology.MetaGroupID, "work"} {
		g := created.Document.Group(id)
		if g == nil || len(g.Replicas) != 5 || len(created.Document.VotersOf(id)) != 3 || g.VoterPolicy == nil {
			t.Fatalf("wrong default %s: %+v", id, g)
		}
	}
	// This calls the exact learner handle, bypassing the fixture's HTTP leader
	// routing. The reply is sent only after the command applies on that learner.
	learnerID := replicaID(created.Document, "work", "node5")
	response, err := c.Request(ctx, "node5", "POST", fmt.Sprintf("/internal/test/propose/work/%d", learnerID), map[string]string{"request_id": "stable-learner-forward", "key": "forwarded", "value": "retained"}, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal("learner forwarding", err)
	}
	local, err := c.Local(ctx, "node5", "work")
	if err != nil || local.Values["forwarded"] != "retained" {
		t.Fatal("learner replied before local apply", local, err)
	}
	var collector string
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			snapshot, err := clusterMetrics(ctx, c, node.Spec.Name)
			if err != nil {
				return false, err
			}
			if !snapshot.IsCollectorLeader {
				continue
			}
			ready := 0
			for _, g := range snapshot.Groups {
				if (g.GroupID == topology.MetaGroupID || g.GroupID == "work") && g.State == metrics.Current && g.EligibleStandbys == 2 {
					ready++
				}
			}
			if ready == 2 {
				collector = node.Spec.Name
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		for _, node := range c.Nodes() {
			sample, e := clusterMetrics(context.Background(), c, node.Spec.Name)
			t.Logf("metrics %s: %+v %v", node.Spec.Name, sample, e)
		}
		t.Fatal("two standbys never became eligible", err)
	}
	t.Log("both groups have two qualified standbys; stopping a voter")
	victim := "node3"
	if collector == victim {
		victim = "node2"
	}
	if err := c.Node(victim).Kill(ctx); err != nil {
		t.Fatal(err)
	}
	var converged topology.Document
	if err := Poll(ctx, func() (bool, error) {
		doc, err := c.Document(ctx, collector, "active")
		if err != nil {
			return false, err
		}
		for _, id := range []string{topology.MetaGroupID, "work"} {
			group := doc.Group(id)
			if group == nil || len(doc.VotersOf(id)) != 3 {
				return false, nil
			}
			former := replicaID(created.Document, id, victim)
			replaced := false
			for _, r := range group.Replicas {
				if r.ReplicaID == former && r.NodeName == victim && r.Role == topology.ReplicaLearner {
					replaced = true
				}
			}
			if !replaced {
				return false, nil
			}
			// All four survivors must have applied the same native voting membership.
			for _, node := range c.Nodes() {
				if node.Spec.Name == victim {
					continue
				}
				local, err := c.Local(ctx, node.Spec.Name, id)
				if err != nil {
					return false, err
				}
				if len(local.Status.ConfState.Voters) != 3 || len(local.Status.ConfState.Learners) != 2 || !slices.Contains(local.Status.ConfState.Learners, former) {
					return false, nil
				}
			}
		}
		latest, err := c.Document(ctx, collector, "latest")
		if err != nil {
			return false, err
		}
		if latest.Version != doc.Version {
			return false, nil
		}
		status, err := c.Status(ctx, collector)
		if err != nil {
			return false, err
		}
		if status.Operator != nil && status.Operator.State != topology.OperatorSucceeded {
			return false, nil
		}
		converged = doc
		return true, nil
	}); err != nil {
		sample, e := clusterMetrics(context.Background(), c, collector)
		t.Logf("metrics: %+v %v", sample, e)
		t.Fatal("automatic replacement did not converge", err)
	}
	// The collector exposes cooldowns derived from verified replicated role
	// records, never from merely proposed policy changes.

	if err := Poll(ctx, func() (bool, error) {
		snapshot, err := clusterMetrics(ctx, c, collector)
		if err != nil {
			return false, err
		}
		if snapshot.ActiveVersion != converged.Version || !snapshot.IsCollectorLeader {
			return false, nil
		}
		for _, id := range []string{topology.MetaGroupID, "work"} {
			if !slices.ContainsFunc(snapshot.Groups, func(g metrics.GroupObservation) bool { return g.GroupID == id && g.CooldownRemaining > 0 }) {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("completed switch records never reached metrics", err)
	}

	if err := c.Node(victim).Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, id := range []string{topology.MetaGroupID, "work"} {
			local, err := c.Local(ctx, victim, id)
			if err != nil {
				return false, err
			}
			old := replicaID(created.Document, id, victim)
			if !local.Found || local.ReplicaID != old || !slices.Contains(local.Status.ConfState.Learners, old) || slices.Contains(local.Status.ConfState.Voters, old) {
				return false, nil
			}
		}
		doc, err := c.Document(ctx, victim, "active")
		return err == nil && doc.Version == converged.Version, err
	}); err != nil {
		t.Fatal("retained voter did not return as the same learner", err)
	}
	local, err = c.Local(ctx, victim, "work")
	if err != nil || local.Values["forwarded"] != "retained" {
		t.Fatal("retained learner lost application state", local, err)
	}

	// Align leaders before rebuilding the remaining standby's window. This lets
	// the same observer see a healthy voter after the first durable switch and
	// then its new failure, without an observer failover rearming all evidence.
	response, err = c.Request(ctx, collector, "POST", "/api/groups/work/leader/"+collector, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	standbys := map[string]topology.ReplicaKey{}
	for _, id := range []string{topology.MetaGroupID, "work"} {
		for _, r := range converged.Group(id).Replicas {
			if r.Role == topology.ReplicaLearner && r.NodeName != victim {
				standbys[id] = topology.ReplicaKey{NodeName: r.NodeName, ReplicaID: r.ReplicaID}
			}
		}
	}
	var cooldownDeadline time.Time
	if err := Poll(ctx, func() (bool, error) {
		snapshot, err := clusterMetrics(ctx, c, collector)
		if err != nil || !snapshot.IsCollectorLeader || snapshot.ActiveVersion != converged.Version {
			return false, err
		}
		ready := 0
		remaining := 5 * time.Minute
		for _, group := range snapshot.Groups {
			candidate, ok := standbys[group.GroupID]
			if !ok || group.State != metrics.Current || group.EligibleStandbys != 1 || group.CooldownRemaining <= 90*time.Second {
				continue
			}
			qualified, suppressed := false, false
			for _, r := range group.Replicas {
				if r.Sample.Identity.NodeName == candidate.NodeName && r.Sample.Identity.ReplicaID == candidate.ReplicaID {
					qualified = r.Evidence.StandbyReadyFor.State == metrics.Current && r.Evidence.StandbyReadyFor.Value >= converged.Group(group.GroupID).VoterPolicy.StandbyWindow
				}
				if r.Sample.Identity.NodeName == victim {
					suppressed = r.Evidence.DemotionCooldownRemaining > 0
				}
			}
			if qualified && suppressed {
				ready++
				remaining = min(remaining, group.CooldownRemaining)
			}
		}
		if ready != 2 {
			return false, nil
		}
		// Leave margin for response transit; this deadline proves replacement
		// completed before the first cooldown could expire on the observer.
		cooldownDeadline = time.Now().Add(remaining - 5*time.Second)
		return true, nil
	}); err != nil {
		t.Fatal("remaining standby did not qualify during cooldown", err)
	}
	secondVictim := ""
	for _, name := range []string{"node1", "node2", "node3"} {
		if name != victim && name != collector {
			secondVictim = name
			break
		}
	}
	if secondVictim == "" {
		t.Fatal("no original voter available for the second failure")
	}
	t.Log("remaining standby qualified while former voter is cooling; stopping", secondVictim)
	if err := c.Node(secondVictim).Kill(ctx); err != nil {
		t.Fatal(err)
	}
	withinCooldown, finish := context.WithDeadline(ctx, cooldownDeadline)
	defer finish()
	if err := Poll(withinCooldown, func() (bool, error) {
		doc, err := c.Document(withinCooldown, collector, "active")
		if err != nil {
			return false, err
		}
		for _, id := range []string{topology.MetaGroupID, "work"} {
			group := doc.Group(id)
			if group == nil || len(doc.VotersOf(id)) != 3 {
				return false, nil
			}
			for _, r := range group.Replicas {
				if r.NodeName == victim || r.NodeName == secondVictim {
					if r.Role != topology.ReplicaLearner || r.ReplicaID != replicaID(created.Document, id, r.NodeName) {
						return false, nil
					}
				}
				if r.NodeName == standbys[id].NodeName && (r.Role != topology.ReplicaVoter || r.ReplicaID != standbys[id].ReplicaID) {
					return false, nil
				}
			}
			for _, node := range c.Nodes() {
				if node.Spec.Name == secondVictim {
					continue
				}
				local, err := c.Local(withinCooldown, node.Spec.Name, id)
				if err != nil {
					return false, err
				}
				if len(local.Status.ConfState.Voters) != 3 || len(local.Status.ConfState.Learners) != 2 || !slices.Contains(local.Status.ConfState.Voters, standbys[id].ReplicaID) || !slices.Contains(local.Status.ConfState.Learners, replicaID(created.Document, id, secondVictim)) {
					return false, nil
				}
			}
		}
		latest, err := c.Document(withinCooldown, collector, "latest")
		if err != nil || latest.Version != doc.Version {
			return false, err
		}
		status, err := c.Status(withinCooldown, collector)
		return err == nil && status.Operator != nil && status.Operator.State == topology.OperatorSucceeded && status.Operator.Version == doc.Version, err
	}); err != nil {
		snapshot, readErr := clusterMetrics(context.Background(), c, collector)
		t.Logf("metrics after second failure: %+v %v", snapshot, readErr)
		t.Fatal("new failure did not replace through the group cooldown", err)
	}
}
