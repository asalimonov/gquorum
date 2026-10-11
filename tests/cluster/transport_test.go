//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestDeadPeerPreservesContinuousLeadershipThreeNodes(t *testing.T) {
	t.Parallel()
	deadPeerLeadership(t, 3)
}

type leadership struct{ leader, term uint64 }

// Every expected observer must answer. A missing leader or response is a
// failure, never an absent metric that can hide a brief step-down/re-election.
func inspectLeaders(ctx context.Context, c *Cluster, document topology.Document, expected map[string]leadership) error {
	for _, node := range c.Nodes() {
		if !node.running.Load() {
			continue
		}
		metaLocal, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		if err != nil {
			return err
		}
		native := metaLocal.Status
		want := expected[topology.MetaGroupID]
		if !native.Found || native.HostNode != node.Spec.Name || native.LeaderID != want.leader || native.Term != want.term {
			return fmt.Errorf("%s meta leadership changed: %+v, want %+v", node.Spec.Name, native, want)
		}
		for _, group := range document.LoadGroups() {
			if replicaID(document, group.ID, node.Spec.Name) == 0 {
				continue
			}
			local, err := c.Local(ctx, node.Spec.Name, group.ID)
			if err != nil {
				return err
			}
			want := expected[group.ID]
			if !local.Found || local.ReplicaID != replicaID(document, group.ID, node.Spec.Name) || local.Status.LeaderID != want.leader || local.Status.Term != want.term {
				return fmt.Errorf("%s/%s native leadership changed: %+v, want %+v", node.Spec.Name, group.ID, local.Status, want)
			}
		}
	}
	return nil
}

func deadPeerLeadership(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	groups := []string{"alpha", "beta"}
	var document topology.Document
	for _, id := range groups {
		created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: id, Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
		if err != nil {
			t.Fatal(err)
		}
		document = created.Document
	}
	if err := transferMeta(ctx, c, "node2", "node1", document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	for i, id := range groups {
		name := fmt.Sprintf("node%d", i+1)
		response, err := c.Request(ctx, "node3", "POST", "/api/groups/"+id+"/leader/"+name, nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	expected := map[string]leadership{}
	if err := Poll(ctx, func() (bool, error) {
		metaLocal, err := c.Local(ctx, "node1", topology.MetaGroupID)
		metaStatus := metaLocal.Status
		if err != nil {
			return false, err
		}
		expected[topology.MetaGroupID] = leadership{document.MemberID("node1"), metaStatus.Term}
		if metaStatus.Term == 0 {
			return false, nil
		}
		for i, id := range groups {
			local, err := c.Local(ctx, c.Spec.Nodes[i].Name, id)
			if err != nil {
				return false, err
			}
			if local.Status.Term == 0 {
				return false, nil
			}
			expected[id] = leadership{replicaID(document, id, c.Spec.Nodes[i].Name), local.Status.Term}
		}
		err = inspectLeaders(ctx, c, document, expected)
		return err == nil, err
	}); err != nil {
		t.Fatal("every node must confirm settled leadership before kill", err)
	}
	victim := c.Node("node3")
	for id, leader := range expected {
		own := document.MemberID(victim.Spec.Name)
		if id != topology.MetaGroupID {
			own = replicaID(document, id, victim.Spec.Name)
		}
		if own == leader.leader {
			t.Fatal("victim holds leadership", id, leader)
		}
	}
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	// Preserve the source's full 20-second continuous observation window. Apply
	// real traffic concurrently, and join before checking every exact command.
	observedAt := time.Now()
	writeResults := make(chan error, 1)
	acknowledged := 0 // read only after joining the writer
	go func() {
		var err error
		defer func() { writeResults <- err }()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for time.Since(observedAt) < 20*time.Second {
			for _, id := range groups {
				key := fmt.Sprintf("during-dead-peer-%03d", acknowledged)
				if err = c.Put(ctx, "node2", id, key, key, id); err != nil {
					return
				}
			}
			acknowledged++
			select {
			case <-ticker.C:
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}
	}()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var observationErr error
	observations := 0
	for time.Since(observedAt) < 20*time.Second {
		if err := inspectLeaders(ctx, c, document, expected); err != nil {
			observationErr = err
			break
		}
		observations++
		select {
		case <-ticker.C:
		case <-ctx.Done():
			observationErr = ctx.Err()
		}
		if observationErr != nil {
			break
		}
	}
	writeErr := <-writeResults
	if observationErr != nil || writeErr != nil || observations < 20 || acknowledged < 20 {
		t.Fatal("dead peer disturbed continuous leadership or acknowledged traffic", observationErr, writeErr, observations, acknowledged)
	}
	for _, name := range []string{"node1", "node2"} {
		for _, id := range groups {
			if err := Poll(ctx, func() (bool, error) {
				local, err := c.Local(ctx, name, id)
				if err != nil {
					return false, err
				}
				if len(local.Values) != acknowledged {
					return false, fmt.Errorf("%s/%s has %d commands, want %d", name, id, len(local.Values), acknowledged)
				}
				for i := range acknowledged {
					if local.Values[fmt.Sprintf("during-dead-peer-%03d", i)] != id {
						return true, fmt.Errorf("%s/%s lost acknowledged command %d", name, id, i)
					}
				}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The same abrupt-killed process must catch up without allocating another ID.
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range groups {
		if err := Poll(ctx, func() (bool, error) {
			local, err := c.Local(ctx, victim.Spec.Name, id)
			if err != nil {
				return false, err
			}
			if !local.Found || local.ReplicaID != replicaID(document, id, victim.Spec.Name) || len(local.Values) != acknowledged {
				return false, nil
			}
			for i := range acknowledged {
				if local.Values[fmt.Sprintf("during-dead-peer-%03d", i)] != id {
					return false, nil
				}
			}
			return true, nil
		}); err != nil {
			t.Fatal("abrupt-killed replica lost acknowledged traffic", id, err)
		}
	}
	t.Logf("METRIC nodes=%d observation_and_recovery_ms=%.3f observations=%d acknowledged_per_group=%d", size, float64(time.Since(observedAt))/float64(time.Millisecond), observations, acknowledged)
}
