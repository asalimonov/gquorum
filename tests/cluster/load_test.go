//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"maps"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestLoadIngressAndExactIndependentCommandsThreeNodes(t *testing.T) {
	t.Parallel()
	loadIngressAndAccounting(t, 3)
}

func exactOwnedValues(ctx context.Context, c *Cluster, document topology.Document, group string, expected map[string]string) error {
	return Poll(ctx, func() (bool, error) {
		for _, replica := range document.Group(group).Replicas {
			local, err := c.Local(ctx, replica.NodeName, group)
			if err != nil {
				return false, err
			}
			if !local.Found || local.ReplicaID != replica.ReplicaID {
				return true, fmt.Errorf("%s/%s owns another replica identity", replica.NodeName, group)
			}
			if len(local.Values) > len(expected) {
				return true, fmt.Errorf("%s/%s has unexpected commands: %v", replica.NodeName, group, local.Values)
			}
			if !maps.Equal(local.Values, expected) {
				return false, fmt.Errorf("%s/%s command results do not match: %v", replica.NodeName, group, local.Values)
			}
		}
		return true, nil
	})
}

// Routing, per-follower accounting and nondefault-group execution use opaque
// exact commands in the caller's own load protocol. No SQL/billing is imported.
func loadIngressAndAccounting(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	groups := []string{"primary", "secondary"}
	var document topology.Document
	for _, group := range groups {
		created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: group, Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
		if err != nil {
			t.Fatal(err)
		}
		document = created.Document
	}
	for i, group := range groups {
		leader := c.Spec.Nodes[i].Name
		if err := Poll(ctx, func() (bool, error) {
			response, err := c.Request(ctx, c.Spec.Nodes[size-1].Name, "POST", "/api/groups/"+group+"/leader/"+leader, nil, 0)
			if err != nil || response.Status == 503 {
				return false, err
			}
			return true, requireStatus(response, nil, 200)
		}); err != nil {
			t.Fatal(err)
		}
		if err := Poll(ctx, func() (bool, error) {
			local, err := c.Local(ctx, leader, group)
			return err == nil && local.Found && local.Status.LeaderID == local.ReplicaID, err
		}); err != nil {
			t.Fatal(err)
		}
	}
	expected := map[string]map[string]string{"primary": {}, "secondary": {}}
	// Retain the source nine-request accounting and three-request settlement
	// workloads. Reusing IDs across independent groups must not suppress commands.
	for i := range 9 {
		key, id := fmt.Sprintf("key-%02d", i), fmt.Sprintf("command-%02d", i)
		value := "primary-" + key
		if err := c.Put(ctx, c.Spec.Nodes[i%size].Name, "primary", key, id, value); err != nil {
			t.Fatal("all ingress nodes must accept and route to actual load leader", i, err)
		}
		expected["primary"][key] = value
		if i < 3 {
			name := c.Spec.Nodes[(i*(size-1))%size].Name
			value := "secondary-" + key
			if err := c.Put(ctx, name, "secondary", key, id, value); err != nil {
				t.Fatal("nondefault group failed to account its own command", i, err)
			}
			expected["secondary"][key] = value
		}
	}
	for _, group := range groups {
		if err := exactOwnedValues(ctx, c, document, group, expected[group]); err != nil {
			t.Fatal(err)
		}
	}
	// Observer ingress may route writes, but it must not fabricate a local FSM or
	// silently proxy a read when it does not host the requested load group.
	for i := 3; i < size; i++ {
		name := c.Spec.Nodes[i].Name
		for _, group := range groups {
			local, err := c.Local(ctx, name, group)
			if err != nil || local.Found {
				t.Fatal("non-host ingress opened a load replica", name, group, local, err)
			}
			response, err := c.Request(ctx, name, "GET", "/api/load/"+group+"/keys/key-00", nil, 0)
			if err := requireStatus(response, err, 503); err != nil {
				t.Fatal("non-host read lost local consistency contract", err)
			}
		}
	}
	// No unexplained state growth without additional commands. Inspect each
	// replica independently throughout the quiet window, not a remote leader.
	stableAt := time.Now()
	for time.Since(stableAt) < time.Second {
		for _, group := range groups {
			for _, replica := range document.Group(group).Replicas {
				local, err := c.Local(ctx, replica.NodeName, group)
				if err != nil || !maps.Equal(local.Values, expected[group]) {
					t.Fatal("idle follower accounting changed", replica.NodeName, group, local.Values, err)
				}
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		timer.Stop()
	}
	// Every ingress retries exactly the same command; caller-machine durable
	// deduplication retains its result and rejects a changed fingerprint.
	for _, node := range c.Nodes() {
		for _, group := range groups {
			if err := c.Put(ctx, node.Spec.Name, group, "key-00", "command-00", expected[group]["key-00"]); err != nil {
				t.Fatal("exact retry changed result", err)
			}
			response, err := c.Request(ctx, node.Spec.Name, "PUT", "/api/load/"+group+"/keys/key-00", map[string]string{"request_id": "command-00", "value": "conflicting"}, 0)
			if err := requireStatus(response, err, 409); err != nil {
				t.Fatal("different command reused an acknowledged ID", err)
			}
			var refusal struct {
				Code string `json:"code"`
			}
			if err := response.Decode(&refusal); err != nil || refusal.Code != "version_conflict" {
				t.Fatal("command rejection lost its business classification", refusal, err)
			}
		}
	}
	// Replace both leaders and continue through a follower/non-host ingress. All
	// earlier acknowledged values must survive and remain in their own group.
	for i, group := range groups {
		leader := c.Spec.Nodes[1-i].Name
		if err := requestLeaderTransfer(ctx, c, "node3", group, leader); err != nil {
			t.Fatal(err)
		}
		if err := Poll(ctx, func() (bool, error) {
			local, err := c.Local(ctx, leader, group)
			return err == nil && local.Status.LeaderID == local.ReplicaID, err
		}); err != nil {
			t.Fatal(err)
		}
		if err := c.Put(ctx, c.Spec.Nodes[size-1].Name, group, "after-transfer", "after-transfer", group); err != nil {
			t.Fatal(err)
		}
		expected[group]["after-transfer"] = group
		if err := exactOwnedValues(ctx, c, document, group, expected[group]); err != nil {
			t.Fatal("leader transfer lost or misrouted acknowledged commands", err)
		}
	}
	t.Logf("METRIC nodes=%d ingress_nodes=%d exact_primary_commands=%d exact_secondary_commands=%d", size, size, len(expected["primary"]), len(expected["secondary"]))
}
