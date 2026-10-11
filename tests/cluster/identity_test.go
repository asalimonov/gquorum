//go:build cluster

package cluster

import (
	"testing"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
)

func TestIdentityReadinessAndMinorityOverrideThreeNodes(t *testing.T) { identityServing(t, 3) }
func TestIdentityReadinessAndMinorityOverrideFiveNodes(t *testing.T)  { identityServing(t, 5) }
func TestIdentityReadinessAndMinorityOverrideSevenNodes(t *testing.T) { identityServing(t, 7) }

// Healthy identity/storage/readiness surfaces, majority loss, exact local
// override targeting and retained override recovery port both identity proofs.
func identityServing(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"committed": "retained"}
	if err := c.Put(ctx, "node1", "work", "committed", "committed", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		name := node.Spec.Name
		local, err := c.Local(ctx, name, topology.MetaGroupID)
		if err != nil || !local.Found || local.Identity.NodeName != name || local.Identity.MemberID != created.Document.MemberID(name) || local.Identity.InstanceID == "" {
			t.Fatal("identity surface is not answering process", local, err)
		}
		status, err := c.Status(ctx, name)
		if err != nil || !status.SessionLive || status.Lifecycle != "ready" || status.ActiveVersion != created.Document.Version {
			t.Fatal("healthy activation status missing", status, err)
		}
		response, err := c.Request(ctx, name, "GET", "/health/ready", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	if err := ownDurableFiles(ctx, c, "node1", "work"); err != nil {
		t.Fatal("caller mount does not contain actual durable files", err)
	}
	// Pin leadership to the surviving process before stopping just the minority.
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	for i := size - 1; i >= size-size/2; i-- {
		if err := c.Node(c.Spec.Nodes[i].Name).Kill(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node1", "GET", "/health/ready", nil, 0)
		return err == nil && response.Status == 200, err
	}); err != nil {
		t.Fatal("majority stopped serving", err)
	}
	// Remove the rest, leaving a one-of-N meta voter and one-of-three load voter.
	for i := size - size/2 - 1; i >= 1; i-- {
		if err := c.Node(c.Spec.Nodes[i].Name).Kill(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node1", "GET", "/health/ready", nil, 0)
		return err == nil && response.Status == 503, err
	}); err != nil {
		t.Fatal("minority retained session readiness", err)
	}
	own, err := c.Local(ctx, "node1", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"node", "cluster", "member", "process"} {
		wrong := own.Identity
		switch field {
		case "node":
			wrong.NodeName = "another-node"
		case "cluster":
			wrong.ClusterID = "another-cluster"
		case "member":
			wrong.MemberID++
		case "process":
			wrong.InstanceID = "another-process"
		}
		response, err := c.Request(ctx, "node1", "POST", "/internal/test/serve-override", map[string]any{"identity": wrong, "minimum": 1}, 0)
		if err := requireStatus(response, err, 409); err != nil {
			t.Fatal("override accepted mismatched "+field, err)
		}
	}
	response, err := c.Request(ctx, "node1", "POST", "/internal/test/serve-override", map[string]any{"identity": own.Identity, "minimum": 1}, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node1", "GET", "/health/ready", nil, 0)
		if err != nil || response.Status != 200 {
			return false, err
		}
		var ready struct {
			SessionLive bool `json:"session_live"`
			Override    int  `json:"serve_override"`
		}
		if err := response.Decode(&ready); err != nil {
			return true, err
		}
		return !ready.SessionLive && ready.Override == 1, nil
	}); err != nil {
		t.Fatal("local override did not relax only serving policy", err)
	}
	candidate := created.Document.Clone()
	candidate.Node("node1").Labels["minority-mutation"] = "forbidden"
	response, err = c.Request(ctx, "node1", "PUT", "/api/topology", candidate, candidate.Version)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("serving override bypassed meta consensus", err)
	}
	response, err = c.Request(ctx, "node1", "PUT", "/api/load/work/keys/minority", map[string]string{"request_id": "minority", "value": "forbidden"}, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("serving override bypassed load consensus", err)
	}
	local, err := c.Local(ctx, "node1", "work")
	if err != nil || len(local.Values) != 1 || local.Values["committed"] != "retained" {
		t.Fatal("minority applied an uncommitted command", local, err)
	}
	// Persisted override belongs to this pin, while process identity changes.
	if err := c.Node("node1").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node1").Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := c.Local(ctx, "node1", topology.MetaGroupID)
	if err != nil || restarted.ServeOverride != 1 || restarted.Identity.MemberID != own.Identity.MemberID || restarted.Identity.InstanceID == own.Identity.InstanceID {
		t.Fatal("retained override lost pin or process identity", restarted, err)
	}
	// Restoring the majority recovers activation and keeps acknowledged values.
	for _, node := range c.Nodes() {
		if node.Spec.Name != "node1" {
			if err := node.Start(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	own, err = c.Local(ctx, "node1", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node1", "POST", "/internal/test/serve-override", map[string]any{"identity": own.Identity, "minimum": 0}, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	t.Logf("METRIC nodes=%d minority=1 override_persisted=true mismatched_identities=4 quorum_bypass=false", size)
}
