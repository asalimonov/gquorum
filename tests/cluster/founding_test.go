//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
)

func foundingSpec(size int) Spec {
	spec := matrixSpec(size)
	spec.Bootstrap, spec.GateFounders = true, true
	return spec
}

func TestFoundingFullTopologyThreeNodes(t *testing.T) {
	t.Parallel()
	foundingFullTopology(t, 3)
}
func TestFoundingDelayedMemberThreeNodes(t *testing.T) {
	t.Parallel()
	foundingDelayedMember(t, 3)
}
func TestFoundingTimeoutExitThreeNodes(t *testing.T) {
	t.Parallel()
	foundingTimeoutExit(t, 3)
}
func TestRetainedFoundingStateBypassesAbsentPeerGateThreeNodes(t *testing.T) {
	t.Parallel()
	retainedFoundingState(t, 3)
}

// Port the source full topology/history/observed-leadership assertions. Caller
// groups replace the SQL-backed source group, and all user node types vote in
// meta while the workers group uses its caller-defined eligibility rule.
func assertFoundedTopology(t *testing.T, ctx context.Context, c *Cluster) topology.Document {
	t.Helper()
	var founded topology.Document
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			if !node.running.Load() {
				continue
			}
			doc, err := c.Document(ctx, node.Spec.Name, "active")
			if err != nil {
				return false, err
			}
			if doc.Version < 3 || len(doc.Nodes) != len(c.Spec.Nodes) || doc.MetaGroup() == nil || len(doc.MetaGroup().Replicas) != len(c.Spec.Nodes) || len(doc.LoadGroups()) != 2 || doc.Group("demo") == nil || doc.Group("workers") == nil {
				return false, fmt.Errorf("incomplete founding document: %+v", doc)
			}
			eligibleWorkers := 0
			for _, n := range c.Spec.Nodes {
				if n.Type == "worker" {
					eligibleWorkers++
				}
			}
			if doc.Placement.MinReplicas != 2 || doc.Placement.MaxReplicas != 3 || len(doc.Group("demo").Replicas) != 3 || len(doc.Group("workers").Replicas) != eligibleWorkers || doc.Group("workers").VoterPolicy == nil || doc.Group("workers").VoterPolicy.TargetVoters != 2 {
				return true, fmt.Errorf("caller founding placement changed: %+v", doc)
			}
			for _, declared := range c.Spec.Nodes {
				got := doc.Node(declared.Name)
				if got == nil || got.Type != declared.Type || got.State != topology.NodeActive {
					return true, fmt.Errorf("founding lost node type/identity: %+v", got)
				}
			}
			for _, replica := range doc.Group("workers").Replicas {
				if doc.Node(replica.NodeName).Type != "worker" {
					return true, fmt.Errorf("ineligible worker host: %+v", replica)
				}
			}
			status, err := c.Status(ctx, node.Spec.Name)
			if err != nil {
				return false, err
			}
			if status.MetaLeaderID == 0 || !status.SessionLive || status.ActiveVersion != doc.Version {
				return false, fmt.Errorf("meta not activated: %+v", status)
			}
			for _, group := range []string{topology.MetaGroupID, "demo", "workers"} {
				found := false
				for _, observed := range status.Groups {
					if observed.GroupID == group && observed.LeaderID != 0 {
						found = true
					}
				}
				if !found {
					return false, fmt.Errorf("%s has no observed leader for %s", node.Spec.Name, group)
				}
			}
			response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/topology/history", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				return false, err
			}
			var history topology.HistoryResult
			if err := response.Decode(&history); err != nil {
				return true, err
			}
			if len(history.Documents) < 3 {
				return false, fmt.Errorf("missing founding history: %s", response.Data)
			}
			local, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
			if err != nil {
				return false, err
			}
			if !local.Found || local.Identity.MemberID != doc.MemberID(node.Spec.Name) || len(local.Status.ConfState.Voters) != len(c.Spec.Nodes) {
				return false, fmt.Errorf("native founding membership differs: %+v", local)
			}
			for _, group := range doc.LoadGroups() {
				for _, replica := range group.Replicas {
					if replica.NodeName != node.Spec.Name {
						continue
					}
					native, err := c.Local(ctx, node.Spec.Name, group.ID)
					if err != nil {
						return false, err
					}
					voters := len(doc.VotersOf(group.ID))
					isVoter := replica.Role == topology.ReplicaVoter
					if !native.Found || native.ReplicaID != replica.ReplicaID || len(native.Status.ConfState.Voters) != voters || len(native.Status.ConfState.Learners) != len(group.Replicas)-voters || slices.Contains(native.Status.ConfState.Voters, replica.ReplicaID) != isVoter || slices.Contains(native.Status.ConfState.Learners, replica.ReplicaID) == isVoter {
						return false, fmt.Errorf("native founding load membership differs: %+v", native)
					}
				}
			}
			founded = doc
		}
		return true, nil
	}); err != nil {
		t.Fatal("full founding topology", err)
	}
	return founded
}
func foundingFullTopology(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceClusterSpec(t, foundingSpec(size))
	doc := assertFoundedTopology(t, ctx, c)
	if err := c.Put(ctx, c.Spec.Nodes[size-1].Name, "demo", "founded", "founded", "committed"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, doc, "demo", map[string]string{"founded": "committed"}); err != nil {
		t.Fatal(err)
	}
}

func assertFoundingGate(t *testing.T, ctx context.Context, c *Cluster, missing string) map[string]string {
	t.Helper()
	instances := map[string]string{}
	// Hold the already-running majority for two seconds: this is an actual gate
	// assertion, not a single transient not-ready reply before Raft election.
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		for _, node := range c.Nodes() {
			if !node.running.Load() {
				continue
			}
			live, err := c.Request(ctx, node.Spec.Name, "GET", "/health/live", nil, 0)
			if err := requireStatus(live, err, 200); err != nil {
				t.Fatal("gated process liveness", err)
			}
			ready, err := c.Request(ctx, node.Spec.Name, "GET", "/health/ready", nil, 0)
			if err := requireStatus(ready, err, 503); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(ready.Data), "greenfield founding") || !strings.Contains(string(ready.Data), missing) {
				t.Fatal("gate does not name missing founder", string(ready.Data))
			}
			api, err := c.Request(ctx, node.Spec.Name, "GET", "/api/topology/active", nil, 0)
			if err := requireStatus(api, err, 503); err != nil {
				t.Fatal("application API bypassed founding gate", err)
			}
			local, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
			if err != nil {
				t.Fatal(err)
			}
			if previous := instances[node.Spec.Name]; previous != "" && previous != local.Identity.InstanceID {
				t.Fatal("waiting founder restarted")
			}
			instances[node.Spec.Name] = local.Identity.InstanceID
			load, err := c.Local(ctx, node.Spec.Name, "demo")
			if err != nil || load.Found {
				t.Fatal("caller load application bootstrapped before all founders", load, err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	for name := range instances {
		logs, err := c.Node(name).Logs(ctx)
		if err != nil || !strings.Contains(logs, "waiting for configured cluster members to activate") || !strings.Contains(logs, missing) {
			t.Fatal("missing founding diagnostics", name, logs, err)
		}
	}
	return instances
}
func foundingDelayedMember(t *testing.T, size int) {
	t.Helper()
	spec := foundingSpec(size)
	missing := spec.Nodes[size-1].Name
	spec.Deferred, spec.SkipReady = []string{missing}, true
	ctx, c := performanceClusterSpec(t, spec)
	instances := assertFoundingGate(t, ctx, c, missing)
	if err := c.Node(missing).Start(ctx); err != nil {
		t.Fatal(err)
	}
	// The harness skips originally deferred nodes in WaitReady; explicitly check
	// the late process too, before comparing all native memberships.
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			response, err := c.Request(ctx, node.Spec.Name, "GET", "/health/ready", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertFoundedTopology(t, ctx, c)
	for name, before := range instances {
		local, err := c.Local(ctx, name, topology.MetaGroupID)
		if err != nil || local.Identity.InstanceID != before {
			t.Fatal("late arrival required peer restart", name, local, err)
		}
	}
}
func foundingTimeoutExit(t *testing.T, size int) {
	t.Helper()
	spec := foundingSpec(size)
	missing := spec.Nodes[size-1].Name
	spec.Deferred, spec.SkipReady, spec.FoundingTimeout = []string{missing}, true, 8*time.Second
	ctx, c := performanceClusterSpec(t, spec)
	assertFoundingGate(t, ctx, c, missing)
	for _, node := range c.Nodes() {
		if !node.running.Load() {
			continue
		}
		if err := Poll(ctx, func() (bool, error) {
			state, err := node.Container.State(ctx)
			if err != nil {
				return false, err
			}
			if state.Running {
				return false, nil
			}
			if state.ExitCode != 1 {
				return true, fmt.Errorf("founding timeout exited with %d", state.ExitCode)
			}
			node.running.Store(false)
			return true, nil
		}); err != nil {
			t.Fatal("bounded founding timeout must exit", node.Spec.Name, err)
		}
		logs, err := node.Logs(ctx)
		if err != nil || !strings.Contains(logs, "greenfield founding timed out after 8s") || !strings.Contains(logs, missing) {
			t.Fatal("timeout lacks exact deadline and missing identity", logs, err)
		}
		_, err = c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/established.json"})
		if !errdefs.IsNotFound(err) {
			t.Fatal("timed-out founding must not persist established startup", node.Spec.Name, err)
		}
	}
	// Same-disk restart must retain the unsatisfied gate rather than treat its
	// already-created meta database as successful application initialization.
	if err := c.Node("node1").Start(ctx); err != nil {
		t.Fatal(err)
	}
	ready, err := c.Request(ctx, "node1", "GET", "/health/ready", nil, 0)
	if err := requireStatus(ready, err, 503); err != nil || !strings.Contains(string(ready.Data), "greenfield founding") {
		t.Fatal("timed-out startup bypassed gate on same-disk restart", ready, err)
	}
}
func retainedFoundingState(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceClusterSpec(t, foundingSpec(size))
	before := assertFoundedTopology(t, ctx, c)
	expected := map[string]string{"retained": "survives-absent-peer"}
	if err := c.Put(ctx, "node2", "demo", "retained", "retained", expected["retained"]); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, before, "demo", expected); err != nil {
		t.Fatal(err)
	}
	identities := map[string]topology.Identity{}
	for _, node := range c.Nodes() {
		local, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		if err != nil {
			t.Fatal(err)
		}
		identities[node.Spec.Name] = local.Identity
	}
	var stops sync.WaitGroup
	failures := make(chan error, size)
	for _, node := range c.Nodes() {
		stops.Go(func() { failures <- node.Kill(ctx) })
	}
	stops.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	// Keep all three data owners and a meta majority; one configured observer is
	// absent for larger sizes. At size three, the surviving data majority suffices.
	survivors := max(2, size/2+1)
	for _, node := range c.Nodes()[:survivors] {
		if err := node.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes()[:survivors] {
			response, err := c.Request(ctx, node.Spec.Name, "GET", "/health/ready", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("retained startup gated on absent peers", err)
	}
	after := assertFoundedTopology(t, ctx, c)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("retained boot changed topology", before, after)
	}
	if err := c.Put(ctx, c.Nodes()[survivors-1].Spec.Name, "demo", "retained", "retained", expected["retained"]); err != nil {
		t.Fatal("retained application ingress cannot retry committed command", err)
	}
	for _, node := range c.Nodes()[:survivors] {
		local, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
		old := identities[node.Spec.Name]
		if err != nil || local.Identity.MemberID != old.MemberID || local.Identity.InstanceID == old.InstanceID {
			t.Fatal("retained process lost pin or did not restart", local, err)
		}
		if replicaID(before, "demo", node.Spec.Name) != 0 {
			if err := c.WaitValue(ctx, node.Spec.Name, "demo", "retained", expected["retained"]); err != nil {
				t.Fatal("retained owner cannot read committed value", node.Spec.Name, err)
			}
		} else {
			response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/load/demo/keys/retained", nil, 0)
			if err := requireStatus(response, err, 503); err != nil {
				t.Fatal("observer must refuse a local load read", node.Spec.Name, err)
			}
		}
	}
}
