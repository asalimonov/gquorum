//go:build cluster

package cluster

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
)

func TestDuplicateNameIntruderExitsWithoutDisturbingHolderThreeNodes(t *testing.T) {
	duplicateNameIntruder(t, 3)
}
func TestDuplicateNameIntruderExitsWithoutDisturbingHolderFiveNodes(t *testing.T) {
	duplicateNameIntruder(t, 5)
}
func TestDuplicateNameIntruderExitsWithoutDisturbingHolderSevenNodes(t *testing.T) {
	duplicateNameIntruder(t, 7)
}

// Port the source duplicate-name process test without the application UI/gossip
// renderer. Read the stable typed, exact-process warning through the public
// parked registry from every founder, including followers.
func duplicateNameIntruder(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	before, err := c.Document(ctx, "node1", "active")
	if err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(ctx, "node1")
	if err != nil {
		t.Fatal(err)
	}
	holder := ""
	for _, node := range c.Spec.Nodes {
		if before.MemberID(node.Name) != status.MetaLeader {
			holder = node.Name
			break
		}
	}
	if holder == "" {
		t.Fatal("missing nonleader holder")
	}
	identity, err := c.Local(ctx, holder, topology.MetaGroupID)
	if err != nil || !identity.Found {
		t.Fatal(identity, err)
	}
	intruder, err := c.PrepareNameClaimingNode(ctx, NodeSpec{Name: "intruder", Type: c.Node(holder).Spec.Type}, holder)
	if err != nil {
		t.Fatal(err)
	}
	// Port-exit may race the listening-port readiness check, so Start can either
	// return its port acknowledgement or a container-exited error. The actual
	// container state and exit code are the authoritative assertion.
	startErr := intruder.Start(ctx)
	if err := Poll(ctx, func() (bool, error) {
		state, err := intruder.Container.State(ctx)
		if err != nil {
			return false, err
		}
		if state.Running {
			return false, nil
		}
		if state.ExitCode != 1 {
			return true, fmt.Errorf("duplicate-name process exited %d (start: %v)", state.ExitCode, startErr)
		}
		intruder.running.Store(false)
		return true, nil
	}); err != nil {
		t.Fatal("live name conflict must exit, not retry indefinitely", err)
	}
	logs, err := intruder.Logs(ctx)
	if err != nil || !strings.Contains(logs, "node name is held by another live process") {
		t.Fatal("exit is not an identity refusal", logs, err)
	}
	// Exact identity can be learned from the leader's retained refusal and matched
	// against the actual intruding executable's startup record after it exits.
	var refused meta.ParkedJoin
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Spec.Nodes {
			response, err := c.Request(ctx, node.Name, "GET", "/api/cluster/parked", nil, 0)
			if err := requireStatus(response, err, 200); err != nil {
				return false, err
			}
			var entries []meta.ParkedJoin
			if err := response.Decode(&entries); err != nil {
				return true, err
			}
			if len(entries) != 1 {
				return false, fmt.Errorf("unexpected refusal registry: %+v", entries)
			}
			entry := entries[0]
			if entry.Name != holder || entry.InstanceID == "" || entry.InstanceID == identity.Identity.InstanceID || entry.ClusterID != c.ID || entry.Type != c.Node(holder).Spec.Type || entry.Address != "intruder:9000" || entry.ReportedMemberID != 0 || entry.ReportedDocVersion != 0 || entry.RefusalKind != "name_conflict" || entry.Reason == "" || entry.Attempts < 1 || entry.FirstSeen.IsZero() || entry.LastSeen.Before(entry.FirstSeen) {
				return true, fmt.Errorf("refusal has wrong reported process identity: %+v", entry)
			}
			if !strings.Contains(logs, "instance_id="+entry.InstanceID) || !strings.Contains(logs, "node="+holder) {
				return true, fmt.Errorf("warning does not identify the actual intruder: %+v", entry)
			}
			if refused.InstanceID != "" && !reflect.DeepEqual(refused, entry) {
				return true, fmt.Errorf("followers read different leader-owned warnings: %+v / %+v", refused, entry)
			}
			refused = entry
		}
		return true, nil
	}); err != nil {
		t.Fatal("typed conflict warning must survive intruder exit and forward from followers", err)
	}
	// A passive conflict never becomes an Add affordance. The public mutation
	// must refuse that admitted name before staging or allocating any new ID.
	response, err := c.Request(ctx, holder, "POST", "/api/nodes", map[string]any{
		"node": joiningNode(c.Node(holder).Spec), "mutation": meta.MutationOptions{Actor: "cluster-test", Apply: true},
	}, before.Version)
	if err := requireStatus(response, err, 400); err != nil {
		t.Fatal("live-name warning must not authorize admission", err)
	}
	for _, node := range c.Spec.Nodes {
		after, err := c.Document(ctx, node.Name, "active")
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("refused intruder changed topology", after, err)
		}
		ready, err := c.Request(ctx, node.Name, "GET", "/health/ready", nil, 0)
		if err := requireStatus(ready, err, 200); err != nil {
			t.Fatal("healthy founder stopped serving", node.Name, err)
		}
		observed, err := c.Status(ctx, node.Name)
		if err != nil {
			t.Fatal(err)
		}
		for _, placed := range observed.Nodes {
			if placed.Node.Name == holder && placed.Address != holder+":9000" {
				t.Fatal("refused intruder poisoned holder route", placed)
			}
		}
	}
	retained, err := c.Local(ctx, holder, topology.MetaGroupID)
	if err != nil || retained.Identity != identity.Identity || !retained.Found || len(retained.Status.ConfState.Voters) != size {
		t.Fatal("healthy holder process or voter identity changed", retained, err)
	}
	// The passive warning does not authorize admission or allocate another ID.
	latest, err := c.Document(ctx, holder, "latest")
	if err != nil || !reflect.DeepEqual(before, latest) {
		t.Fatal("conflict allocated a membership identity", latest, err)
	}
}
