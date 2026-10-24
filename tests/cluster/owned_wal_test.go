//go:build clustertest

package cluster

import (
	"fmt"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

func TestOwnedWALRuntimeSmoke(t *testing.T) {
	t.Parallel()
	t.Run("retained generations", func(t *testing.T) {
		t.Parallel()
		spec := matrixSpec(3)
		spec.NodeOwnedWAL = true
		spec.MetaFaultProxy = true
		ctx, c := performanceClusterSpec(t, spec)
		created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "owned", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}})
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]string{}
		for round := range 2 {
			key := fmt.Sprintf("round-%d", round)
			if err := Poll(ctx, func() (bool, error) {
				leader := ""
				for _, node := range c.Spec.Nodes {
					local, err := c.Local(ctx, node.Name, "owned")
					if err != nil {
						return false, err
					}
					if local.Found && local.Status.LeaderID == local.ReplicaID && local.Status.LeaderID != 0 {
						leader = node.Name
						break
					}
				}
				if leader == "" {
					return false, nil
				}
				// Retry the same request ID if an election changes between observation and
				// proposal; application deduplication preserves exactly one command.
				err := c.Put(ctx, leader, "owned", key, key, "retained")
				return err == nil, err
			}); err != nil {
				t.Fatal(err)
			}
			values[key] = "retained"
			if err := exactOwnedValues(ctx, c, created.Document, "owned", values); err != nil {
				t.Fatal(err)
			}
			for _, node := range c.Spec.Nodes {
				before, err := c.Local(ctx, node.Name, "owned")
				if err != nil || !before.Found || before.GroupIdentity == nil || before.StorageAvailable || before.WALControlAvailable || before.StorageIdentity != nil {
					t.Fatal("invalid owned observation", before, err)
				}
				if err := c.Snapshot(ctx, node.Name, "owned"); err != nil {
					t.Fatal(err)
				}
				response, err := c.Request(ctx, node.Name, "POST", "/internal/test/node/stop", nil, 0)
				if err := requireStatus(response, err, 200); err != nil {
					t.Fatal("embedded Node stop", err)
				}
				response, err = c.Request(ctx, node.Name, "GET", "/internal/test/node/status", nil, 0)
				if err := requireStatus(response, err, 200); err != nil {
					t.Fatal("management unavailable with Node stopped", err)
				}
				response, err = c.Request(ctx, node.Name, "GET", "/internal/test/local/owned", nil, 0)
				if err := requireStatus(response, err, 503); err != nil {
					t.Fatal("stopped replica remained available", err)
				}
				response, err = c.Request(ctx, node.Name, "POST", "/internal/test/node/start", nil, 0)
				if err := requireStatus(response, err, 200); err != nil {
					t.Fatal("embedded Node restart", err)
				}
				if err := c.WaitReady(ctx); err != nil {
					t.Fatal(err)
				}
				if err := exactOwnedValues(ctx, c, created.Document, "owned", values); err != nil {
					t.Fatal(err)
				}
				after, err := c.Local(ctx, node.Name, "owned")
				if err != nil || after.GroupIdentity == nil || *after.GroupIdentity != *before.GroupIdentity || after.Identity.InstanceID == before.Identity.InstanceID {
					t.Fatal("restart changed replica identity or reused incarnation", before, after, err)
				}
			}
		}
		latest, err := c.Document(ctx, "node1", "latest")
		if err != nil || latest.Version != created.Document.Version {
			t.Fatal("retained restart changed approved topology", latest, err)
		}
	})
	t.Run("readmission", func(t *testing.T) {
		t.Parallel()
		spec := matrixSpec(3)
		spec.NodeOwnedWAL = true
		spec.MetaFaultProxy = true
		// This scenario uses public identities, native membership and application
		// values; it has no physical-store/wrapper assumptions from borrowed fixtures.
		lostMetaLeaderReadmissionSpec(t, spec)
	})
}
