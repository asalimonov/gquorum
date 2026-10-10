//go:build cluster

package cluster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
	docker "github.com/moby/moby/client"
)

func TestCorrelatedLoadWALStallPreservesMetaAndRecoversThreeNodes(t *testing.T) {
	correlatedWALStall(t, 3)
}

func TestDurableOwnSnapshotAndWALRecoverTenCommandsThreeNodes(t *testing.T) { durableOwnRecovery(t, 3) }
func TestDurableOwnSnapshotAndWALRecoverTenCommandsFiveNodes(t *testing.T)  { durableOwnRecovery(t, 5) }
func TestDurableOwnSnapshotAndWALRecoverTenCommandsSevenNodes(t *testing.T) { durableOwnRecovery(t, 7) }

func ownDurableFiles(ctx context.Context, c *Cluster, node, group string) error {
	root := path.Join("/data", "load", group) // Caller-owned fixture mount and layout.
	for _, item := range []struct {
		name      string
		directory bool
	}{
		{root, true}, {path.Join(root, walstore.StateFile), false},
		{path.Join(root, walstore.IdentityFile), false}, {path.Join(root, walstore.WALDir), true},
		{path.Join(root, walstore.SnapDir), true}, {"/data/meta.bolt", false},
	} {
		result, err := c.docker.ContainerStatPath(ctx, c.Node(node).Container.GetContainerID(), docker.ContainerStatPathOptions{Path: item.name})
		if err != nil {
			return fmt.Errorf("own durable path %s: %w", item.name, err)
		}
		if result.Stat.Mode.IsDir() != item.directory || !item.directory && result.Stat.Size <= 0 {
			return fmt.Errorf("own durable path has wrong kind or empty contents: %s %+v", item.name, result.Stat)
		}
	}
	return nil
}

func durableOwnRecovery(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.SnapshotThreshold = 100000
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{}
	for i := range 10 {
		key := fmt.Sprintf("durable-%02d", i)
		if err := c.Put(ctx, c.Spec.Nodes[i%size].Name, "work", key, key, key); err != nil {
			t.Fatal(err)
		}
		expected[key] = key
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	if err := c.Snapshot(ctx, "node1", "work"); err != nil {
		t.Fatal(err)
	}
	before, err := c.Local(ctx, "node1", "work")
	if err != nil || !before.Found || before.Storage.SnapshotIndex == 0 || before.Storage.SnapshotBytes == 0 || before.Storage.LogBytes == 0 {
		t.Fatal("restart has no actual full snapshot and local WAL", before, err)
	}
	if err := ownDurableFiles(ctx, c, "node1", "work"); err != nil {
		t.Fatal(err)
	}
	victim := c.Node("node1")
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal("own snapshot recovery lost exact state", err)
	}
	after, err := c.Local(ctx, "node1", "work")
	if err != nil || after.Identity.MemberID != before.Identity.MemberID || after.ReplicaID != before.ReplicaID || after.Identity.InstanceID == before.Identity.InstanceID || after.Storage.SnapshotIndex < before.Storage.SnapshotIndex || after.Storage.SnapshotBytes == 0 || after.Storage.LogBytes == 0 {
		t.Fatal("restart discarded its own durable identity or files", before, after, err)
	}
	// Peer snapshots cannot be used to hide a failed local FSM restore. All ten
	// commands were compacted into this process's own full snapshot before kill.
	if after.ReceivedSnapshots != 0 {
		t.Fatal("own recovery was replaced by a peer snapshot", after.ReceivedSnapshots)
	}
	if err := ownDurableFiles(ctx, c, "node1", "work"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		doc, err := c.Document(ctx, node.Spec.Name, "latest")
		if err != nil || doc.Version != created.Document.Version || doc.MemberID("node1") != before.Identity.MemberID || replicaID(doc, "work", "node1") != before.ReplicaID {
			t.Fatal("retained restart churned topology or identity", node.Spec.Name, doc, err)
		}
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := groupStatus(ctx, c, "node1", "work")
		return err == nil && status.LeaderID != 0 && status.HealthKnown && len(status.LiveVoters) >= 2, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "post-restart", "post-restart", "post-restart"); err != nil {
		t.Fatal(err)
	}
	expected["post-restart"] = "post-restart"
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	t.Logf("METRIC nodes=%d recovered_own_commands=10 snapshot_index=%d snapshot_bytes=%d local_wal_bytes=%d new_process=true peer_snapshots=0 continued_commands=%d", size, after.Storage.SnapshotIndex, after.Storage.SnapshotBytes, after.Storage.LogBytes, len(expected))
}
func TestCorrelatedLoadWALStallPreservesMetaAndRecoversFiveNodes(t *testing.T) {
	correlatedWALStall(t, 5)
}
func TestCorrelatedLoadWALStallPreservesMetaAndRecoversSevenNodes(t *testing.T) {
	correlatedWALStall(t, 7)
}

func correlatedWALStall(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.SnapshotThreshold = 100000
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{}
	for i := range 3 {
		key := fmt.Sprintf("warm-%d", i)
		if err := c.Put(ctx, "node1", "work", key, key, key); err != nil {
			t.Fatal(err)
		}
		expected[key] = key
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	metaLocal, err := c.Local(ctx, "node1", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	expectedMeta := metaLocal.Status
	if err := Poll(ctx, func() (bool, error) {
		for _, n := range c.Nodes() {
			s, e := c.Local(ctx, n.Spec.Name, topology.MetaGroupID)
			if e != nil || s.Status.Term != expectedMeta.Term || s.Status.LeaderID != expectedMeta.LeaderID {
				return false, e
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	// The first request may remain uncertain across the outage. Give it a long
	// deadline, then cancel/join explicitly and retry its same durable command ID.
	c.http.Timeout = 60 * time.Second
	probesCtx, stopProbes := context.WithCancel(ctx)
	var probes sync.WaitGroup
	type outcome struct {
		response  Response
		repliedAt time.Time
		err       error
	}
	results := make([]outcome, 11)
	t.Cleanup(func() { stopProbes(); probes.Wait() })
	replicas := created.Document.Group("work").Replicas
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, r := range replicas {
			if err := c.WALFault(cleanup, r.NodeName, "work", r.ReplicaID, false, 0, true); err != nil {
				t.Error("release WAL fault", err)
			}
		}
	})
	baseline := map[string]WALCounters{}
	for _, r := range replicas {
		local, err := c.Local(ctx, r.NodeName, "work")
		if err != nil {
			t.Fatal(err)
		}
		baseline[r.NodeName] = local.WAL
		if err := c.WALFault(ctx, r.NodeName, "work", r.ReplicaID, true, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	pausedAt := time.Now()
	for i := range results {
		probes.Go(func() {
			delay := time.Duration(max(0, i-1)) * 1500 * time.Millisecond
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-probesCtx.Done():
				results[i].err = probesCtx.Err()
				return
			}
			key := fmt.Sprintf("stall-%02d", i)
			results[i].response, results[i].err = c.Request(probesCtx, c.Spec.Nodes[i%size].Name, "PUT", "/api/load/work/keys/"+key, map[string]string{"request_id": key, "value": key}, 0)
			results[i].repliedAt = time.Now()
		})
	}
	var staged meta.MutationResult
	observed := 0
	readySamples := 0
	var nextReady time.Time
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for time.Since(pausedAt) < 15*time.Second {
		for _, node := range c.Nodes() {
			native, err := c.Local(ctx, node.Spec.Name, topology.MetaGroupID)
			if err != nil || !native.Found || native.Status.LeaderID != expectedMeta.LeaderID || native.Status.Term != expectedMeta.Term {
				t.Fatal("load outage stalled or demoted meta", node.Spec.Name, native.Status, err)
			}
		}
		for _, r := range replicas {
			local, err := c.Local(ctx, r.NodeName, "work")
			if err != nil || !local.Found || !local.WAL.Blocked || !maps.Equal(local.Values, expected) {
				t.Fatal("stalled durability halted the group or applied unpersisted traffic", r.NodeName, local, err)
			}
		}
		if !time.Now().Before(nextReady) {
			for _, node := range c.Nodes() {
				response, err := c.Request(ctx, node.Spec.Name, "GET", "/health/ready", nil, 200)
				if err != nil {
					t.Fatal("load WAL state disabled independent serving/session readiness", node.Spec.Name, err)
				}
				var ready struct {
					Node          string `json:"node"`
					SessionLive   bool   `json:"session_live"`
					ActiveVersion uint64 `json:"active_version"`
				}
				if err := response.Decode(&ready); err != nil || ready.Node != node.Spec.Name || !ready.SessionLive || ready.ActiveVersion != created.Document.Version {
					t.Fatal("readiness changed identity, activation or topology during WAL stall", ready, err)
				}
				readySamples++
			}
			nextReady = time.Now().Add(time.Second)
		}
		if staged.Document.Version == 0 && time.Since(pausedAt) >= 2*time.Second {
			candidate := created.Document.Clone()
			candidate.ParentVersion = candidate.Version
			candidate.Node("node1").Labels["during.wal.stall"] = "committed"
			staged, err = c.Stage(ctx, "node3", candidate)
			if err != nil || staged.Document.Version != created.Document.Version+1 {
				t.Fatal("meta proposal failed during correlated load-store outage", staged, err)
			}
		}
		observed++
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if observed < 20 || readySamples < 10*size || staged.Document.Version == 0 {
		t.Fatal("empty/short outage observation", observed, staged)
	}
	// A blocked leader cannot send its unpersisted entries to followers. Idle
	// followers need not enter their writer, but at least one real writer must
	// retry the outage and every store must remain armed until release.
	blockedWriters := 0
	for _, r := range replicas {
		local, err := c.Local(ctx, r.NodeName, "work")
		if err != nil || !local.WAL.Blocked {
			t.Fatal("correlated fault disappeared before release", r.NodeName, local.WAL, err)
		}
		if local.WAL.Attempts > baseline[r.NodeName].Attempts && local.WAL.Failures > baseline[r.NodeName].Failures {
			blockedWriters++
		}
		t.Logf("stalled writer node=%s baseline=%+v observed=%+v", r.NodeName, baseline[r.NodeName], local.WAL)
	}
	if blockedWriters == 0 {
		t.Fatal("outage did not reach any actual durable writer")
	}
	releasedAt := time.Now()
	for _, r := range replicas {
		if err := c.WALFault(ctx, r.NodeName, "work", r.ReplicaID, false, 0, true); err != nil {
			t.Fatal(err)
		}
	}
	stopProbes()
	probes.Wait()
	refusals, uncertain := 0, 0
	for i, result := range results {
		if result.err != nil {
			if !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, context.DeadlineExceeded) {
				t.Fatal("unexpected probe transport failure", i, result.err)
			}
			uncertain++
			continue
		}
		if result.response.Status == 200 {
			if result.repliedAt.Before(releasedAt) {
				t.Fatal("correlated stalled replicas acknowledged an unpersisted command", i)
			}
			continue
		}
		if err := requireStatus(result.response, nil, 503); err != nil {
			t.Fatal(err)
		}
		var body struct {
			Code string `json:"code"`
		}
		if err := result.response.Decode(&body); err != nil || !slices.Contains([]string{"unavailable", "not_leader", "replica_not_ready", "overloaded"}, body.Code) {
			t.Fatal("storage unavailability became a business/fatal rejection", body, err)
		}
		refusals++
	}
	if refusals == 0 {
		t.Fatal("fault produced no typed availability responses")
	}
	// No restart/operator repair: removing the storage fault restores consensus.
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal("load did not recover automatically", err)
	}
	// Serving/session readiness intentionally has no WAL-health input. Wait
	// separately for real consensus to recover before requiring new writes.
	if err := Poll(ctx, func() (bool, error) {
		var leader, term uint64
		for _, r := range replicas {
			local, err := c.Local(ctx, r.NodeName, "work")
			if err != nil {
				return false, err
			}
			if local.Status.LeaderID == 0 || local.WAL.Active != 0 || local.WALStallNS != 0 {
				return false, nil
			}
			if leader == 0 {
				leader, term = local.Status.LeaderID, local.Status.Term
			}
			if local.Status.LeaderID != leader || local.Status.Term != term {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("load consensus did not recover automatically", err)
	}
	for i := range results {
		key := fmt.Sprintf("stall-%02d", i)
		if err := c.Put(ctx, c.Spec.Nodes[i%size].Name, "work", key, key, key); err != nil {
			t.Fatal("retry of uncertain command did not resume", i, err)
		}
		expected[key] = key
	}
	if err := exactOwnedValues(ctx, c, staged.Document, "work", expected); err != nil {
		t.Fatal("recovery lost/duplicated exact commands", err)
	}
	if _, err := c.Apply(ctx, "node3", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	victim := c.Node("node3")
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, staged.Document, "work", expected); err != nil {
		t.Fatal("recovering process lost full committed machine state", err)
	}
	for _, r := range replicas {
		local, err := c.Local(ctx, r.NodeName, "work")
		if err != nil || local.WAL.Blocked || local.WAL.Active > 1 || local.Storage.LogBytes <= 0 {
			t.Fatal("fault leaked durable calls or recovered without local WAL", r.NodeName, local, err)
		}
	}
	t.Logf("METRIC nodes=%d correlated_stall_ms=15000 meta_samples=%d ready_samples=%d blocked_writers=%d typed_refusals=%d uncertain_requests=%d exact_commands=%d", size, observed, readySamples, blockedWriters, refusals, uncertain, len(expected))
}
