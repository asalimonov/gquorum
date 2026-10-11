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

	gquorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
	docker "github.com/moby/moby/client"
	"go.etcd.io/raft/v3/raftpb"
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
		{path.Join(root, walstore.SnapDir), true}, {"/data/meta.bolt", false}, {"/data/events.bolt", false},
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
	// The source current-voter restart chooses a nonleader. Transfer only meta
	// leadership; this changes neither the document nor the retained load store.
	if err := transferMeta(ctx, c, "node1", "node2", created.Document.MemberID("node2")); err != nil {
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
		response, err := c.Request(ctx, node.Spec.Name, "GET", "/api/cluster/parked", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal("current-voter restart warning query", err)
		}
		var parked []meta.ParkedJoin
		if err := response.Decode(&parked); err != nil || len(parked) != 0 {
			t.Fatal("healthy retained restart was recorded as awaiting admission", parked, err)
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

// A voted identity must never refound an empty WAL. Only an explicitly approved
// reseat may replace it. This ports the source's data-loss proof through public
// library APIs and inspects the recovering process, rather than peer proxies.
func TestParkedVotedReplicaReseatThreeNodes(t *testing.T) { parkedReplicaReseat(t, 3) }
func TestParkedVotedReplicaReseatFiveNodes(t *testing.T)  { parkedReplicaReseat(t, 5) }
func TestParkedVotedReplicaReseatSevenNodes(t *testing.T) { parkedReplicaReseat(t, 7) }

func parkedReplicaReseat(t *testing.T, size int) {
	t.Helper()
	spec := matrixSpec(size)
	spec.SnapshotThreshold = 100000
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"before-loss": "durable"}
	if err := c.Put(ctx, "node3", "work", "before-loss", "before-loss", "durable"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	before, err := c.Local(ctx, "node3", "work")
	if err != nil || before.Ledger == nil || before.Ledger.ReplicaID != before.ReplicaID || before.Parked != nil {
		t.Fatal("missing established boot ledger", before, err)
	}
	oldID := before.ReplicaID
	var largest uint64
	for _, r := range created.Document.Group("work").Replicas {
		largest = max(largest, r.ReplicaID)
	}
	// Mismatched fault identities must not destroy a live successor's store.
	response, err := c.Request(ctx, "node3", "POST", fmt.Sprintf("/internal/test/lose-store/work/%d", oldID+1000), nil, 0)
	if err := requireStatus(response, err, 409); err != nil {
		t.Fatal(err)
	}
	response, err = c.Request(ctx, "node3", "POST", fmt.Sprintf("/internal/test/lose-store/work/%d", oldID), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var lost struct {
		Identity  storage.Identity `json:"identity"`
		HardState raftpb.HardState `json:"hard_state"`
	}
	if err := response.Decode(&lost); err != nil {
		t.Fatal(err)
	}
	want := storage.Identity{NodeName: "node3", ClusterID: c.ID, GroupID: "work", ReplicaID: oldID}
	if lost.Identity != want || lost.HardState.Term == 0 || lost.HardState.Vote == 0 || lost.HardState.Commit == 0 {
		t.Fatal("lost replica never durably voted and committed", lost)
	}
	victim := c.Node("node3")
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var parked LocalObservation
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node3", "work")
		if err != nil {
			return false, err
		}
		parked = local
		return !local.Found && local.Parked != nil && local.Parked.Reason == gquorum.ParkStoreMissing && local.Parked.Identity == want, nil
	}); err != nil {
		t.Fatal("voted replica did not park after lost WAL", parked, err)
	}
	if parked.Ledger == nil || *parked.Ledger != *before.Ledger || parked.Identity.MemberID != before.Identity.MemberID || parked.Identity.InstanceID == before.Identity.InstanceID {
		t.Fatal("restart erased ledger or changed meta pin", before, parked)
	}
	response, err = c.Request(ctx, "node3", "GET", "/health/ready", nil, 0)
	if err := requireStatus(response, err, 503); err != nil {
		t.Fatal("parked voter incorrectly served", err)
	}
	// Healthy majority still commits, while Latest/Target/Active retain the old
	// identity. Waiting for a replacement is not permission to rewrite topology.
	if err := Poll(ctx, func() (bool, error) {
		status, err := groupStatus(ctx, c, "node1", "work")
		return err == nil && status.LeaderID != 0 && status.HealthKnown && len(status.LiveVoters) >= 2, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "during-loss", "during-loss", "available"); err != nil {
		t.Fatal(err)
	}
	expected["during-loss"] = "available"
	for _, view := range []string{"latest", "target", "active"} {
		doc, err := c.Document(ctx, "node1", view)
		if err != nil || doc.Version != created.Document.Version || replicaID(doc, "work", "node3") != oldID {
			t.Fatal("data loss silently reseated a voter", view, doc, err)
		}
	}
	repaired, err := c.Mutate(ctx, "node2", "POST", "/api/groups/work/replicas/node3/reseat", meta.MutationOptions{Actor: "operator", Comment: "replace lost voted WAL"})
	if err != nil {
		t.Fatal("explicit reseat failed", repaired, err)
	}
	freshID := replicaID(repaired.Document, "work", "node3")
	if repaired.ExecutionID == "" || freshID <= largest || repaired.Document.MemberID("node3") != before.Identity.MemberID {
		t.Fatal("reseat reused retired ID or changed meta identity", repaired)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, repaired.Document, "work", expected); err != nil {
		t.Fatal("reseated process did not recover exact committed state", err)
	}
	after, err := c.Local(ctx, "node3", "work")
	if err != nil || after.Parked != nil || !after.Found || after.ReplicaID != freshID || after.Ledger == nil || after.Ledger.ReplicaID != freshID {
		t.Fatal("fresh replica retained park or old ledger", after, err)
	}
	if err := c.Put(ctx, "node3", "work", "after-reseat", "after-reseat", "resumed"); err != nil {
		t.Fatal(err)
	}
	expected["after-reseat"] = "resumed"
	if err := exactOwnedValues(ctx, c, repaired.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	t.Logf("METRIC nodes=%d durable_vote=%d old_replica=%d new_replica=%d recovered_commands=%d", size, lost.HardState.Vote, oldID, freshID, len(expected))
}

func TestLostMetaLeaderAutomaticallyReadmitsFreshIdentityThreeNodes(t *testing.T) {
	lostMetaLeaderReadmission(t, 3)
}
func TestLostMetaLeaderAutomaticallyReadmitsFreshIdentityFiveNodes(t *testing.T) {
	lostMetaLeaderReadmission(t, 5)
}
func TestLostMetaLeaderAutomaticallyReadmitsFreshIdentitySevenNodes(t *testing.T) {
	lostMetaLeaderReadmission(t, 7)
}

func lostMetaLeaderReadmission(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	created, err := c.CreateGroup(ctx, "node2", meta.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"before-meta-loss": "retained"}
	if err := c.Put(ctx, "node1", "work", "before-meta-loss", "before-meta-loss", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", expected); err != nil {
		t.Fatal(err)
	}
	// Always lose a worker's current meta leadership, forcing a survivor election
	// while its replacement process attempts admission. No operator mutation runs.
	if err := transferMeta(ctx, c, "node2", "node3", created.Document.MemberID("node3")); err != nil {
		t.Fatal(err)
	}
	before, err := c.Local(ctx, "node3", topology.MetaGroupID)
	if err != nil || before.Status.LeaderID != before.Identity.MemberID {
		t.Fatal("loss victim is not actual meta leader", before, err)
	}
	oldMember := before.Identity.MemberID
	var largest uint64
	for _, replica := range created.Document.MetaGroup().Replicas {
		largest = max(largest, replica.ReplicaID)
	}
	var faultIdentity meta.Identity
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node3", "POST", fmt.Sprintf("/internal/test/lose-meta/%d", oldMember), nil, 0)
		if err != nil {
			return false, err
		}
		if response.Status == 503 {
			return false, nil
		}
		if err := requireStatus(response, nil, 200); err != nil {
			return true, err
		}
		return true, response.Decode(&faultIdentity)
	}); err != nil {
		t.Fatal(err)
	}
	if faultIdentity != before.Identity {
		t.Fatal("data-loss fault targeted another process", faultIdentity, before.Identity)
	}
	victim := c.Node("node3")
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var repaired topology.Document
	if err := Poll(ctx, func() (bool, error) {
		doc, err := c.Document(ctx, "node1", "active")
		if err != nil {
			return false, err
		}
		repaired = doc
		return doc.MemberID("node3") > largest && doc.Node("node3") != nil && doc.Node("node3").State == topology.NodeActive, nil
	}); err != nil {
		t.Fatal("same-name data-lost founder did not readmit", repaired, err)
	}
	freshMember := repaired.MemberID("node3")
	executionID := topology.MakeExecutionID(repaired.Version, 1)
	if err := c.WaitExecution(ctx, "node1", executionID); err != nil {
		t.Fatal(err)
	}
	execution, err := c.Execution(ctx, "node1", executionID)
	if err != nil || execution.TotalSteps < 4 || execution.State != topology.ExecutionSucceeded {
		t.Fatal("readmission omitted durable learner-first execution", execution, err)
	}
	if err := c.WaitActive(ctx, repaired.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := c.Local(ctx, "node3", topology.MetaGroupID)
	if err != nil || !after.Found || after.Identity.MemberID != freshMember || after.Identity.InstanceID == before.Identity.InstanceID || after.Status.LeaderID == 0 || !slices.Contains(after.Status.ConfState.Voters, freshMember) || slices.Contains(after.Status.ConfState.Voters, oldMember) {
		t.Fatal("recovering process reused old member or did not vote", after, err)
	}
	if replicaID(repaired, "work", "node3") <= replicaID(created.Document, "work", "node3") {
		t.Fatal("readmission retained old load identity", repaired)
	}
	if err := exactOwnedValues(ctx, c, repaired, "work", expected); err != nil {
		t.Fatal("automatic readmission lost caller data", err)
	}
	if err := c.Put(ctx, "node3", "work", "after-meta-loss", "after-meta-loss", "resumed"); err != nil {
		t.Fatal(err)
	}
	expected["after-meta-loss"] = "resumed"
	if err := exactOwnedValues(ctx, c, repaired, "work", expected); err != nil {
		t.Fatal(err)
	}
	// A second retained restart recovers the fresh pin without another admission.
	if err := victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	stable, err := c.Document(ctx, "node1", "latest")
	if err != nil || stable.Version != repaired.Version || stable.MemberID("node3") != freshMember {
		t.Fatal("retained fresh restart churned topology", stable, err)
	}
	if err := exactOwnedValues(ctx, c, repaired, "work", expected); err != nil {
		t.Fatal(err)
	}
	t.Logf("METRIC nodes=%d lost_meta_leader=%d readmitted_member=%d fresh_load_replica=%d execution_steps=%d recovered_commands=%d", size, oldMember, freshMember, replicaID(repaired, "work", "node3"), execution.TotalSteps, len(expected))
}
