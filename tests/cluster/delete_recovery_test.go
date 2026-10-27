//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func approveDeletion(ctx context.Context, t *testing.T, c *Cluster, node, group string, version uint64) gquorum.MutationResult {
	t.Helper()
	response, err := c.Request(ctx, node, "DELETE", "/api/groups/"+group, gquorum.MutationOptions{Actor: "deletion-test"}, version)
	if err = requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var result gquorum.MutationResult
	if err = response.Decode(&result); err != nil {
		t.Fatal(err)
	}
	result.OperationID = startApply(ctx, t, c, node, result.Document.Version)
	if err = c.WaitOperation(ctx, node, result.OperationID); err != nil {
		t.Fatal("logical deletion blocked", err)
	}
	return result
}

func waitDeletionCleanup(ctx context.Context, t *testing.T, c *Cluster, node string, id storage.Identity) {
	t.Helper()
	if err := Poll(ctx, func() (bool, error) {
		page, err := c.Progress(ctx, node, topology.ProgressQuery{GroupID: id.GroupID})
		debt, found := exactCleanup(page, id)
		if err != nil {
			return false, err
		}
		if !found || debt.StoppedAt == nil || debt.RetiredAt == nil || debt.CollectedAt == nil {
			return false, fmt.Errorf("cleanup found=%t progress=%+v", found, debt)
		}
		return true, nil
	}); err != nil {
		t.Fatal("exact physical cleanup did not complete", id, err)
	}
}

func TestFailureIsolation_DeletionWithoutLoadQuorum(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.Retention = time.Nanosecond
	ctx, c, doc := readProcessClusterSpec(t, "node1", spec)
	independent, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "independent"})
	if err != nil {
		t.Fatal(err)
	}
	blocked := map[string]LocalObservation{}
	for _, node := range c.Nodes() {
		name := node.Spec.Name
		if err = c.SetBarrier(ctx, name, "work", topology.StepStopReplica, true); err != nil {
			t.Fatal(err)
		}
		local, err := c.Local(ctx, name, "work")
		if err != nil {
			t.Fatal(err)
		}
		blocked[name] = local
		if _, err = c.SetReadFaults(ctx, name, "work", local.ReplicaID, "PUT", ReadFaults{Identity: local.Identity, DropRaft: true}); err != nil {
			t.Fatal(err)
		}
	}
	for name, prior := range blocked {
		if err := Poll(ctx, func() (bool, error) {
			local, err := c.Local(ctx, name, "work")
			return local.Reads.RaftDropped > prior.Reads.RaftDropped, err
		}); err != nil {
			t.Fatal(err)
		}
	}
	short, cancel := context.WithTimeout(ctx, time.Second)
	_, err = c.Read(short, "node1", "work", "key")
	cancel()
	if err == nil {
		t.Fatal("fault did not remove load quorum")
	}
	deleted := approveDeletion(ctx, t, c, "node2", "work", independent.Document.Version)
	if err = c.Put(ctx, "node2", "independent", "during-delete", "during-delete", "available"); err != nil {
		t.Fatal(err)
	}
	for name, prior := range blocked {
		id := storage.Identity{ClusterID: c.ID, GroupID: "work", NodeName: name, ReplicaID: replicaID(doc, "work", name)}
		page, err := c.Progress(ctx, "node2", topology.ProgressQuery{GroupID: "work"})
		debt, found := exactCleanup(page, id)
		if err != nil || !found || debt.StoppedAt != nil || debt.RetiredAt != nil || debt.CollectedAt != nil {
			t.Fatal("logical deletion fabricated cleanup", debt, err)
		}
		if _, err = c.SetReadFaults(ctx, name, "work", prior.ReplicaID, "DELETE", ReadFaults{Identity: prior.Identity}); err != nil {
			t.Fatal(err)
		}
		if err = c.SetBarrier(ctx, name, "work", topology.StepStopReplica, false); err != nil {
			t.Fatal(err)
		}
	}
	for name := range blocked {
		waitDeletionCleanup(ctx, t, c, "node2", storage.Identity{ClusterID: c.ID, GroupID: "work", NodeName: name, ReplicaID: replicaID(doc, "work", name)})
	}
	if deleted.Document.Group("work") != nil {
		t.Fatal("approved deletion retained desired group")
	}
}

func TestFailureIsolation_DeletionSnapshotCatchup(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.MetaFaultProxy = true
	spec.Retention = time.Nanosecond
	spec.MetaSnapshotThreshold = 8
	ctx, c, doc := readProcessClusterSpec(t, "node1", spec)
	if err := transferMeta(ctx, c, "node1", "node1", doc.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	old, err := c.Local(ctx, "node3", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	victim := c.Node("node3")
	volume := victim.Volume
	if err = victim.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	deleted := approveDeletion(ctx, t, c, "node1", "work", doc.Version)
	latest := deleted.Document
	// Each approved metadata label update advances the retained log well beyond
	// the fixture threshold while the follower's retained snapshot stays old.
	for i := range 24 {
		latest.Node("node1").Labels = map[string]string{"deletion-snapshot": fmt.Sprint(i)}
		staged, err := c.Stage(ctx, "node1", latest)
		if err != nil {
			t.Fatal(err)
		}
		op := startApply(ctx, t, c, "node1", staged.Document.Version)
		if err = c.WaitOperation(ctx, "node1", op); err != nil {
			t.Fatal(err)
		}
		latest = staged.Document
	}
	if err = victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if victim.Volume != volume {
		t.Fatal("snapshot catchup replaced retained volume")
	}
	if err := Poll(ctx, func() (bool, error) {
		meta, err := c.Local(ctx, "node3", topology.MetaGroupID)
		if err != nil {
			return false, err
		}
		observed, err := c.MetaFault(ctx, "node3", "GET", meta.Identity, false)
		if err != nil {
			return false, err
		}
		return observed.Identity == meta.Identity && meta.Identity.InstanceID != old.Identity.InstanceID &&
			observed.SnapshotsAccepted > 0 && observed.SnapshotIndex > old.Status.Applied && meta.Status.Applied >= observed.SnapshotIndex, nil
	}); err != nil {
		t.Fatal("return did not apply received metadata snapshot", err)
	}
	id := storage.Identity{ClusterID: c.ID, GroupID: "work", NodeName: "node3", ReplicaID: replicaID(doc, "work", "node3")}
	waitDeletionCleanup(ctx, t, c, "node1", id)
	runtime, err := c.runtime(ctx, "node3")
	if err != nil || runtime.Factories != 0 {
		t.Fatal("return transiently acquired retired load owner", runtime, err)
	}
	response, err := c.Request(ctx, "node3", "POST", "/internal/test/replica/start", load.ReplicaRef{GroupID: "work", ReplicaID: id.ReplicaID}, 0)
	var refusal struct {
		Code string `json:"code"`
	}
	if err != nil || response.Decode(&refusal) != nil || refusal.Code != "group_retired" {
		t.Fatal("retired ID did not refuse exact restart", response, err)
	}
	response, err = c.Request(ctx, "node1", "POST", "/api/groups", map[string]any{
		"group": gquorum.GroupSpec{ID: "work"}, "mutation": gquorum.MutationOptions{Actor: "reuse-check"},
	}, latest.Version)
	refusal.Code = ""
	if err != nil || response.Decode(&refusal) != nil || refusal.Code != "group_retired" {
		t.Fatal("deleted group ID became reusable after snapshot", response, err)
	}
}

type deletionReadmissionReport struct {
	NodeName    string               `json:"node_name"`
	MemberID    uint64               `json:"member_id"`
	OperationID topology.OperationID `json:"operation_id"`
	InstanceID  string               `json:"instance_id"`
	Held        bool                 `json:"held"`
	Hits        uint64               `json:"hits"`
}

func TestFailureIsolation_DeletionDuringReadmission(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.MetaFaultProxy = true
	spec.Retention = time.Nanosecond
	ctx, c, doc := readProcessClusterSpec(t, "node1", spec)
	if err := transferMeta(ctx, c, "node1", "node1", doc.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	old, err := c.Local(ctx, "node3", "work")
	if err != nil {
		t.Fatal(err)
	}
	victim := c.Node("node3")
	volume := victim.Volume
	if err = victim.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err = waitClusterHealth(ctx, c, "node3"); err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node1", "POST", "/api/nodes/node3/readmit", map[string]any{
		"actor": "deletion-test", "expected_member_id": doc.MemberID("node3")}, doc.Version)
	if err = requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var rotated gquorum.MutationResult
	if err = response.Decode(&rotated); err != nil {
		t.Fatal(err)
	}
	rotated.OperationID = startApply(ctx, t, c, "node1", rotated.Document.Version)
	control := deletionReadmissionReport{NodeName: "node3", MemberID: rotated.Document.MemberID("node3"), OperationID: rotated.OperationID}
	for _, name := range []string{"node1", "node2"} {
		response, err = c.Request(ctx, name, "PUT", "/internal/test/readmission-report", control, 0)
		if err = requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	if err = victim.StartJoining(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, name := range []string{"node1", "node2"} {
			response, err := c.Request(ctx, name, "GET", "/internal/test/readmission-report", nil, 0)
			if err = requireStatus(response, err, 200); err != nil {
				return false, err
			}
			var view deletionReadmissionReport
			if err = response.Decode(&view); err != nil {
				return false, err
			}
			if view.Hits > 0 && view.InstanceID != "" && view.InstanceID != old.Identity.InstanceID {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("readmission never reached partial receipt barrier", err)
	}
	deleted := approveDeletion(ctx, t, c, "node1", "work", rotated.Document.Version)
	id := storage.Identity{ClusterID: c.ID, GroupID: "work", NodeName: "node3", ReplicaID: old.ReplicaID}
	page, err := c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: "work"})
	debt, found := exactCleanup(page, id)
	if err != nil || !found || debt.StoppedAt != nil || debt.RetiredAt != nil || debt.CollectedAt != nil {
		t.Fatal("partial batch fabricated physical completion", debt, err)
	}
	for _, name := range []string{"node1", "node2"} {
		response, err = c.Request(ctx, name, "DELETE", "/internal/test/readmission-report", nil, 0)
		if err = requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
	}
	waitDeletionCleanup(ctx, t, c, "node1", id)
	if err = Poll(ctx, func() (bool, error) {
		op, err := c.Operation(ctx, "node1", rotated.OperationID)
		for _, block := range op.Blocks {
			if block.GroupID == topology.MetaGroupID {
				return block.State == topology.OperationSucceeded, err
			}
		}
		return false, err
	}); err != nil {
		t.Fatal("deletion blocked independent metadata readmission", err)
	}
	if victim.Volume != volume || deleted.Document.MemberID("node3") != rotated.Document.MemberID("node3") {
		t.Fatal("deletion changed replacement identity or volume")
	}
	returned, err := c.Local(ctx, "node3", "work")
	if err != nil || returned.Found {
		t.Fatal("readmitted host published retired group", returned, err)
	}
}
