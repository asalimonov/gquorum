//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum/load"
	"maps"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func permanentLossCluster(t *testing.T) (context.Context, *Cluster, topology.Document) {
	t.Helper()
	spec := matrixSpec(3)
	spec.NodeOwnedWAL = true
	spec.Scheduling = true
	spec.SnapshotThreshold = 100000
	spec.ProposalTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Second)
	t.Cleanup(cancel)
	c := acquire(t, spec).Cluster
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Put(ctx, "node1", "work", "key", "retained-request", "before"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "work", map[string]string{"key": "before"}); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.Snapshot(ctx, node.Spec.Name, "work"); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, c, created.Document
}
func stopLostPeers(ctx context.Context, t *testing.T, c *Cluster, survivor string) {
	t.Helper()
	for _, node := range c.Nodes() {
		if node.Spec.Name != survivor {
			if err := node.Kill(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func preparePermanentLoss(ctx context.Context, t *testing.T, c *Cluster, node string) RecoveryJob {
	t.Helper()
	req := recovery.Request{RecoveryID: "recover-" + node, Mode: recovery.Rebuild, Actor: "operator", Comment: "permanent majority loss"}
	if _, err := c.PrepareRecovery(ctx, node, "job", req); err != nil {
		t.Fatal(err)
	}
	job, err := c.WaitRecoveryJob(ctx, node, "job", "prepared")
	if err != nil {
		t.Fatal(err)
	}
	if job.Plan == nil || job.Plan.Digest == "" || !job.Plan.MayLoseAcknowledgedWrites {
		t.Fatal("missing loss/positions report", job)
	}
	return job
}
func confirmPermanentLoss(ctx context.Context, t *testing.T, c *Cluster, node string, job RecoveryJob) RecoveryJob {
	t.Helper()
	acceptance := recovery.Acceptance{PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	if _, err := c.RecoveryCommand(ctx, node, "job", "confirm", acceptance); err != nil {
		t.Fatal(err)
	}
	// Discarding an accepted response is safe: repeat the exact confirmation.
	if _, err := c.RecoveryCommand(ctx, node, "job", "confirm", acceptance); err != nil {
		t.Fatal(err)
	}
	complete, err := c.WaitRecoveryJob(ctx, node, "job", "complete")
	if err != nil {
		t.Fatal(err)
	}
	if complete.Maintenance || complete.Status == nil || complete.Status.Phase != recovery.Complete || len(complete.Status.History) < 5 {
		t.Fatal("incomplete remote recovery", complete)
	}
	return complete
}
func TestPermanentMajorityLossCompleteSurvivorAndReturningNodes(t *testing.T) {
	t.Parallel()
	ctx, c, source := permanentLossCluster(t)
	source = strictLossPolicy(ctx, t, c)
	before, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	oldAuthority, err := c.Authority(ctx, "node1", "work")
	if err != nil || len(oldAuthority.Actions) == 0 {
		t.Fatal("missing old action evidence", oldAuthority, err)
	}
	stopLostPeers(ctx, t, c, "node1")
	job := preparePermanentLoss(ctx, t, c, "node1")
	if job.Plan.Source.ClusterID != c.ID || len(job.Plan.Groups) != 2 {
		t.Fatal("wrong source inventory", job.Plan)
	}
	complete := confirmPermanentLoss(ctx, t, c, "node1", job)
	identity := complete.Status.Identity
	recovered, err := c.Document(ctx, "node1", "target")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{topology.MetaGroupID, "work"} {
		group := recovered.Group(id)
		if group.VoterConfig == nil || group.VoterConfig.TargetVoters != 3 || group.VoterConfig.DistinctVoterLabel != "az" {
			t.Fatal("desired voter policy was lowered", group)
		}
	}
	safeLossDeficitEdit(ctx, t, c)
	if identity.ClusterID == c.ID || identity.MemberID <= source.MemberID("node1") {
		t.Fatal("reused lineage", identity)
	}
	local, err := c.Local(ctx, "node1", "work")
	if err != nil || local.ReplicaID <= before.ReplicaID || !maps.Equal(local.Values, before.Values) || local.GroupIdentity == nil ||
		local.GroupIdentity.ClusterID != identity.ClusterID {
		t.Fatal("wrong recovered state", local, err)
	}
	delayed := oldAuthority.Actions[0]
	delayed.To = load.Peer{Node: local.Identity, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: local.ReplicaID}}
	response, replayErr := c.ReplayAction(ctx, "node1", local.Identity, delayed)
	if err := requireStatus(response, replayErr, 409); err != nil {
		t.Fatal("old-cluster authority crossed recovered lineage", err)
	}
	unchanged, err := c.Local(ctx, "node1", "work")
	if err != nil || !reflect.DeepEqual(local.Status.ConfState, unchanged.Status.ConfState) || !maps.Equal(local.Values, unchanged.Values) {
		t.Fatal("stale delivery changed B", unchanged, err)
	}
	birth := recoveredBirthIndex(ctx, t, c)
	if err := c.Put(ctx, "node1", "work", "key", "retained-request", "before"); err != nil {
		t.Fatal("lost dedup identity", err)
	}
	if err := c.Put(ctx, "node1", "work", "new", "post-recovery", "after"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"node2", "node3"} {
		returningNode(ctx, t, c, source, name, identity.ClusterID)
	}
	requireLaterRecoveryReceipt(ctx, t, c, birth)
	target, err := c.Document(ctx, "node1", "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, target, "work", map[string]string{"key": "before", "new": "after"}); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := node.Kill(ctx); err != nil {
			t.Fatal(err)
		}
		if err := node.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal("retained recovered cluster restart", err)
	}
	if err := exactOwnedValues(ctx, c, target, "work", map[string]string{"key": "before", "new": "after"}); err != nil {
		t.Fatal(err)
	}
}
func returningNode(ctx context.Context, t *testing.T, c *Cluster, source topology.Document, name, clusterID string) {
	t.Helper()
	if err := c.Node(name).Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, name, "GET", "/internal/test/node/status", nil, 0)
		if err := requireStatus(response, err, 200); err != nil {
			return false, err
		}
		var status gquorum.NodeStatus
		if err := response.Decode(&status); err != nil {
			return false, err
		}
		if status.Identity.ClusterID != c.ID {
			return true, fmt.Errorf("old participant silently joined B: %+v", status.Identity)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	latest, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	declaration := *source.Node(name)
	response, err := c.Request(ctx, "node1", "POST", "/api/nodes", map[string]any{"node": declaration,
		"mutation": gquorum.MutationOptions{Approve: true, Actor: "operator"}}, latest.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal("approve returning declaration", err)
	}
	var added gquorum.MutationResult
	if err := response.Decode(&added); err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{RecoveryID: "reset-" + name, Mode: recovery.ResetForJoin, ClusterID: clusterID,
		Actor: "operator", Comment: "join recovered lineage"}
	if _, err := c.PrepareRecovery(ctx, name, "reset", request); err != nil {
		t.Fatal(err)
	}
	job, err := c.WaitRecoveryJob(ctx, name, "reset", "prepared")
	if err != nil {
		t.Fatal(err)
	}
	acceptance := recovery.Acceptance{PlanDigest: job.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}
	if _, err := c.RecoveryCommand(ctx, name, "reset", "confirm", acceptance); err != nil {
		t.Fatal(err)
	}
	joined, err := c.WaitRecoveryJob(ctx, name, "reset", "complete")
	if err != nil {
		t.Fatal(err)
	}
	if joined.Status == nil || joined.Status.Phase != recovery.Joined || joined.Status.Identity.ClusterID != clusterID ||
		joined.Status.Identity.MemberID <= source.MemberID(name) {
		t.Fatal("reset did not admit fresh identity", joined)
	}
	if err := c.WaitOperation(ctx, "node1", added.OperationID); err != nil {
		t.Fatal(err)
	}
}

func TestPermanentMajorityLossOldMemberReachableBlocks(t *testing.T) {
	t.Parallel()
	ctx, c, _ := permanentLossCluster(t)
	if err := c.Node("node3").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{RecoveryID: "recover-alive", Mode: recovery.Rebuild, Actor: "operator", Comment: "must fence old peers"}
	if _, err := c.PrepareRecovery(ctx, "node1", "job", request); err != nil {
		t.Fatal(err)
	}
	var blocked RecoveryJob
	if err := Poll(ctx, func() (bool, error) {
		var err error
		blocked, err = c.RecoveryJob(ctx, "node1", "job")
		if err != nil {
			return false, err
		}
		return blocked.Error != "", nil
	}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, issue := range blocked.Plan.Issues {
		if issue.Code == "old_member_reachable" && issue.Blocking {
			found = true
		}
	}
	if !found {
		t.Fatal("living old member was not identified", blocked)
	}
	response, err := c.Request(ctx, "node1", "POST", "/internal/test/recovery/job/confirm", recovery.Acceptance{
		PlanDigest: blocked.Plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true}, 0)
	if err != nil || response.Status == 202 {
		t.Fatal("attestation bypassed live old-member refusal", response, err)
	}
	if err := c.Node("node2").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RecoveryCommand(ctx, "node1", "job", "retry", nil); err != nil {
		t.Fatal(err)
	}
	retried, err := c.WaitRecoveryJob(ctx, "node1", "job", "prepared")
	if err != nil {
		t.Fatal(err)
	}
	// A prepared phase can still be visible while its retry is joining. Require
	// the newly inspected report to shed the point-in-time blocking observation.
	if err := Poll(ctx, func() (bool, error) {
		var err error
		retried, err = c.RecoveryJob(ctx, "node1", "job")
		if err != nil {
			return false, err
		}
		for _, issue := range retried.Plan.Issues {
			if issue.Blocking {
				return false, nil
			}
		}
		return retried.Error == "", nil
	}); err != nil {
		t.Fatal(err)
	}
	if retried.Plan.Digest != blocked.Plan.Digest {
		t.Fatal("network observation changed accepted digest")
	}
	confirmPermanentLoss(ctx, t, c, "node1", retried)
}

func setLossAppendDrop(ctx context.Context, t *testing.T, c *Cluster, node string) LocalObservation {
	t.Helper()
	local, err := c.Local(ctx, node, "work")
	if err != nil || !local.Found {
		t.Fatal(local, err)
	}
	if _, err := c.SetReadFaults(ctx, node, "work", local.ReplicaID, "PUT", ReadFaults{Identity: local.Identity, DropAppend: true}); err != nil {
		t.Fatal(err)
	}
	// These exact incarnations are killed in the scenario. Lease teardown joins
	// their transports; a DELETE directed to a future lineage would be incorrect.
	return local
}
func TestPermanentMajorityLossLaggingCommittedSurvivor(t *testing.T) {
	t.Parallel()
	ctx, c, source := permanentLossCluster(t)
	forceReadLeader(ctx, t, c, source, "node1")
	donor := setLossAppendDrop(ctx, t, c, "node3")
	if err := c.Put(ctx, "node1", "work", "newer", "acknowledged-by-majority", "not-on-donor"); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		current, err := c.Local(ctx, "node3", "work")
		return err == nil && current.Reads.AppendDropped > 0 && current.Status.Commit == donor.Status.Commit &&
			maps.Equal(current.Values, donor.Values), err
	}); err != nil {
		t.Fatal(err)
	}
	stopLostPeers(ctx, t, c, "node3")
	job := preparePermanentLoss(ctx, t, c, "node3")
	if !job.Plan.MayLoseAcknowledgedWrites {
		t.Fatal("loss acceptance missing")
	}
	confirmPermanentLoss(ctx, t, c, "node3", job)
	local, err := c.Local(ctx, "node3", "work")
	if err != nil || !maps.Equal(local.Values, donor.Values) {
		t.Fatal("donor commit was fabricated", local, err)
	}
	if _, found := local.Values["newer"]; found {
		t.Fatal("unretained acknowledged write invented")
	}
	if err := c.Put(ctx, "node3", "work", "fresh", "after-lagging-recovery", "allowed"); err != nil {
		t.Fatal(err)
	}
}
func TestPermanentMajorityLossCommittedPrefix(t *testing.T) {
	t.Parallel()
	ctx, c, source := permanentLossCluster(t)
	forceReadLeader(ctx, t, c, source, "node1")
	before, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	setLossAppendDrop(ctx, t, c, "node2")
	setLossAppendDrop(ctx, t, c, "node3")
	if err := c.Put(ctx, "node1", "work", "tail", "never-committed", "must-not-apply"); err == nil {
		t.Fatal("tail unexpectedly acknowledged")
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, name := range []string{"node2", "node3"} {
			local, err := c.Local(ctx, name, "work")
			if err != nil || local.Reads.AppendDropped == 0 {
				return false, err
			}
		}
		leader, err := c.Local(ctx, "node1", "work")
		return err == nil && leader.Status.LeaderID == leader.ReplicaID && leader.Status.Commit == before.Status.Commit &&
			maps.Equal(leader.Values, before.Values), err
	}); err != nil {
		t.Fatal("tail setup did not preserve stable committed leader", err)
	}
	stopLostPeers(ctx, t, c, "node1")
	job := preparePermanentLoss(ctx, t, c, "node1")
	if err := RequireRecoveryTail(job, "work"); err != nil {
		t.Fatal(err)
	}
	confirmPermanentLoss(ctx, t, c, "node1", job)
	local, err := c.Local(ctx, "node1", "work")
	if err != nil || !maps.Equal(local.Values, before.Values) {
		t.Fatal("recovery applied beyond retained commit", local, err)
	}
	if _, found := local.Values["tail"]; found {
		t.Fatal("uncommitted tail entered application state")
	}
}

func strictLossPolicy(ctx context.Context, t *testing.T, c *Cluster) topology.Document {
	t.Helper()
	placementEdit(ctx, t, c, func(doc *topology.Document) {
		for i, name := range []string{"node1", "node2", "node3"} {
			labels := doc.Node(name).GetLabels()
			labels["az"] = fmt.Sprintf("zone-%d", i)
			if err := doc.Node(name).SetLabels(labels); err != nil {
				t.Fatal(err)
			}
		}
	})
	policy := topology.DefaultVoterConfig()
	policy.TargetVoters = 3
	policy.DistinctVoterLabel = "az"
	for _, group := range []string{topology.MetaGroupID, "work"} {
		if _, err := c.Mutate(ctx, "node1", "PUT", "/api/groups/"+group+"/voter-config", map[string]any{
			"voter_config": policy, "mutation": gquorum.MutationOptions{Approve: true, Actor: "operator"},
		}); err != nil {
			t.Fatal("strict source policy", err)
		}
	}
	source, err := c.Document(ctx, "node1", "target")
	if err != nil {
		t.Fatal(err)
	}
	return source
}
func safeLossDeficitEdit(ctx context.Context, t *testing.T, c *Cluster) {
	t.Helper()
	latest, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	labels := latest.Node("node1").GetLabels()
	labels["recovery_note"] = "safe"
	if err := latest.Node("node1").SetLabels(labels); err != nil {
		t.Fatal(err)
	}
	latest.ChangedBy = "operator"
	staged, err := c.Stage(ctx, "node1", latest)
	if err != nil {
		t.Fatal("deficit blocked unrelated safe edit", err)
	}
	if _, err := c.Approve(ctx, "node1", staged.Document.Version); err != nil {
		t.Fatal("deficit blocked unrelated approval", err)
	}
}

func recoveredBirthIndex(ctx context.Context, t *testing.T, c *Cluster) uint64 {
	t.Helper()
	view, err := c.Authority(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	var birth uint64
	for _, result := range view.Receipts {
		for _, receipt := range result.Receipts {
			if receipt.Action != nil && receipt.Action.Kind == topology.StepCreateFoundingGroup {
				birth = max(birth, receipt.AppliedIndex)
			}
		}
	}
	if birth == 0 {
		t.Fatal("missing real seeded creation receipt", view)
	}
	return birth
}
func requireLaterRecoveryReceipt(ctx context.Context, t *testing.T, c *Cluster, birth uint64) {
	t.Helper()
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.Authority(ctx, "node1", "work")
		for _, result := range view.Receipts {
			for _, receipt := range result.Receipts {
				if receipt.Action != nil && receipt.Action.Kind != topology.StepCreateFoundingGroup && receipt.AppliedIndex > birth {
					return true, err
				}
			}
		}
		return false, err
	}); err != nil {
		t.Fatal("later group effect did not retain a real receipt beyond seeded creation", err)
	}
}
