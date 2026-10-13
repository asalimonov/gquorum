//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func readProcessCluster(t *testing.T, leader string) (context.Context, *Cluster, topology.Document) {
	t.Helper()
	spec := matrixSpec(3)
	return readProcessClusterSpec(t, leader, spec)
}
func readProcessClusterSpec(t *testing.T, leader string, spec Spec) (context.Context, *Cluster, topology.Document) {
	t.Helper()
	spec.SnapshotThreshold = 1 << 20 // appends, rather than snapshot catch-up, are the controlled boundary
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	t.Cleanup(cancel)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if len(created.Document.Group("work").Replicas) != 3 {
		t.Fatal("read scenario requires three exact replicas", created.Document)
	}
	if leader != "" {
		forceReadLeader(t, ctx, c, created.Document, leader)
	} else {
		waitReadBarrier(t, ctx, c, "node1")
	}
	if err := c.Put(ctx, "node1", "work", "key", "before-read-fault", "before"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.WaitValue(ctx, node.Spec.Name, "work", "key", "before"); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, c, created.Document
}
func forceReadLeader(t *testing.T, ctx context.Context, c *Cluster, doc topology.Document, node string) {
	t.Helper()
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node2", "POST", "/api/groups/work/leader/"+node, nil, 0)
		if err != nil {
			return false, err
		}
		if response.Status == 200 {
			return true, nil
		}
		var refusal struct {
			Code fault.Code `json:"code"`
		}
		if response.Decode(&refusal) == nil && response.Status == 503 && slices.Contains([]fault.Code{fault.AuthorityPending, fault.Unavailable, fault.NotLeader, fault.ReplicaNotReady}, refusal.Code) {
			return false, requireStatus(response, nil, 200)
		}
		return true, requireStatus(response, nil, 200)
	}); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, host := range c.Nodes() {
			local, err := c.Local(ctx, host.Spec.Name, "work")
			if err != nil || local.Status.LeaderID != replicaID(doc, "work", node) {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	waitReadBarrier(t, ctx, c, node)
}
func waitReadBarrier(t *testing.T, ctx context.Context, c *Cluster, node string) {
	t.Helper()
	// A leader hint may precede a committed entry in its new term. Confirm a
	// fresh application barrier before the setup mutation or fault injection.
	if err := Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, node, "GET", "/api/load/work/read/key", nil, 0)
		if err != nil || response.Status != 200 && response.Status != 404 {
			return false, err
		}
		var value Value
		err = response.Decode(&value)
		return err == nil && value.Consistency == "linearizable", err
	}); err != nil {
		t.Fatal("new leader did not confirm its current-term read barrier", err)
	}
}
func armReadFault(t *testing.T, ctx context.Context, c *Cluster, node string, faults ReadFaults) LocalObservation {
	return armGroupReadFault(t, ctx, c, node, "work", faults)
}
func armGroupReadFault(t *testing.T, ctx context.Context, c *Cluster, node, group string, faults ReadFaults) LocalObservation {
	t.Helper()
	local, err := c.Local(ctx, node, group)
	if err != nil || !local.Found {
		t.Fatal(local, err)
	}
	faults.Identity = local.Identity
	if _, err := c.SetReadFaults(ctx, node, group, local.ReplicaID, "PUT", faults); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.SetReadFaults(cleanup, node, group, local.ReplicaID, "DELETE", ReadFaults{Identity: local.Identity}); err != nil {
			t.Error("read fault cleanup", err)
		}
	})
	return local
}
func clearReadFault(t *testing.T, ctx context.Context, c *Cluster, node string, local LocalObservation) {
	t.Helper()
	if _, err := c.SetReadFaults(ctx, node, "work", local.ReplicaID, "DELETE", ReadFaults{Identity: local.Identity}); err != nil {
		t.Fatal(err)
	}
}
func requireReadValue(t *testing.T, value Value, err error, node, expected string) {
	t.Helper()
	if err != nil || !value.Found || value.Value != expected || value.Node != node || value.Index == 0 || value.Consistency != "linearizable" {
		t.Fatal("consistent application read", value, err)
	}
}

func TestConsistentReadWaitsForFollowerApplyAfterConfirmationThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, _ := readProcessCluster(t, "node1")
	value, err := c.Read(ctx, "node2", "work", "key")
	requireReadValue(t, value, err, "node2", "before")
	before := armReadFault(t, ctx, c, "node2", ReadFaults{DropAppend: true})
	if err := c.Put(ctx, "node1", "work", "key", "after-follower-lag", "after"); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitValue(ctx, "node3", "work", "key", "after"); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		value Value
		err   error
	}
	readCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	joined := make(chan answer, 1)
	go func() { value, err := c.Read(readCtx, "node2", "work", "key"); joined <- answer{value, err} }()
	var confirmed LocalObservation
	if err := Poll(readCtx, func() (bool, error) {
		local, err := c.Local(readCtx, "node2", "work")
		confirmed = local
		return err == nil && local.Reads.ReadResponsesReceived > before.Reads.ReadResponsesReceived && local.Reads.ReadResponseIndex > local.Status.Applied && local.Values["key"] == "before", err
	}); err != nil {
		t.Fatal("did not observe quorum confirmation ahead of follower application", confirmed, err)
	}
	select {
	case answer := <-joined:
		t.Fatal("read finished before applying its confirmed index", answer)
	default:
	}
	clearReadFault(t, ctx, c, "node2", before)
	answerValue := <-joined
	requireReadValue(t, answerValue.value, answerValue.err, "node2", "after")
	if answerValue.value.Index <= value.Index {
		t.Fatal("query did not advance application state", value, answerValue.value)
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node2", "work")
		return err == nil && local.Status.Applied >= confirmed.Reads.ReadResponseIndex && local.Reads.QueryCompleted > before.Reads.QueryCompleted, err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestConsistentLearnerReadLostRepliesKeepRoundsBoundedThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, doc := readProcessCluster(t, "node1")
	learner := doc.Clone()
	policy := topology.DefaultVoterPolicy()
	policy.TargetVoters = 2
	learner.Group("work").VoterPolicy = &policy
	learner.Group("work").Placement.MinReplicas = 2
	for i := range learner.Group("work").Replicas {
		if learner.Group("work").Replicas[i].NodeName == "node3" {
			learner.Group("work").Replicas[i].Role = topology.ReplicaLearner
			learner.Group("work").Replicas[i].PinnedLearner = true
		}
	}
	staged, err := c.Stage(ctx, "node2", learner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Approve(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	value, err := c.Read(ctx, "node3", "work", "key")
	requireReadValue(t, value, err, "node3", "before")
	before := armReadFault(t, ctx, c, "node3", ReadFaults{DropReadResponse: true})
	short, stop := context.WithTimeout(ctx, 600*time.Millisecond)
	_, err = c.Read(short, "node3", "work", "key")
	stop()
	if err == nil {
		t.Fatal("lost read confirmation still completed a query")
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node3", "work")
		return err == nil && local.Reads.ReadResponsesDropped > before.Reads.ReadResponsesDropped, err
	}); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	var unexpected sync.Mutex
	var success bool
	for range 16 {
		workers.Go(func() {
			for range 32 {
				attempt, done := context.WithTimeout(ctx, 30*time.Millisecond)
				_, err := c.Read(attempt, "node3", "work", "key")
				done()
				if err == nil {
					unexpected.Lock()
					success = true
					unexpected.Unlock()
				}
			}
		})
	}
	workers.Wait()
	if success {
		t.Fatal("canceled read flood invented a confirmation")
	}
	local, err := c.Local(ctx, "node3", "work")
	if err != nil || local.Reads.ReadContextsSent != before.Reads.ReadContextsSent+1 {
		t.Fatal("cancellation grew unresolved upstream rounds", local.Reads, before.Reads, err)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			local, err := c.Local(ctx, node.Spec.Name, "work")
			if err != nil {
				return false, err
			}
			q := local.ReadQueue
			if local.ReadQueueStamp.State != metrics.Current || q.Limit == 0 {
				return false, nil
			}
			if q.InFlight > q.Limit || q.Queued > q.Limit || q.AwaitingQuorum > q.Limit || q.AwaitingApply > q.Limit || q.LocalRounds > 1 || q.RemoteRounds > q.Limit {
				return true, fmt.Errorf("unbounded read ownership on %s: %+v", node.Spec.Name, q)
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "work", "key", "after-canceled-reads", "after"); err != nil {
		t.Fatal(err)
	}
	clearReadFault(t, ctx, c, "node3", before)
	value, err = c.Read(ctx, "node3", "work", "key")
	requireReadValue(t, value, err, "node3", "after")
	local, err = c.Local(ctx, "node3", "work")
	if err != nil || local.Reads.ReadContextsSent <= before.Reads.ReadContextsSent+1 || !slices.Contains(local.Status.ConfState.Learners, local.ReplicaID) || slices.Contains(local.Status.ConfState.Voters, local.ReplicaID) {
		t.Fatal("fresh read reused old confirmation or promoted its learner", local, err)
	}
}

func TestConsistentReadRejectsIsolatedFormerLeaderThreeNodes(t *testing.T) {
	t.Parallel()
	ctx, c, doc := readProcessCluster(t, "")
	observed, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	oldLeader := observed.Status.LeaderID
	var oldNode string
	var healthy []string
	for _, replica := range doc.Group("work").Replicas {
		if replica.ReplicaID == oldLeader {
			oldNode = replica.NodeName
		} else {
			healthy = append(healthy, replica.NodeName)
		}
	}
	if oldNode == "" || len(healthy) != 2 {
		t.Fatal("no exact leader in approved native membership", observed)
	}
	before := armReadFault(t, ctx, c, oldNode, ReadFaults{DropRaft: true})
	if err := Poll(ctx, func() (bool, error) {
		var elected uint64
		for _, node := range healthy {
			local, err := c.Local(ctx, node, "work")
			if err != nil || local.Status.LeaderID == 0 || local.Status.LeaderID == oldLeader {
				return false, err
			}
			if elected != 0 && elected != local.Status.LeaderID {
				return false, nil
			}
			elected = local.Status.LeaderID
		}
		return true, nil
	}); err != nil {
		t.Fatal("healthy majority did not replace the isolated load leader", err)
	}
	waitReadBarrier(t, ctx, c, healthy[0])
	if err := c.Put(ctx, healthy[0], "work", "key", "majority-after-partition", "after"); err != nil {
		t.Fatal(err)
	}
	for _, node := range healthy {
		value, err := c.Read(ctx, node, "work", "key")
		requireReadValue(t, value, err, node, "after")
	}
	short, cancel := context.WithTimeout(ctx, time.Second)
	_, err = c.Read(short, oldNode, "work", "key")
	cancel()
	if err == nil {
		t.Fatal("isolated former leader served a fresh consistent read")
	}
	local, err := c.Local(ctx, oldNode, "work")
	if err != nil || local.Values["key"] != "before" || local.Reads.RaftDropped == before.Reads.RaftDropped {
		t.Fatal("fault did not isolate the exact old replica", local, err)
	}
	clearReadFault(t, ctx, c, oldNode, before)
	if err := c.WaitValue(ctx, oldNode, "work", "key", "after"); err != nil {
		t.Fatal(err)
	}
	value, err := c.Read(ctx, oldNode, "work", "key")
	requireReadValue(t, value, err, oldNode, "after")
}
