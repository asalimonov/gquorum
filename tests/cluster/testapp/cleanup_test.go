package main

import (
	"encoding/json"
	"github.com/asalimonov/gquorum"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestConcurrentAuthorizedCleanupRetriesAreIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("functional: real three-node effects and storage")
	}
	c := newRESTCluster(t)
	app := c.apps[2]
	h := app.clusterHooks
	key := effectKey{"demo", topology.StepCleanupReplicaState}
	barrier := &effectBarrier{release: make(chan struct{})}
	h.mu.Lock()
	h.barriers[key] = barrier
	h.mu.Unlock()
	var release sync.Once
	defer release.Do(func() { close(barrier.release) })
	base := c.document(t, 0, "latest")
	code, data := c.request(t, 0, "POST", "/api/nodes/node3/drain", gquorum.MutationOptions{Actor: "operator", Approve: true}, base.Version)
	if code != 202 {
		t.Fatalf("approve drain: %d %s", code, data)
	}
	var result gquorum.MutationResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	var approved topology.AuthorizedStep
	c.wait(t, "approved cleanup blocked before caller mutation", 10*time.Second, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		approved = barrier.last
		return barrier.hits > 0
	})
	if approved.GroupID != "demo" || approved.NodeName != "node3" || approved.Kind != topology.StepCleanupReplicaState || approved.OperatorID != result.OperatorID || approved.ReplicaID == 0 {
		t.Fatal("barrier captured wrong authorization", approved)
	}
	// Keep the dispatcher blocked while concurrent retries perform the exact same
	// approved local cleanup. No subsequent topology step can authorize a new ID.
	var workers sync.WaitGroup
	start := make(chan struct{})
	results := make(chan error, 32)
	for range 32 {
		workers.Go(func() {
			<-start
			_, err := app.node.ApplyLoadAction(c.ctx, load.ActionRequest{From: app.node.Identity(), To: load.Peer{Node: app.node.Identity(), Replica: load.ReplicaRef{GroupID: approved.GroupID, ReplicaID: approved.ReplicaID}}, Authorization: approved, LocalTarget: storage.Identity{NodeName: app.cfg.NodeName, ClusterID: app.cfg.ClusterID, GroupID: approved.GroupID, ReplicaID: approved.ReplicaID}})
			results <- err
		})
	}
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal("concurrent approved cleanup retry failed", err)
		}
	}
	if _, found := app.node.LoadGroup("demo"); found {
		t.Fatal("retired replica still published")
	}
	if probe, err := app.store.Probe(c.ctx, "demo"); err != nil || probe.Exists {
		t.Fatal("retired store retained after cleanup", probe, err)
	}
	release.Do(func() { close(barrier.release) })
	c.waitOperator(t, result.OperatorID)
	c.wait(t, "drain activates after idempotent primary callback", 5*time.Second, func() bool { return c.document(t, 0, "active").Version == result.Document.Version })
}
