//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"github.com/asalimonov/gquorum"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func threeNodes() Spec {
	return Spec{Nodes: []NodeSpec{{Name: "node1", Type: "worker"}, {Name: "node2", Type: "worker"}, {Name: "node3", Type: "observer"}}, Limits: (topology.Limits{}).WithDefaults()}
}
func TestPoolReusesIsolatedNetworkAndReplacesAllPersistentState(t *testing.T) {
	t.Parallel()
	// This dedicated one-unit pool makes reuse observable rather than relying on
	// scheduler order in the parallel suite pool.
	pool, err := NewPool(PoolOptions{Image: suitePool.opts.Image, Size: 1, CookParallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if err := pool.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	first, err := pool.Acquire(ctx, threeNodes())
	if err != nil {
		t.Fatal(err)
	}
	c := first.Cluster
	result, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "retire-me", Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if result.OperatorID == "" {
		t.Fatal("missing durable execution")
	}
	if err := c.Put(ctx, "node3", "retire-me", "key", "first-command", "value"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.WaitValue(ctx, node.Spec.Name, "retire-me", "key", "value"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Mutate(ctx, "node2", "DELETE", "/api/groups/retire-me", gquorum.MutationOptions{Approve: true}); err != nil {
		t.Fatal(err)
	}
	latest, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	retired, err := c.Request(ctx, "node1", "POST", "/api/groups", map[string]any{"group": gquorum.GroupSpec{ID: "retire-me"}}, latest.Version)
	if err := requireStatus(retired, err, 409); err != nil {
		t.Fatal("group ID was not retired", err)
	}
	// Simulate a Docker create whose acknowledgement was lost: neither handle
	// appears in Cluster.Nodes. Cleanup must discover the lease labels itself.
	orphanVolume, err := c.docker.VolumeCreate(ctx, docker.VolumeCreateOptions{Name: c.resourcePrefix + "-orphan", Labels: c.labels})
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: pool.opts.Image, Name: c.resourcePrefix + "-orphan", Labels: c.labels, Networks: []string{c.Network}, Mounts: testcontainers.Mounts(testcontainers.VolumeMount(orphanVolume.Volume.Name, "/data"))}})
	if err != nil {
		t.Fatal(err)
	}
	network, id, credential := c.Network, c.ID, c.Credential
	nodes := c.Nodes()
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := pool.Acquire(ctx, threeNodes())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if err := second.Release(ctx); err != nil {
			t.Error(err)
		}
	}()
	d := second.Cluster
	_, inspectErr := d.docker.ContainerInspect(ctx, orphan.GetContainerID(), docker.ContainerInspectOptions{})
	if !errdefs.IsNotFound(inspectErr) {
		t.Fatal("unacknowledged container was not removed", inspectErr)
	}
	_, inspectErr = d.docker.VolumeInspect(ctx, orphanVolume.Volume.Name, docker.VolumeInspectOptions{})
	if !errdefs.IsNotFound(inspectErr) {
		t.Fatal("unacknowledged volume was not removed", inspectErr)
	}
	if d.Network != network || d.ID == id || d.Credential == credential || second.generation <= first.generation {
		t.Fatal("deployment reuse/isolation invariant")
	}
	doc, err := d.Document(ctx, "node1", "active")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Version != 1 || len(doc.Groups) != 1 || doc.Group("retire-me") != nil {
		t.Fatal("persistent state leaked across leases", doc)
	}
	for i, node := range d.Nodes() {
		if node.Container.GetContainerID() == nodes[i].Container.GetContainerID() || node.Volume == nodes[i].Volume {
			t.Fatal("reused node storage")
		}
	}
	if _, err := d.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "retire-me"}); err != nil {
		t.Fatal("previous cluster group/identity state leaked", err)
	}
}
func TestPoolSimultaneousUnitsHavePrivateRoutingAndClusterState(t *testing.T) {
	t.Parallel()
	// Holding two suite leases could starve the second acquisition when the
	// other parallel cases occupy every remaining unit (or -parallel=1).
	pool, err := NewPool(PoolOptions{Image: suitePool.opts.Image, Size: 2, CookParallel: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if err := pool.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	first := acquireFrom(t, pool, threeNodes())
	second := acquireFrom(t, pool, threeNodes())
	a, b := first.Cluster, second.Cluster
	if a.Network == b.Network || a.ID == b.ID || a.Credential == b.Credential {
		t.Fatal("units share a security or network namespace")
	}
	for _, cluster := range []*Cluster{a, b} {
		if _, err := cluster.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "same-logical-group"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, cluster := range []*Cluster{a, b} {
		value := fmt.Sprintf("unit-%d", i)
		if err := cluster.Put(ctx, "node3", "same-logical-group", "key", "same-command-id", value); err != nil {
			t.Fatal(err)
		}
		for _, node := range cluster.Nodes() {
			if err := cluster.WaitValue(ctx, node.Spec.Name, "same-logical-group", "key", value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestPoolCanceledBootReturnsUnitOnlyAfterOwnedResourcesAreGone(t *testing.T) {
	t.Parallel()
	pool, err := NewPool(PoolOptions{Image: suitePool.opts.Image, Size: 1, CookParallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if err := pool.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	bootCtx, stopBoot := context.WithCancel(ctx)
	returned := make(chan error, 1)
	go func() {
		lease, err := pool.Acquire(bootCtx, threeNodes())
		if lease != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 35*time.Second)
			_ = lease.Release(cleanup)
			cancel()
		}
		returned <- err
	}()
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	filter := make(docker.Filters).Add("label", "gquorum.test.run="+pool.runID, "gquorum.test.unit=0")
	// Cancel after Docker has acknowledged persistent state, not merely after a
	// transient lifecycle state during initial network warming.
	if err := Poll(ctx, func() (bool, error) {
		volumes, err := cli.VolumeList(ctx, docker.VolumeListOptions{Filters: filter})
		return len(volumes.Items) > 0, err
	}); err != nil {
		t.Fatal(err)
	}
	stopBoot()
	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("canceled boot succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquisition did not honor cancellation")
	}
	if err := Poll(ctx, func() (bool, error) { return unitState(pool.units[0].state.Load()) == unitReady, nil }); err != nil {
		t.Fatal("canceled unit not safely reusable", err)
	}
	remaining, err := cli.VolumeList(ctx, docker.VolumeListOptions{Filters: filter})
	if err != nil || len(remaining.Items) != 0 {
		t.Fatal("canceled boot left persistent state", remaining, err)
	}
	lease, err := pool.Acquire(ctx, threeNodes())
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancelWait := context.WithCancel(ctx)
	cancelWait()
	if err := lease.Release(canceled); err == nil {
		t.Fatal("expired release should retain cleanup ownership")
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal("joining the same release", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal("release not idempotent", err)
	}
}

func TestPoolCanceledWarmupJoinsAndRemovesItsNetworks(t *testing.T) {
	t.Parallel()
	pool, err := NewPool(PoolOptions{Image: suitePool.opts.Image, Size: 2, CookParallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	defer func() {
		if err := pool.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	start, stopStart := context.WithCancel(ctx)
	returned := make(chan error, 1)
	go func() { returned <- pool.Start(start) }()
	if err := Poll(ctx, func() (bool, error) {
		pool.startMu.Lock()
		defer pool.startMu.Unlock()
		return len(pool.units) == 2, nil
	}); err != nil {
		t.Fatal(err)
	}
	stopStart()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("warmup ignored cancellation")
	}
	if err := pool.Close(ctx); err != nil {
		t.Fatal(err)
	}
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	networks, err := cli.NetworkList(ctx, docker.NetworkListOptions{Filters: make(docker.Filters).Add("label", "gquorum.test.run="+pool.runID)})
	if err != nil || len(networks.Items) != 0 {
		t.Fatal("interrupted warmup leaked a private network", networks, err)
	}
}
