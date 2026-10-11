//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/topology"
	"github.com/containerd/errdefs"
	docker "github.com/moby/moby/client"
)

type processResources struct {
	Identity                          topology.Identity `json:"identity"`
	At                                time.Time         `json:"at"`
	HeapBytes, ResidentBytes, CPUNS   uint64
	Goroutines, OpenFiles, LoadGroups int
}

func (c *Cluster) resources(ctx context.Context, node string) (processResources, error) {
	response, err := c.Request(ctx, node, "GET", "/internal/test/resources", nil, 0)
	var result processResources
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	if err := response.Decode(&result); err != nil {
		return result, err
	}
	if result.Identity.NodeName != node || result.Identity.ClusterID != c.ID || result.Identity.MemberID == 0 || result.Identity.InstanceID == "" || result.At.IsZero() || result.ResidentBytes == 0 || result.HeapBytes == 0 || result.CPUNS == 0 || result.Goroutines <= 0 || result.OpenFiles <= 0 {
		return result, fmt.Errorf("missing or foreign process resource measurement: %+v", result)
	}
	return result, nil
}

func TestPerf_FifteenGroupResourcesThreeNodes(t *testing.T) { fifteenGroupResources(t, 3) }
func TestPerf_FifteenGroupResourcesFiveNodes(t *testing.T)  { fifteenGroupResources(t, 5) }
func TestPerf_FifteenGroupResourcesSevenNodes(t *testing.T) { fifteenGroupResources(t, 7) }

func fifteenGroupResources(t *testing.T, size int) {
	t.Helper()
	ctx, c := performanceCluster(t, size)
	baseline := map[string]processResources{}
	peaks := map[string]processResources{}
	for _, node := range c.Nodes() {
		observed, err := c.resources(ctx, node.Spec.Name)
		if err != nil || observed.LoadGroups != 0 {
			t.Fatal("invalid empty-runtime baseline", observed, err)
		}
		baseline[node.Spec.Name], peaks[node.Spec.Name] = observed, observed
	}
	sample := func() {
		t.Helper()
		for _, node := range c.Nodes() {
			observed, err := c.resources(ctx, node.Spec.Name)
			if err != nil {
				t.Fatal(err)
			}
			base, peak := baseline[node.Spec.Name], peaks[node.Spec.Name]
			if observed.Identity != base.Identity {
				t.Fatal("resource sample changed process", observed.Identity, base.Identity)
			}
			if observed.HeapBytes > 128<<20 || observed.ResidentBytes > 256<<20 || observed.Goroutines > base.Goroutines+15*32+64 || observed.OpenFiles > base.OpenFiles+15*8+32 {
				t.Fatal("resource bound exceeded", node.Spec.Name, observed, base)
			}
			peak.HeapBytes = max(peak.HeapBytes, observed.HeapBytes)
			peak.ResidentBytes = max(peak.ResidentBytes, observed.ResidentBytes)
			peak.Goroutines = max(peak.Goroutines, observed.Goroutines)
			peak.OpenFiles = max(peak.OpenFiles, observed.OpenFiles)
			peaks[node.Spec.Name] = peak
		}
	}
	var document topology.Document
	expected := map[string]map[string]string{}
	for i := range 15 {
		id := fmt.Sprintf("resource-%02d", i)
		created, err := c.CreateGroup(ctx, c.Spec.Nodes[i%size].Name, metaservice.GroupSpec{ID: id, Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker"}}})
		if err != nil {
			t.Fatal(id, err)
		}
		document = created.Document
		// Reuse command/key identities across groups to detect cross-group state.
		if err := c.Put(ctx, "node2", id, "shared-key", "shared-request", id); err != nil {
			t.Fatal(err)
		}
		expected[id] = map[string]string{"shared-key": id}
		sample()
	}
	if len(document.LoadGroups()) != 15 {
		t.Fatal("workload lacks fifteen actual groups", document)
	}
	for _, node := range c.Nodes() {
		observed, err := c.resources(ctx, node.Spec.Name)
		owned := 0
		for _, group := range document.LoadGroups() {
			if replicaID(document, group.ID, node.Spec.Name) > 0 {
				owned++
			}
		}
		if err != nil || observed.LoadGroups != owned {
			t.Fatal("resource workload has absent local engines", node.Spec.Name, observed, owned, err)
		}
	}
	// Retain ten real sampling rounds with writes across every live group.
	// The process CPU measurement covers the same interval as command traffic.
	starts := map[string]processResources{}
	for _, node := range c.Nodes() {
		observed, err := c.resources(ctx, node.Spec.Name)
		if err != nil {
			t.Fatal(err)
		}
		starts[node.Spec.Name] = observed
	}
	rounds := time.NewTicker(500 * time.Millisecond)
	defer rounds.Stop()
	for round := range 10 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-rounds.C:
		}
		for _, group := range document.LoadGroups() {
			key := fmt.Sprintf("round-%02d", round)
			if err := c.Put(ctx, c.Spec.Nodes[round%size].Name, group.ID, key, key, group.ID); err != nil {
				t.Fatal(group.ID, err)
			}
			expected[group.ID][key] = group.ID
		}
		sample()
	}
	for _, node := range c.Nodes() {
		end, err := c.resources(ctx, node.Spec.Name)
		if err != nil {
			t.Fatal(err)
		}
		start := starts[node.Spec.Name]
		elapsed := end.At.Sub(start.At)
		if elapsed < 5*time.Second || end.CPUNS < start.CPUNS {
			t.Fatal("CPU interval is empty or regressed", start, end)
		}
		cores := float64(end.CPUNS-start.CPUNS) / float64(elapsed)
		if math.IsNaN(cores) || cores > 4 {
			t.Fatal("average process CPU exceeds four cores", node.Spec.Name, cores)
		}
		peak := peaks[node.Spec.Name]
		t.Logf("METRIC node=%s groups=15 samples=25 heap_bytes=%d rss_bytes=%d goroutines=%d open_files=%d avg_cpu_cores=%.3f", node.Spec.Name, peak.HeapBytes, peak.ResidentBytes, peak.Goroutines, peak.OpenFiles, cores)
	}
	for _, group := range document.LoadGroups() {
		if len(group.Replicas) != 3 {
			t.Fatal("wrong typed placement", group)
		}
		if err := exactOwnedValues(ctx, c, document, group.ID, expected[group.ID]); err != nil {
			t.Fatal(err)
		}
	}
	// Remove all fifteen through one approved plan to stress joined teardown.
	removed := document.Clone()
	removed.Groups = slices.DeleteFunc(removed.Groups, func(g topology.Group) bool { return g.ID != topology.MetaGroupID })
	staged, err := c.Stage(ctx, "node2", removed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Apply(ctx, "node2", staged.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			observed, err := c.resources(ctx, node.Spec.Name)
			if err != nil {
				return false, err
			}
			base := baseline[node.Spec.Name]
			if observed.Identity != base.Identity {
				return false, fmt.Errorf("cleanup sample changed process")
			}
			if observed.LoadGroups != 0 || observed.Goroutines > base.Goroutines+96 || observed.OpenFiles > base.OpenFiles+32 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("teardown retained runtime resources", err)
	}
	for _, node := range c.Nodes() {
		doc, err := c.Document(ctx, node.Spec.Name, "active")
		if err != nil || len(doc.LoadGroups()) != 0 || len(doc.MetaGroup().Replicas) != size {
			t.Fatal("scale teardown changed meta or retained a group", doc, err)
		}
		for _, group := range document.LoadGroups() {
			if _, err := c.docker.ContainerStatPath(ctx, node.Container.GetContainerID(), docker.ContainerStatPathOptions{Path: "/data/load/" + group.ID}); !errdefs.IsNotFound(err) {
				t.Fatal("teardown retained usable store", node.Spec.Name, group.ID, err)
			}
		}
		observed, err := c.resources(ctx, node.Spec.Name)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("METRIC cleanup_node=%s load_groups=%d goroutines=%d baseline_goroutines=%d open_files=%d baseline_open_files=%d", node.Spec.Name, observed.LoadGroups, observed.Goroutines, baseline[node.Spec.Name].Goroutines, observed.OpenFiles, baseline[node.Spec.Name].OpenFiles)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal("meta readiness lost after scale teardown", err)
	}
}
