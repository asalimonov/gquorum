//go:build clustertest && snapshotlarge

package cluster

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	docker "github.com/moby/moby/client"
)

func TestLargeSnapshot_EncodedQualification(t *testing.T) {
	t.Parallel()
	if flag.Lookup("test.parallel").Value.String() != "1" {
		t.Fatal("qualification requires -parallel=1 / CLUSTER_PARALLEL=1")
	}
	for _, size := range []uint64{256000000, 1000000000, 2000000000} {
		t.Run(fmt.Sprintf("encoded_%d", size), func(t *testing.T) {
			qualifyLargeSnapshot(t, size)
		})
	}
}
func qualifyLargeSnapshot(t *testing.T, size uint64) {
	t.Helper()
	spec := matrixSpec(3)
	spec.NodeOwnedWAL = true
	spec.SnapshotThreshold = 1 << 20
	spec.SnapshotTransfer = snapshot.Options{MaxSnapshotBytes: size, MaxReceiveBytes: size, MaxSendBytes: size}
	resolved, err := spec.SnapshotTransfer.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	spec.SnapshotTransfer = resolved
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Minute)
	defer cancel()
	info, err := c.docker.Info(ctx, docker.InfoOptions{})
	if err != nil {
		t.Fatal("resource preflight unqualified", err)
	}
	if info.Info.MemTotal <= 0 || uint64(info.Info.MemTotal) < size*12 {
		t.Fatalf("unqualified: Docker memory %d needs at least %d for resident/creation/transfer/restore buffers", info.Info.MemTotal, size*12)
	}
	before, err := c.resources(ctx, "node1")
	if err != nil {
		t.Fatal(err)
	}
	if before.AvailableDiskBytes < size*8 {
		t.Fatalf("unqualified: available storage %d needs %d", before.AvailableDiskBytes, size*8)
	}
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	forceReadLeader(ctx, t, c, created.Document, "node1")
	if err = c.Put(ctx, "node1", "work", "base", "large-base", "retained"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err = c.WaitValue(ctx, node.Spec.Name, "work", "base", "retained"); err != nil {
			t.Fatal(err)
		}
	}
	source, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	target, err := c.Local(ctx, "node3", "work")
	if err != nil {
		t.Fatal(err)
	}
	// Calibrate only the small unchanged envelope. The application byte length
	// is public fixture metadata and the actual encoded manifest is observed.
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "PUT", ReadFaults{Identity: target.Identity, DropAppend: true}); err != nil {
		t.Fatal(err)
	}
	snapshotProcessGenerate(ctx, t, c, "node1", source, 256<<10, 1)
	if err = c.Put(ctx, "node1", "work", "pilot", "pilot-tail", "calibrated"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "DELETE", ReadFaults{Identity: target.Identity}); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitValue(ctx, "node3", "work", "pilot", "calibrated"); err != nil {
		t.Fatal(err)
	}
	var framing uint64
	if err = Poll(ctx, func() (bool, error) {
		sent, err := snapshotProcessView(ctx, c, "node1", "work", source.ReplicaID)
		if err != nil {
			return false, err
		}
		raw := sent.Application.ApplicationBytes
		encoded := sent.Offer.EncodedBytes
		applicationWire := 4 * ((raw + 2) / 3)
		if raw == 0 || encoded <= applicationWire {
			return false, nil
		}
		framing = encoded - applicationWire
		return true, nil
	}); err != nil {
		t.Fatal("small envelope calibration failed", err)
	}
	if framing > 64<<10 {
		t.Fatal("unexpected envelope dimension", framing)
	}
	if _, err = c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "traffic", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "PUT", ReadFaults{Identity: target.Identity, DropAppend: true}); err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	snapshotProcessFault(ctx, t, c, "node3", "work", target.ReplicaID, snapshotProcessControl{Identity: target.Identity, Hold: "chunk", HoldOffset: &zero, DropChunkAckAt: 1 << 20})
	started := time.Now()
	response, err := c.Request(ctx, "node1", "POST", fmt.Sprintf("/internal/test/snapshot/work/%d/generate", source.ReplicaID), map[string]any{
		"identity": source.Identity, "request_id": fmt.Sprintf("large-%d", size), "spec": map[string]any{"version": 1, "seed": 42, "encoded_bytes": size, "framing_bytes": framing}}, 0)
	if err = requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(ctx, "node1", "work", "tail", "large-tail", "committed"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "DELETE", ReadFaults{Identity: target.Identity}); err != nil {
		t.Fatal(err)
	}
	if err = Poll(ctx, func() (bool, error) {
		view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		return view.Active > 0, err
	}); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(ctx, "node1", "traffic", "live", "large-isolation", "progress"); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitValue(ctx, "node3", "traffic", "live", "progress"); err != nil {
		t.Fatal(err)
	}
	snapshotProcessFault(ctx, t, c, "node3", "work", target.ReplicaID, snapshotProcessControl{Identity: target.Identity, DropChunkAckAt: 1 << 20})
	var peakHeap, peakRSS, peakDisk, peakReceive, peakSend uint64
	var transferDone, restoreDone time.Time
	if err = Poll(ctx, func() (bool, error) {
		for _, name := range []string{"node1", "node3"} {
			resources, err := largeSnapshotResources(ctx, c, name)
			if err != nil {
				return false, err
			}
			peakHeap = max(peakHeap, resources.HeapBytes)
			peakRSS = max(peakRSS, resources.ResidentBytes)
			peakDisk = max(peakDisk, resources.StoreDiskBytes)
		}
		view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		if err != nil {
			return false, err
		}
		peakReceive = max(peakReceive, view.Transfers.ReceiveBytes)
		sent, err := snapshotProcessView(ctx, c, "node1", "work", source.ReplicaID)
		if err != nil {
			return false, err
		}
		peakSend = max(peakSend, sent.Transfers.SendBytes)
		for _, row := range view.Transfers.Items {
			if row.VerifiedBytes == row.TotalBytes && row.TotalBytes > size/2 && transferDone.IsZero() {
				transferDone = time.Now()
			}
		}
		if view.Application.Spec.Seed == 42 && view.Application.Counter >= 1 && view.Application.Restores >= 2 {
			restoreDone = time.Now()
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	snapshotProcessWaitInstalled(ctx, t, c, source, target, size)
	observed, err := snapshotProcessView(ctx, c, "node1", "work", source.ReplicaID)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Offer.EncodedBytes > size || size-observed.Offer.EncodedBytes >= 4 {
		t.Fatal("qualification did not reach the requested encoded ceiling within base64 granularity", observed.Offer.EncodedBytes, size)
	}
	if err = c.Put(ctx, "node3", "work", "after", "large-after", "accepted"); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitValue(ctx, "node3", "work", "after", "accepted"); err != nil {
		t.Fatal(err)
	}
	if err = c.Node("node3").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.Node("node3").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitValue(ctx, "node3", "work", "after", "accepted"); err != nil {
		t.Fatal("large snapshot restart/tail replay failed", err)
	}
	t.Logf("requested=%d encoded=%d digest=%s peak_heap=%d peak_rss=%d store_disk_high_water=%d receive_reserved_peak=%d send_reserved_peak=%d baseline_heap=%d baseline_rss=%d transfer=%s restore_wait=%s total=%s chunk=%d window=%d wire_frames=%d observed_chunk_peak=%d observed_window_peak=%d",
		size, observed.Offer.EncodedBytes, observed.Offer.SHA256, peakHeap, peakRSS, peakDisk, peakReceive, peakSend, before.HeapBytes, before.ResidentBytes,
		transferDone.Sub(started), restoreDone.Sub(transferDone), time.Since(started), spec.SnapshotTransfer.ChunkBytes, spec.SnapshotTransfer.WindowChunks, observed.Wire.Frames, observed.Wire.ChunkPeak, observed.Wire.WindowPeak)
}

func largeSnapshotResources(ctx context.Context, c *Cluster, node string) (processResources, error) {
	response, err := c.Request(ctx, node, "GET", "/internal/test/resources?store_disk=1", nil, 0)
	var result processResources
	if err = requireStatus(response, err, 200); err != nil {
		return result, err
	}
	return result, response.Decode(&result)
}
