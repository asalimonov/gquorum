//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
)

type snapshotProcessControl struct {
	Identity         topology.Identity `json:"identity"`
	Hold             string            `json:"hold"`
	HoldOffset       *uint64           `json:"hold_offset,omitempty"`
	ChunkDelayMillis int               `json:"chunk_delay_ms"`
	CorruptOffset    *uint64           `json:"corrupt_offset,omitempty"`
	DropAdmissionAck bool              `json:"drop_admission_ack"`
	DropChunkAckAt   uint64            `json:"drop_chunk_ack_at"`
	DropFinalAck     bool              `json:"drop_final_ack"`
	FailRestoreCount uint64            `json:"fail_restore_count"`
}

func TestSnapshotTransfer_LocalOversizeRefusalIsObservable(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.SnapshotTransfer = snapshot.Options{MaxSnapshotBytes: 8 << 10}
	ctx, c, _ := snapshotProcessSetup(t, spec)
	source, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	target, err := c.Local(ctx, "node3", "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "PUT", ReadFaults{Identity: target.Identity, DropAppend: true}); err != nil {
		t.Fatal(err)
	}
	// Ordinary application state can exceed transfer policy. Local snapshot
	// creation remains supported; the refusal belongs to sending it, not Apply.
	if err = c.Put(ctx, "node1", "work", "large", "oversize-state", strings.Repeat("x", 32<<10)); err != nil {
		t.Fatal(err)
	}
	if err = c.Snapshot(ctx, "node1", "work"); err != nil {
		t.Fatal("local snapshot was incorrectly rejected", err)
	}
	if err = c.Put(ctx, "node1", "work", "tail", "oversize-tail", "committed"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "DELETE", ReadFaults{Identity: target.Identity}); err != nil {
		t.Fatal(err)
	}
	if err = Poll(ctx, func() (bool, error) {
		view, err := snapshotProcessView(ctx, c, "node1", "work", source.ReplicaID)
		if err != nil {
			return false, err
		}
		for _, row := range view.Transfers.Items {
			if row.AdmissionRefused && row.ErrorReason == snapshot.ReasonSizeLimit && row.Key.To.Replica.ReplicaID == target.ReplicaID {
				if row.ErrorSize <= 8<<10 || row.ErrorLimit != 8<<10 || row.Refusals == 0 || row.LastRefusedAt.IsZero() || row.Key.DeliveryID != 0 || row.ReservedBytes != 0 || view.Transfers.SendBytes != 0 || view.Transfers.Outgoing != 0 {
					return false, fmt.Errorf("invalid local refusal: %+v", row)
				}
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("source refusal not observable", err)
	}
	if err = Poll(ctx, func() (bool, error) {
		response, err := c.Request(ctx, "node1", "GET", "/api/events?group=work&type="+string(events.RuntimeError), nil, 0)
		if err = requireStatus(response, err, 200); err != nil {
			return false, err
		}
		var page struct {
			Events []events.Event `json:"events"`
		}
		if err = response.Decode(&page); err != nil {
			return false, err
		}
		warning, refused := false, false
		for _, event := range page.Events {
			if event.Code == "snapshot_size_limit" {
				warning = warning || event.State == "local_snapshot_oversize"
				refused = refused || event.State == "send_refused"
			}
		}
		return warning && refused, nil
	}); err != nil {
		t.Fatal("allowance warning/refusal events missing", err)
	}
	if _, err = c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "traffic", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}}); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(ctx, "node1", "traffic", "live", "refusal-isolation", "progress"); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitValue(ctx, "node3", "traffic", "live", "progress"); err != nil {
		t.Fatal("refusal blocked unrelated group", err)
	}
}

type snapshotProcessObservation struct {
	Wire struct {
		Frames     uint64 `json:"frames"`
		ChunkPeak  uint64 `json:"chunk_peak"`
		WindowPeak uint64 `json:"window_peak"`
	} `json:"wire"`
	Identity  topology.Identity `json:"identity"`
	ReplicaID uint64            `json:"replica_id"`
	Active    uint64            `json:"active"`
	Hits      map[string]uint64 `json:"hits"`
	Offer     struct {
		Index, Term  uint64
		EncodedBytes uint64 `json:"encoded_bytes"`
		SHA256       string `json:"sha256"`
	} `json:"offer"`
	Application struct {
		Spec struct {
			Version      uint32
			Seed         uint64
			EncodedBytes uint64 `json:"encoded_bytes"`
		} `json:"spec"`
		Counter          uint64 `json:"counter"`
		ApplicationBytes uint64 `json:"application_bytes"`
		SHA256           string `json:"application_sha256"`
		Restores         uint64 `json:"restores"`
		AppliedIndex     uint64 `json:"applied_index"`
	} `json:"application"`
	Transfers snapshot.Page `json:"transfers"`
}

func snapshotProcessSetup(t *testing.T, spec Spec, policies ...*topology.VoterConfig) (context.Context, *Cluster, topology.Document) {
	t.Helper()
	spec.SnapshotThreshold = 1 << 20
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
	t.Cleanup(cancel)
	group := gquorum.GroupSpec{ID: "work", Placement: &topology.PlacementRule{MinReplicas: len(spec.Nodes), MaxReplicas: len(spec.Nodes)}}
	if len(policies) > 0 {
		group.VoterConfig = policies[0]
		group.Placement.MinReplicas = policies[0].TargetVoters
	}
	created, err := c.CreateGroup(ctx, "node1", group)
	if err != nil {
		t.Fatal(err)
	}
	forceReadLeader(ctx, t, c, created.Document, "node1")
	if err = c.Put(ctx, "node1", "work", "base", "snapshot-base", "retained"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err = c.WaitValue(ctx, node.Spec.Name, "work", "base", "retained"); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, c, created.Document
}
func snapshotProcessView(ctx context.Context, c *Cluster, node, group string, replica uint64) (snapshotProcessObservation, error) {
	response, err := c.Request(ctx, node, "GET", fmt.Sprintf("/internal/test/snapshot/%s/%d", group, replica), nil, 0)
	var result snapshotProcessObservation
	if err = requireStatus(response, err, 200); err != nil {
		return result, err
	}
	return result, response.Decode(&result)
}
func snapshotProcessFault(ctx context.Context, t *testing.T, c *Cluster, node, group string, replica uint64, control snapshotProcessControl) {
	t.Helper()
	response, err := c.Request(ctx, node, "PUT", fmt.Sprintf("/internal/test/snapshot/%s/%d", group, replica), control, 0)
	if err = requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
}
func snapshotProcessGenerate(ctx context.Context, t *testing.T, c *Cluster, node string, local LocalObservation, size uint64, seed uint64) {
	t.Helper()
	body := map[string]any{"identity": local.Identity, "request_id": fmt.Sprintf("synthetic-%d", seed),
		"spec": map[string]any{"version": 1, "seed": seed, "encoded_bytes": size}}
	response, err := c.Request(ctx, node, "POST", fmt.Sprintf("/internal/test/snapshot/work/%d/generate", local.ReplicaID), body, 0)
	if err = requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
}
func snapshotProcessCatchup(ctx context.Context, t *testing.T, c *Cluster, size uint64, control snapshotProcessControl) (LocalObservation, LocalObservation) {
	t.Helper()
	source, err := c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	target, err := c.Local(ctx, "node3", "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "PUT", ReadFaults{Identity: target.Identity, DropAppend: true}); err != nil {
		t.Fatal(err)
	}
	control.Identity = target.Identity
	snapshotProcessFault(ctx, t, c, "node3", "work", target.ReplicaID, control)
	snapshotProcessGenerate(ctx, t, c, "node1", source, size, 42)
	if err = c.Put(ctx, "node1", "work", "tail", "snapshot-tail", "committed"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "DELETE", ReadFaults{Identity: target.Identity}); err != nil {
		t.Fatal(err)
	}
	return source, target
}
func snapshotProcessWaitInstalled(ctx context.Context, t *testing.T, c *Cluster, source, target LocalObservation, size uint64) {
	t.Helper()
	if err := c.WaitValue(ctx, "node3", "work", "tail", "committed"); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		sent, err := snapshotProcessView(ctx, c, "node1", "work", source.ReplicaID)
		if err != nil {
			return false, err
		}
		received, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		if err != nil {
			return false, err
		}
		if sent.Offer.EncodedBytes == 0 || sent.Offer.EncodedBytes > size || sent.Offer.EncodedBytes+128<<10 < size || sent.Offer.SHA256 == "" {
			return false, nil
		}
		if received.Application.Restores == 0 || received.Application.Spec.Seed != 42 || received.Application.Counter < 1 {
			return false, nil
		}
		for _, row := range received.Transfers.Items {
			if row.Direction == "incoming" && row.ReceiverOutcome == snapshot.ReceiverInstalled && row.VerifiedBytes == sent.Offer.EncodedBytes {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("snapshot not durably installed with matching size/state", err)
	}
}
func TestSnapshotTransfer_LargeChunksTailAndHeartbeatIsolation(t *testing.T) {
	t.Parallel()
	ctx, c, doc := snapshotProcessSetup(t, matrixSpec(3))
	if _, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "traffic", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}}); err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	source, target := snapshotProcessCatchup(ctx, t, c, 32<<20, snapshotProcessControl{Hold: "chunk", HoldOffset: &zero})
	if err := Poll(ctx, func() (bool, error) {
		view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		return view.Active > 0, err
	}); err != nil {
		t.Fatal(err)
	}
	// The same destination continues ordinary Raft traffic over its independent
	// production connection while the work group's bulk stream is held.
	if err := c.Put(ctx, "node1", "traffic", "live", "bulk-isolation", "progress"); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitValue(ctx, "node3", "traffic", "live", "progress"); err != nil {
		t.Fatal(err)
	}
	snapshotProcessFault(ctx, t, c, "node3", "work", target.ReplicaID, snapshotProcessControl{Identity: target.Identity, ChunkDelayMillis: 25})
	snapshotProcessWaitInstalled(ctx, t, c, source, target, 32<<20)
	forceReadLeader(ctx, t, c, doc, "node3")
	if err := c.Put(ctx, "node3", "work", "after", "snapshot-new-leader", "accepted"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.WaitValue(ctx, node.Spec.Name, "work", "after", "accepted"); err != nil {
			t.Fatal(err)
		}
	}
}
func TestSnapshotTransfer_LostAcknowledgementsAndCorruption(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"admission", "chunk", "final", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, c, _ := snapshotProcessSetup(t, matrixSpec(3))
			control := snapshotProcessControl{}
			switch mode {
			case "admission":
				control.DropAdmissionAck = true
			case "chunk":
				control.DropChunkAckAt = 1 << 20
			case "final":
				control.DropFinalAck = true
			case "corrupt":
				offset := uint64(0)
				control.CorruptOffset = &offset
			}
			source, target := snapshotProcessCatchup(ctx, t, c, 4<<20, control)
			snapshotProcessWaitInstalled(ctx, t, c, source, target, 4<<20)
		})
	}
}
func TestSnapshotTransfer_SourceElectionAndReceiverIncarnation(t *testing.T) {
	t.Parallel()
	ctx, c, doc := snapshotProcessSetup(t, matrixSpec(3))
	zero := uint64(0)
	_, target := snapshotProcessCatchup(ctx, t, c, 4<<20, snapshotProcessControl{Hold: "chunk", HoldOffset: &zero})
	if err := Poll(ctx, func() (bool, error) {
		view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		return view.Active > 0, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Snapshot(ctx, "node2", "work"); err != nil {
		t.Fatal(err)
	}
	forceReadLeader(ctx, t, c, doc, "node2")
	snapshotProcessFault(ctx, t, c, "node3", "work", target.ReplicaID, snapshotProcessControl{Identity: target.Identity})
	if err := c.WaitValue(ctx, "node3", "work", "tail", "committed"); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node3", "work")
		return local.Found && local.Identity.InstanceID != target.Identity.InstanceID, err
	}); err != nil {
		t.Fatal(err)
	}
	reply, err := c.Request(ctx, "node3", "PUT", fmt.Sprintf("/internal/test/snapshot/work/%d", target.ReplicaID), snapshotProcessControl{Identity: target.Identity, Hold: "chunk"}, 0)
	if err != nil || reply.Status == 200 {
		t.Fatal("stale control reached restarted receiver", reply.Status, err)
	}
	if err = c.Put(ctx, "node2", "work", "restarted", "snapshot-incarnation", "new"); err != nil {
		t.Fatal(err)
	}
	if err = c.WaitValue(ctx, "node3", "work", "restarted", "new"); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotTransfer_CrashAtOwnedBoundaries(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before_finish", "before_persist", "after_persist", "after_restore"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			ctx, c, _ := snapshotProcessSetup(t, matrixSpec(3))
			_, target := snapshotProcessCatchup(ctx, t, c, 2<<20, snapshotProcessControl{Hold: phase})
			if err := Poll(ctx, func() (bool, error) {
				view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
				return view.Active > 0 && view.Hits[phase] > 0, err
			}); err != nil {
				t.Fatal(err)
			}
			if err := c.Node("node3").Kill(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.Node("node3").Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.WaitValue(ctx, "node3", "work", "tail", "committed"); err != nil {
				t.Fatal("full state/tail not recovered after crash", phase, err)
			}
			local, err := c.Local(ctx, "node3", "work")
			if err != nil || local.Identity.InstanceID == target.Identity.InstanceID {
				t.Fatal(local, err)
			}
			view, err := snapshotProcessView(ctx, c, "node3", "work", local.ReplicaID)
			if err != nil || view.Application.Spec.Seed != 42 || view.Application.Counter < 1 {
				t.Fatal("partial or wrong snapshot state survived", view, err)
			}
		})
	}
}

// Three voters retain a healthy majority while the fourth, stable learner
// supplies an actual replacement. Three participants cannot exercise both the
// progressing voter exemption and a guarded standby replacement in one group.
func TestSnapshotTransfer_RepairProtectionFourNodes(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(4)
	spec.Scheduling = true
	policy := topology.DefaultVoterConfig()
	// Preserve default 30-second lag/stall failure windows. Only standby wait and
	// the entry threshold are reduced so small generated state exercises them.
	policy.StandbyWindow = 3 * time.Second
	policy.FailureLagEntries, policy.StandbyLagEntries = 1, 0
	spec.SnapshotTransfer = snapshot.Options{TotalTimeout: 45 * time.Second, ProgressTimeout: 2 * time.Second}
	spec.Metrics = gquorum.MetricsOptions{SampleInterval: 100 * time.Millisecond, CollectInterval: 200 * time.Millisecond, CollectTimeout: 150 * time.Millisecond}
	ctx, c, doc := snapshotProcessSetup(t, spec, &policy)
	victim := replicaID(doc, "work", "node3")
	if err := Poll(ctx, func() (bool, error) {
		for _, node := range c.Nodes() {
			sample, err := clusterMetrics(ctx, c, node.Spec.Name)
			if err != nil {
				return false, err
			}
			if sample.IsCollectorLeader {
				for _, group := range sample.Groups {
					if group.GroupID == "work" && group.EligibleStandbys >= 1 {
						return true, nil
					}
				}
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("standby not eligible", err)
	}
	source, target := snapshotProcessCatchup(ctx, t, c, 32<<20, snapshotProcessControl{ChunkDelayMillis: 1200})
	started := time.Now()
	sawProtection := false
	if err := Poll(ctx, func() (bool, error) {
		current, err := c.Document(ctx, "node1", "target")
		if err != nil {
			return false, err
		}
		if replicaID(current, "work", "node3") != victim {
			return true, fmt.Errorf("progressing snapshot voter replaced early")
		}
		for _, node := range c.Nodes() {
			sample, err := clusterMetrics(ctx, c, node.Spec.Name)
			if err != nil {
				return false, err
			}
			if sample.IsCollectorLeader {
				for _, group := range sample.Groups {
					if group.GroupID == "work" {
						for _, replica := range group.Replicas {
							if replica.Sample.Identity.ReplicaID == victim {
								for _, reason := range replica.Evidence.Reasons {
									if reason == "snapshot_catchup" {
										sawProtection = true
									}
								}
							}
						}
					}
				}
			}
		}
		view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		return view.Application.Restores > 0 && view.Application.Counter >= 1, err
	}); err != nil {
		t.Fatal(err)
	}
	if !sawProtection || time.Since(started) < policy.FailureWindow {
		t.Fatal("scenario did not span the default failure window with positive protection")
	}
	snapshotProcessWaitInstalled(ctx, t, c, source, target, 32<<20)
	// A later catch-up episode makes one chunk of genuine progress per attempt,
	// then repeatedly times out. Those fresh bytes must not reset failure windows.
	target, err := c.Local(ctx, "node3", "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "PUT", ReadFaults{Identity: target.Identity, DropAppend: true}); err != nil {
		t.Fatal(err)
	}
	source, err = c.Local(ctx, "node1", "work")
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node1", "POST", fmt.Sprintf("/internal/test/snapshot/work/%d/generate", source.ReplicaID), map[string]any{
		"identity": source.Identity, "request_id": "failing-episode", "spec": map[string]any{"version": 1, "seed": 43, "encoded_bytes": 8 << 20}}, 0)
	if err = requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(ctx, "node1", "work", "failure-tail", "failure-tail", "pending"); err != nil {
		t.Fatal(err)
	}
	offset := uint64(1 << 20)
	snapshotProcessFault(ctx, t, c, "node3", "work", target.ReplicaID, snapshotProcessControl{Identity: target.Identity, Hold: "chunk", HoldOffset: &offset})
	if _, err = c.SetReadFaults(ctx, "node3", "work", target.ReplicaID, "DELETE", ReadFaults{Identity: target.Identity}); err != nil {
		t.Fatal(err)
	}
	if err = Poll(ctx, func() (bool, error) {
		sent, err := snapshotProcessView(ctx, c, "node1", "work", source.ReplicaID)
		if err != nil {
			return false, err
		}
		timeouts := 0
		for _, row := range sent.Transfers.Items {
			if row.ErrorReason == snapshot.ReasonProgressTimeout {
				timeouts++
			}
		}
		current, err := c.Document(ctx, "node1", "target")
		if err != nil {
			return false, err
		}
		group := current.Group("work")
		if group == nil {
			return false, nil
		}
		promoted, demoted := false, false
		for _, replica := range group.Replicas {
			if replica.NodeName == "node4" && replica.Role == topology.ReplicaVoter {
				promoted = true
			}
			if replica.ReplicaID == victim && replica.Role != topology.ReplicaVoter {
				demoted = true
			}
		}
		return timeouts >= 2 && promoted && demoted, nil
	}); err != nil {
		t.Fatal("repeated progressing failures never qualified guarded replacement", err)
	}
}
func TestSnapshotTransfer_ReceiverAutomaticRecoveryAfterRestoreFailures(t *testing.T) {
	t.Parallel()
	ctx, c, _ := snapshotProcessSetup(t, matrixSpec(3))
	_, target := snapshotProcessCatchup(ctx, t, c, 2<<20, snapshotProcessControl{FailRestoreCount: 3})
	if err := Poll(ctx, func() (bool, error) {
		view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		return view.Hits["restore_failed"] >= 3 && view.Application.Restores > 0 && view.Application.Counter >= 1, err
	}); err != nil {
		t.Fatal("receiver run recovery did not converge", err)
	}
	if err := c.WaitValue(ctx, "node3", "work", "tail", "committed"); err != nil {
		t.Fatal(err)
	}
	view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
	if err != nil || view.Identity != target.Identity {
		t.Fatal("engine recovery changed process identity", view.Identity, err)
	}
}

func TestSnapshotTransfer_MultiGroupCapacityRefillsWithoutLongBackoff(t *testing.T) {
	t.Parallel()
	ctx, c, _ := snapshotProcessSetup(t, matrixSpec(3))
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "queued", Placement: &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if err = Poll(ctx, func() (bool, error) {
		reply, err := c.Request(ctx, "node1", "POST", "/api/groups/queued/leader/node2", nil, 0)
		return reply.Status == 200, err
	}); err != nil {
		t.Fatal(err)
	}
	if err = Poll(ctx, func() (bool, error) {
		local, err := c.Local(ctx, "node2", "queued")
		return local.Status.LeaderID == replicaID(created.Document, "queued", "node2"), err
	}); err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	source, target := snapshotProcessCatchup(ctx, t, c, 2<<20, snapshotProcessControl{Hold: "chunk", HoldOffset: &zero})
	if err = Poll(ctx, func() (bool, error) {
		view, err := snapshotProcessView(ctx, c, "node3", "work", target.ReplicaID)
		return view.Active == 1 && view.Transfers.Incoming == 1, err
	}); err != nil {
		t.Fatal(err)
	}
	queuedTarget, err := c.Local(ctx, "node3", "queued")
	if err != nil {
		t.Fatal(err)
	}
	queuedSource, err := c.Local(ctx, "node2", "queued")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "queued", queuedTarget.ReplicaID, "PUT", ReadFaults{Identity: queuedTarget.Identity, DropAppend: true}); err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node2", "POST", fmt.Sprintf("/internal/test/snapshot/queued/%d/generate", queuedSource.ReplicaID), map[string]any{
		"identity": queuedSource.Identity, "request_id": "queued-synthetic", "spec": map[string]any{"version": 1, "seed": 42, "encoded_bytes": 2 << 20}}, 0)
	if err = requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(ctx, "node2", "queued", "tail", "queued-tail", "committed"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.SetReadFaults(ctx, "node3", "queued", queuedTarget.ReplicaID, "DELETE", ReadFaults{Identity: queuedTarget.Identity}); err != nil {
		t.Fatal(err)
	}
	if err = Poll(ctx, func() (bool, error) {
		view, err := snapshotProcessView(ctx, c, "node2", "queued", queuedSource.ReplicaID)
		if err != nil {
			return false, err
		}
		for _, row := range view.Transfers.Items {
			if row.ErrorReason == snapshot.ReasonCapacity {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("receiver capacity refusal not observed", err)
	}
	snapshotProcessFault(ctx, t, c, "node3", "work", target.ReplicaID, snapshotProcessControl{Identity: target.Identity})
	refill, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err = c.WaitValue(refill, "node3", "queued", "tail", "committed"); err != nil {
		t.Fatal("freed receive permit remained idle behind long backoff", err)
	}
	snapshotProcessWaitInstalled(ctx, t, c, source, target, 2<<20)
}
