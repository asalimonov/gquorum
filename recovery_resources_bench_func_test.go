//go:build functional

package gquorum

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/asalimonov/gquorum/internal/walbackend"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
)

type recoveryResourceMachine struct {
	data  []byte
	count uint64
}

func (m *recoveryResourceMachine) Apply(_ context.Context, _ load.Command) (load.Result, error) {
	m.count++
	return load.Result{Code: "ok"}, nil
}
func (m *recoveryResourceMachine) Snapshot(context.Context) ([]byte, error) {
	data := bytes.Clone(m.data)
	if len(data) >= 8 {
		binary.LittleEndian.PutUint64(data, m.count)
	}
	return data, nil
}
func (m *recoveryResourceMachine) Restore(_ context.Context, data []byte) error {
	if len(data) > 0 {
		m.data = bytes.Clone(data)
		if len(data) >= 8 {
			m.count = binary.LittleEndian.Uint64(data)
		}
	}
	return nil
}

type recoveryResourceCase struct {
	name                                   string
	groups, snapshot, tailCount, tailBytes int
}

// Opt-in: compile once and select one case with -test.bench and -test.benchtime=1x.
// Large fixtures never run in make test or the default functional test suites.
func BenchmarkRecoveryResources(b *testing.B) {
	cases := []recoveryResourceCase{
		{name: "metadata-only"},
		{name: "snapshot-64MiB-tail", groups: 1, snapshot: 64 << 20, tailCount: 8, tailBytes: 64 << 10},
		{name: "sequential-three-groups", groups: 3, snapshot: 4 << 20, tailCount: 4, tailBytes: 256 << 10},
		{name: "near-1GiB", groups: 1, snapshot: 24 << 20, tailCount: 90, tailBytes: 8 << 20},
	}
	for _, shape := range cases {
		for _, unsafe := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/disable-fsync-%t", shape.name, unsafe), func(b *testing.B) {
				for range b.N {
					benchmarkRecoveryResources(b, shape, unsafe)
				}
			})
		}
	}
}
func benchmarkRecoveryResources(b *testing.B, shape recoveryResourceCase, unsafe bool) {
	b.StopTimer()
	root := b.TempDir()
	opts := authorityNodeOptions(b, root, "resource-source")
	opts.Node = opts.Bootstrap.Founders[0].Node
	opts.Meta.Timing.TickInterval = 10 * time.Millisecond
	loop := &loopbackTransport{}
	var factories, runs atomic.Uint64
	var machines sync.Map
	opts.Loads = LoadOptions{Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads"), DisableFSync: unsafe}},
		Transport: loop, Engine: load.EngineOptions{SnapshotThreshold: 100000, Timing: load.Timing{TickInterval: 10 * time.Millisecond}},
		Factory: func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
			factories.Add(1)
			machine := &recoveryResourceMachine{data: make([]byte, shape.snapshot)}
			machines.Store(spec.Identity.GroupID, machine)
			return load.Application{StateMachine: machine,
				Run: func(context.Context, *load.Group) error { runs.Add(1); return nil }}, nil
		}}
	source, err := NewNode(opts)
	if err != nil {
		b.Fatal(err)
	}
	loop.node = source
	b.Cleanup(func() { stopAuthorityNode(b, source) })
	ctx, cancel := context.WithTimeout(b.Context(), 5*time.Minute)
	defer cancel()
	if err := source.Start(ctx); err != nil {
		b.Fatal(err)
	}
	if err := source.WaitReady(ctx); err != nil {
		b.Fatal(err)
	}
	for i := range shape.groups {
		resourceSourceGroup(ctx, b, source, i, shape)
	}
	stopAuthorityNode(b, source)
	resourceCancelMeasurement(ctx, b, opts)
	runtime.GC()
	beforeFactories := factories.Load()
	facade, err := NewRecovery(opts)
	if err != nil {
		b.Fatal(err)
	}
	beforeBytes := resourceDiskBytes(b, root)
	b.ReportAllocs()
	b.StartTimer()
	started := time.Now()
	stopDisk := resourceDiskSampler(root, beforeBytes)
	defer stopDisk()
	plan, err := facade.Inspect(ctx, recovery.Request{Mode: recovery.Rebuild, Actor: "resource-benchmark", Comment: "measured recovery"})
	if err != nil {
		b.Fatal("Inspect", err)
	}
	if factories.Load() != beforeFactories {
		b.Fatal("inspection called application factory")
	}
	var selected uint64
	for _, group := range plan.Groups {
		selected += group.Bytes
		b.Logf("group=%s S=%d/%d C=%d/%d L=%d/%d encoded_bytes=%d", group.GroupID,
			group.Snapshot.Index, group.Snapshot.Term, group.Commit.Index, group.Commit.Term, group.LastLog.Index, group.LastLog.Term, group.Bytes)
	}
	b.ReportMetric(float64(selected), "selected-bytes")
	b.Logf("effective_resources=%+v groups=%d source_disk=%d", plan.Resources, shape.groups, beforeBytes)
	inspectionTime := time.Since(started)
	b.StopTimer()
	resourceBoundMeasurements(ctx, b, opts, plan)
	runtime.GC()
	b.StartTimer()
	resumed := time.Now()
	operation, err := facade.Approve(ctx, plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true, OldDeploymentFenced: true})
	if err != nil {
		b.Fatal("Approve", err)
	}
	status, err := facade.Run(ctx, operation.RecoveryID)
	if err != nil {
		b.Fatal("Run", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	opts.Meta.Network.Listener = listener
	opts.Meta.Network.AdvertiseAddress = listener.Addr().String()
	opts.Identity.InstanceID = "resource-destination"
	opts, err = RecoveryStartOptions(status, opts)
	if err != nil {
		b.Fatal(err)
	}
	destination, err := NewNode(opts)
	if err != nil {
		b.Fatal(err)
	}
	loop.node = destination
	b.Cleanup(func() { stopAuthorityNode(b, destination) })
	if err := destination.Start(ctx); err != nil {
		b.Fatal("Start", err)
	}
	if err := destination.WaitReady(ctx); err != nil {
		b.Fatal("WaitReady", err, destination.Status())
	}
	status, err = facade.Status(ctx, operation.RecoveryID)
	if err != nil || status.Phase != recovery.Complete {
		b.Fatal("not complete", status, err)
	}
	stopAuthorityNode(b, destination)
	for i := range shape.groups {
		value, found := machines.Load(fmt.Sprintf("resource-%d", i))
		if !found || value.(*recoveryResourceMachine).count != uint64(shape.tailCount) || len(value.(*recoveryResourceMachine).data) != shape.snapshot {
			b.Fatal("recovery did not preserve full snapshot and committed tail", i, value)
		}
	}
	b.ReportMetric(float64((inspectionTime + time.Since(resumed)).Nanoseconds()), "recovery-ns")
	b.ReportMetric(float64(resourceDiskBytes(b, root)-beforeBytes), "extra-disk-bytes")
	peak, peakErr := stopDisk()
	if peakErr != nil {
		b.Fatal(peakErr)
	}
	b.ReportMetric(float64(peak), "peak-extra-disk-bytes")
	b.Logf("factories=%d runs=%d history=%d", factories.Load(), runs.Load(), len(status.History))
	b.StopTimer()
}
func resourceSourceGroup(ctx context.Context, b *testing.B, node Node, i int, shape recoveryResourceCase) {
	b.Helper()
	spec := singleReplicaSpec(fmt.Sprintf("resource-%d", i), nil)
	latest, err := node.Cluster().Latest(ctx)
	if err != nil {
		b.Fatal(err)
	}
	result, err := node.Cluster().CreateGroup(ctx, spec, MutationOptions{ExpectedVersion: latest.Version, Actor: "resource-benchmark", Approve: true})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := node.Cluster().WaitOperation(ctx, result.OperationID); err != nil {
		b.Fatal(err)
	}
	group, found := node.LoadGroup(spec.ID)
	if !found {
		b.Fatal("missing source group")
	}
	if err := group.Snapshot(ctx); err != nil {
		b.Fatal(err)
	}
	data := make([]byte, shape.tailBytes)
	for j := range shape.tailCount {
		if _, err := group.Propose(ctx, load.Proposal{RequestID: fmt.Sprintf("tail-%d-%d", i, j), Data: data}); err != nil {
			b.Fatal(err)
		}
	}
}
func resourceDiskBytes(b *testing.B, root string) uint64 {
	b.Helper()
	size, err := resourceSampleDisk(root)
	if err != nil {
		b.Fatal(err)
	}
	return size
}
func resourceSampleDisk(root string) (uint64, error) {
	var size uint64
	seen := make(map[[2]uint64]bool)
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		key := [2]uint64{uint64(stat.Dev), uint64(stat.Ino)}
		if !seen[key] {
			seen[key] = true
			size += uint64(info.Size())
		}
		return nil
	})
	return size, err
}

// Sample logical file lengths every 10ms. Deduplicate inodes so an archive rename
// cannot count the same source twice while a directory walk is in progress.
func resourceDiskSampler(root string, baseline uint64) func() (uint64, error) {
	stop, done := make(chan struct{}), make(chan struct{})
	var peak uint64
	var failure error
	go func() {
		defer close(done)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			size, err := resourceSampleDisk(root)
			if err != nil {
				failure = err
				return
			}
			if size > baseline {
				peak = max(peak, size-baseline)
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	var once sync.Once
	return func() (uint64, error) { once.Do(func() { close(stop); <-done }); return peak, failure }
}

func resourceCancelMeasurement(ctx context.Context, b *testing.B, opts NodeOptions) {
	b.Helper()
	entered := make(chan struct{})
	facade, err := NewRecovery(opts, recovery.Options{Files: func(ctx context.Context, task recovery.FilesTask) error {
		if task.Action == recovery.FilesUndo {
			return nil
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}})
	if err != nil {
		b.Fatal(err)
	}
	plan, err := facade.Inspect(ctx, recovery.Request{Mode: recovery.ResetForJoin, ClusterID: "cancel-resource", Actor: "resource", Comment: "measure callback join"})
	if err != nil {
		b.Fatal(err)
	}
	operation, err := facade.Approve(ctx, plan.Request, recovery.Acceptance{PlanDigest: plan.Digest, AcknowledgeDataLoss: true})
	if err != nil {
		b.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := facade.Run(runCtx, operation.RecoveryID); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		b.Fatal(ctx.Err())
	}
	started := time.Now()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		b.Fatal(err)
	}
	if _, err := facade.Cancel(ctx, operation.RecoveryID, "resource", "cancel measured work"); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(time.Since(started).Nanoseconds()), "cancel-join-ns")
}
func resourceBoundMeasurements(ctx context.Context, b *testing.B, opts NodeOptions, plan recovery.Plan) {
	b.Helper()
	if len(plan.Groups) <= 1 {
		return
	}
	var group recovery.GroupRecovery
	for _, row := range plan.Groups {
		if row.GroupID != "_meta" {
			group = row
			break
		}
	}
	limit := plan.Resources
	limit.MaxGroupBytes = group.Bytes - 1
	handle, err := NewRecovery(opts, recovery.Options{Resources: limit})
	if err != nil {
		b.Fatal(err)
	}
	refusal, inspectErr := handle.Inspect(ctx, plan.Request)
	err = inspectErr
	if err == nil {
		b.Fatal("just-over-limit group was accepted")
	}
	b.Logf("group_budget_refusal: selected=%d bound=%d error=%v issues=%+v", group.Bytes, limit.MaxGroupBytes, err, refusal.Issues)
	frame := resourceLargestFrame(b, opts.Loads.Storage.WAL.RootDir, group.GroupID)
	limit = plan.Resources
	limit.MaxRecordBytes = frame - 1
	handle, err = NewRecovery(opts, recovery.Options{Resources: limit})
	if err != nil {
		b.Fatal(err)
	}
	refused, err := handle.Inspect(ctx, plan.Request)
	if err == nil {
		b.Fatal("one-byte-over WAL frame was accepted")
	}
	b.ReportMetric(float64(frame), "largest-frame-bytes")
	b.Logf("record_budget_refusal: actual=%d bound=%d error=%v issues=%+v", frame, limit.MaxRecordBytes, err, refused.Issues)
	statuses, listErr := handle.List(ctx)
	if listErr != nil {
		b.Fatal(listErr)
	}
	for _, status := range statuses {
		if status.RecoveryID == plan.Request.RecoveryID {
			b.Fatal("bounds refusal mutated recovery state")
		}
	}
}

// Measure existing frame headers without loading their payloads. This is a
// library-owned format measurement, never a caller recovery parser or repair.
func resourceLargestFrame(b *testing.B, root, group string) uint64 {
	b.Helper()
	dir := filepath.Join(root, group, walbackend.WALDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	var largest uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		file, err := os.Open(filepath.Join(dir, entry.Name()))
		if err != nil {
			b.Fatal(err)
		}
		for {
			var header [8]byte
			_, err := io.ReadFull(file, header[:])
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			encoded := binary.LittleEndian.Uint64(header[:])
			if encoded == 0 {
				break
			}
			length := encoded & ((1 << 56) - 1)
			padding := (encoded >> 56) & 7
			largest = max(largest, length)
			if _, err := file.Seek(int64(length+padding), io.SeekCurrent); err != nil {
				b.Fatal(err)
			}
		}
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
	}
	if largest < 2 {
		b.Fatal("source has no measurable WAL frames")
	}
	return largest
}
