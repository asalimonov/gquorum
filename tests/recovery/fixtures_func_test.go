//go:build functional

package recovery_test

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
)

func authorityNodeOptions(t testing.TB, root, instance string) quorum.NodeOptions {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return quorum.NodeOptions{StorageDir: root, Identity: topology.Identity{ClusterID: "cluster", NodeName: "n1", InstanceID: instance}, Bootstrap: quorum.BootstrapOptions{Founders: []quorum.FoundingMember{{Node: topology.Node{Name: "n1", Type: "worker"}, MemberID: 1, Address: listener.Addr().String()}}}, Meta: quorum.MetaOptions{Network: quorum.MetaNetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("test")}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func stopAuthorityNode(t testing.TB, n quorum.Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

// ensureMachine is a minimal state machine for single-replica groups.
type ensureMachine struct{}

func (ensureMachine) Apply(context.Context, load.Command) (load.Result, error) {
	return load.Result{Code: "ok"}, nil
}
func (ensureMachine) Snapshot(context.Context) ([]byte, error) { return []byte("{}"), nil }
func (ensureMachine) Restore(context.Context, []byte) error    { return nil }

// loopbackTransport delivers a single node's own load effects back to it.
type loopbackTransport struct{ node quorum.Node }

func (l *loopbackTransport) SendRaft(ctx context.Context, b load.RaftBatch) error {
	return l.node.ReceiveLoadRaft(ctx, b)
}
func (l *loopbackTransport) ReadReplica(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	return l.node.ReadLoadReplica(ctx, q)
}
func (l *loopbackTransport) ApplyAction(ctx context.Context, a load.ActionRequest) (load.ActionResult, error) {
	return l.node.ApplyLoadAction(ctx, a)
}
func (l *loopbackTransport) InstallFence(ctx context.Context, f load.FenceRequest) (load.FenceResult, error) {
	return l.node.InstallLoadFence(ctx, f)
}

func singleReplicaSpec(id string, labels map[string]string) quorum.GroupSpec {
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.TargetVoters, voterConfig.MaxSurgeVoters = 1, 0
	return quorum.GroupSpec{ID: id, Labels: labels, VoterConfig: &voterConfig, Nodes: []string{"n1"}, Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 1}}
}

type fixtureNode struct {
	quorum.Node
	options quorum.NodeOptions
}

func newNode(options quorum.NodeOptions) (quorum.Node, error) {
	participant, err := quorum.NewNode(options)
	if err != nil {
		return nil, err
	}
	return &fixtureNode{Node: participant, options: options}, nil
}

func (l *loopbackTransport) SendSnapshot(ctx context.Context, send snapshot.Send) (snapshot.Ack, error) {
	return sendLoopbackSnapshot(ctx, l.node.LoadSnapshots(), send)
}

func sendLoopbackSnapshot(ctx context.Context, service snapshot.Service, send snapshot.Send) (snapshot.Ack, error) {
	if service == nil {
		return snapshot.Ack{}, &snapshot.TransferError{Reason: snapshot.ReasonUnsupported}
	}
	policy := service.Options()
	remaining := policy.TotalTimeout
	if due, ok := ctx.Deadline(); ok {
		remaining = min(remaining, time.Until(due))
	}
	ack, err := service.Begin(ctx, snapshot.BeginRequest{Manifest: send.Manifest, RemainingTotal: remaining, ProgressTimeout: min(policy.ProgressTimeout, remaining)})
	if err != nil {
		return ack, err
	}
	if ack.State != snapshot.DeliveryNotNeeded {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			_ = service.Detach(cleanup, ack.Session)
		}()
	}
	report := func(ack snapshot.Ack) {
		if send.ReportProgress != nil {
			send.ReportProgress(snapshot.Progress{
				ReceiverRunID: ack.ReceiverRunID, VerifiedBytes: ack.NextOffset, State: ack.State, RemainingTotal: ack.RemainingTotal, ProgressTimeout: ack.ProgressTimeout})
		}
	}
	report(ack)
	if ack.State == snapshot.DeliveryNotNeeded || ack.State == snapshot.DeliveryAccepted {
		return ack, nil
	}
	if ack.ChunkBytes == 0 {
		return snapshot.Ack{}, &snapshot.TransferError{Reason: snapshot.ReasonMalformed}
	}
	for ack.NextOffset < uint64(len(send.Data)) {
		offset := ack.NextOffset
		n := min(uint64(ack.ChunkBytes), uint64(len(send.Data))-offset)
		data := send.Data[offset : offset+n]
		ack, err = service.Write(ctx, snapshot.Chunk{Session: ack.Session, Offset: offset, Ordinal: offset / uint64(ack.ChunkBytes), Data: data, SHA256: sha256.Sum256(data)})
		if err != nil {
			return ack, err
		}
		report(ack)
	}
	ack, err = service.Finish(ctx, ack.Session)
	if err == nil {
		report(ack)
	}
	return ack, err
}
