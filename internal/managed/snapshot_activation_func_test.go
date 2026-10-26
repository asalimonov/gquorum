//go:build functional

package managed

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadsnapshot"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type activationIngress struct {
	loadgrpc.Ingress
	identity topology.Identity
	service  snapshot.Service
	receive  func(context.Context, load.RaftBatch) error
	report   func(context.Context, load.DeliveryReport) error
}

func (n *activationIngress) Identity() topology.Identity     { return n.identity }
func (n *activationIngress) LoadSnapshots() snapshot.Service { return n.service }
func (n *activationIngress) ReceiveLoadRaft(ctx context.Context, b load.RaftBatch) error {
	return n.receive(ctx, b)
}
func (n *activationIngress) ReportLoadDelivery(ctx context.Context, r load.DeliveryReport) error {
	return n.report(ctx, r)
}
func (n *activationIngress) ReadLoadReplica(_ context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	return load.ReplicaStatus{Peer: q.To}, nil
}

// This peer is a protocol double, not a second running replica. It accepts
// native ownership, then reports injected install failure; no installed state
// or durable replication is fabricated by the transport acknowledgement.
type activationOwner struct {
	key      loadsnapshot.OwnerKey
	ctx      context.Context
	accepted atomic.Uint64
}

func (o *activationOwner) Key() loadsnapshot.OwnerKey { return o.key }
func (o *activationOwner) Context() context.Context   { return o.ctx }
func (o *activationOwner) SnapshotBounds() raftengine.SnapshotBounds {
	return raftengine.SnapshotBounds{RunID: o.key.RunID}
}
func (*activationOwner) PreflightSnapshot(context.Context, raftpb.Message) (raftengine.SnapshotPreflight, error) {
	return raftengine.SnapshotPreflight{}, nil
}
func (*activationOwner) PrepareOwnedSnapshot(_ context.Context, m raftpb.Message) (raftstate.Snapshot, error) {
	return raftstate.DecodeRaftSnapshot(*m.Snapshot, "cluster", "work")
}
func (o *activationOwner) StepOwnedSnapshot(_ context.Context, in raftengine.SnapshotIngress) error {
	o.accepted.Store(in.Message.Snapshot.Metadata.Index)
	in.Done(raftengine.SnapshotInstallFailed, errors.New("injected protocol-double installation failure"))
	return nil
}
func TestManagedSnapshot_NativeReadyUsesProductionAdapterAndCommonIngress(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, founding := fixtureAuthority(t, f)
	listener := bufconn.Listen(1 << 20)
	options := loadgrpc.Options{Resolve: func(context.Context, topology.Identity) (string, error) { return "passthrough:///learner", nil },
		Authenticate: func(context.Context, topology.Identity) error { return nil }, DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) })}}
	sender, err := loadgrpc.New(options)
	must(t, err)
	f.cfg.Transport = sender
	h := f.host(t)
	view := loadsnapshot.NewService(h.cfg.SnapshotTransfer, func() (*loadsnapshot.Coordinator, error) { return h.snapshots, nil })
	must(t, sender.Bind(&activationIngress{identity: f.node, service: view, receive: h.ReceiveRaft, report: h.ReportDelivery}))
	peer, err := f.cfg.ResolvePeer(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 2})
	must(t, err)
	owner := &activationOwner{key: loadsnapshot.OwnerKey{Peer: peer, RunID: "protocol-double"}, ctx: t.Context()}
	receiver := loadsnapshot.NewCoordinator(h.cfg.SnapshotTransfer, loadsnapshot.Ports{Authorize: func(_ context.Context, key snapshot.Key) (loadsnapshot.Owner, error) {
		if key.To != peer {
			return nil, fault.ErrIdentityMismatch
		}
		return owner, nil
	}})
	receiver.Start(t.Context())
	received := loadsnapshot.NewService(h.cfg.SnapshotTransfer, func() (*loadsnapshot.Coordinator, error) { return receiver, nil })
	adapter, err := loadgrpc.New(options)
	must(t, err)
	var appendAfterReport atomic.Bool
	ingress := &activationIngress{identity: peer.Node, service: received}
	ingress.receive = func(ctx context.Context, b load.RaftBatch) error {
		var responses []raftpb.Message
		for _, m := range b.Messages {
			if m.Type == raftpb.MsgHeartbeat {
				responses = append(responses, raftpb.Message{Type: raftpb.MsgHeartbeatResp, From: 2, To: 1, Term: m.Term})
			}
			if m.Type == raftpb.MsgApp {
				if index := owner.accepted.Load(); index > 0 && m.Index >= index {
					appendAfterReport.Store(true)
					continue
				}
				responses = append(responses, raftpb.Message{Type: raftpb.MsgAppResp, From: 2, To: 1, Term: m.Term, Reject: true, Index: m.Index})
			}
		}
		if len(responses) == 0 {
			return nil
		}
		return h.ReceiveRaft(ctx, load.RaftBatch{From: peer, To: b.From, DeliveryID: b.DeliveryID, Messages: responses})
	}
	must(t, adapter.Bind(ingress))
	server := grpc.NewServer()
	adapter.Register(server)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		must(t, h.Stop(cleanup))
		must(t, sender.Close(cleanup))
		must(t, receiver.Stop(cleanup))
		must(t, adapter.Close(cleanup))
		server.Stop()
		_ = listener.Close()
		<-done
	})
	_, err = h.InstallHostFence(deadline(t), founding)
	must(t, err)
	group, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	await(deadline(t), t, group.Core().IsLeader)
	target := storage.Identity{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}
	action := f.approve(topology.StepAddLearner, target)
	request := f.request(action, f.localID(1))
	request.LocalTarget = storage.Identity{}
	grant := founding.Clone()
	grant.Key.Revision++
	grant.Action, grant.Resources = &action, grant.Resources[:1]
	grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2})
	request.Grant = grant
	await(deadline(t), t, func() bool {
		_, err = group.Core().InstallAuthority(deadline(t), grant)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	await(deadline(t), t, func() bool {
		_, err = h.ApplyAction(deadline(t), request)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	must(t, group.Core().Snapshot(deadline(t)))
	await(deadline(t), t, func() bool { return owner.accepted.Load() > 0 && appendAfterReport.Load() })
	await(deadline(t), t, func() bool {
		page := h.snapshots.List(snapshot.Query{})
		for _, row := range page.Items {
			if row.Direction == "outgoing" && row.Delivery == snapshot.DeliveryAccepted && row.ReservedBytes == 0 {
				return true
			}
		}
		return false
	})
	await(deadline(t), t, func() bool {
		page := receiver.List(snapshot.Query{})
		for _, row := range page.Items {
			if row.ReceiverOutcome == snapshot.ReceiverFailed && row.Delivery == snapshot.DeliveryAccepted {
				return true
			}
		}
		return false
	})
}
