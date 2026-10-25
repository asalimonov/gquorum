//go:build functional

package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fixtureSnapshotSender struct {
	gquorum.Node
	identity topology.Identity
}

func (n *fixtureSnapshotSender) Identity() topology.Identity { return n.identity }
func fixtureSnapshotClient(t *testing.T, a *application, credential string) (*loadgrpc.Transport, snapshot.Send) {
	t.Helper()
	identity := a.currentNode().Identity()
	from := identity
	from.NodeName, from.MemberID, from.InstanceID = "unapproved-sender", 2, "remote-process"
	token, err := loadgrpc.NewSharedToken(credential, false)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := loadgrpc.New(loadgrpc.Options{Resolve: func(context.Context, topology.Identity) (string, error) { return a.loadListener.Addr().String(), nil },
		Authenticate: token.Authenticate, DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(token)}})
	if err != nil {
		t.Fatal(err)
	}
	if err = sender.Bind(&fixtureSnapshotSender{Node: a.currentNode(), identity: from}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := sender.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	header, err := (&raftpb.Message{Type: raftpb.MsgSnap, From: 2, To: 1, Term: 1, Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("data")
	key := snapshot.Key{From: load.Peer{Node: from, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 2}},
		To: load.Peer{Node: identity, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 1}}, DeliveryID: 1}
	return sender, snapshot.Send{Manifest: snapshot.Manifest{Key: key, NativeHeader: header, TotalBytes: 4, SHA256: sha256.Sum256(data), ChunkBytes: 4}, Data: data}
}
func TestFixtureSnapshotProductionRegistrationAndCredentialRefusal(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	recoveryFixtureGroup(t, a)
	info := a.loadServer.GetServiceInfo()
	if _, found := info["gquorum.load.v1.Load"]; !found {
		t.Fatal("production RPC missing")
	}
	if _, found := info["gquorum.fixture.load.v1.Load"]; !found {
		t.Fatal("fixture RPC missing")
	}
	sender, send := fixtureSnapshotClient(t, a, "wrong-credential")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := a.generation.production.transport.ReadReplica(ctx, load.ReplicaQuery{From: a.currentNode().Identity(), To: send.Manifest.Key.To})
	if err != nil {
		t.Fatal("production ordinary RPC not wired", err)
	}
	_, err = sender.SendSnapshot(ctx, send)
	if !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("production snapshot authentication bypassed", err)
	}
	service := a.generation.production.snapshots
	service.mu.Lock()
	receipts := len(service.receipts)
	service.mu.Unlock()
	if receipts != 0 || a.currentNode().LoadSnapshots().List(snapshot.Query{}).Incoming != 0 {
		t.Fatal("unauthenticated snapshot dispatched")
	}
}
func TestFixtureSnapshotGenerationRetainsBlockedAckPump(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	recoveryFixtureGroup(t, a)
	old := a.generation
	listener, serviceCount := a.loadListener, len(a.loadServer.GetServiceInfo())
	entered, release := make(chan struct{}), make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	old.production.snapshots.mu.Lock()
	old.production.snapshots.faults.sendAck = func(_ context.Context, ack fixtureSnapshotWireAck) error {
		if ack.Key.To.Replica.GroupID != "work" {
			t.Error("receipt lost exact identity")
		}
		close(entered)
		<-release
		return context.Canceled
	}
	old.production.snapshots.mu.Unlock()
	sender, send := fixtureSnapshotClient(t, a, a.cfg.Credential)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sent := make(chan error, 1)
	go func() { _, err := sender.SendSnapshot(ctx, send); sent <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("production snapshot RPC did not reach its Ack pump")
	}
	cancel()
	select {
	case err := <-sent:
		if err == nil {
			t.Fatal("canceled handshake succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("client did not join")
	}
	// Finish Node ownership first. The borrowed adapter still owns the blocked
	// network pump, and the next generation cannot be installed until it joins.
	a.generationMu.Lock()
	old.accepting = false
	old.cancel()
	a.generationMu.Unlock()
	stop, cancelStop := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancelStop()
	if err := old.node.Stop(stop); err != nil {
		t.Fatal(err)
	}
	wait, cancelWait := context.WithTimeout(t.Context(), 20*time.Millisecond)
	err := a.stopNodeGeneration(wait)
	cancelWait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Ack pump ownership released early", err)
	}
	if err = a.restartNodeGeneration(stop); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("successor overlapped old adapter", err)
	}
	released.Do(func() { close(release) })
	if err = a.stopNodeGeneration(stop); err != nil {
		t.Fatal(err)
	}
	if err = a.restartNodeGeneration(stop); err != nil {
		t.Fatal(err)
	}
	if err = a.currentNode().WaitReady(stop); err != nil {
		t.Fatal(err)
	}
	if a.generation.production == old.production || a.currentNode().Identity().InstanceID == old.node.Identity().InstanceID {
		t.Fatal("production adapter redirected instead of replacing its exact binding")
	}
	_, err = a.generation.production.transport.ReadReplica(stop, load.ReplicaQuery{
		From: a.currentNode().Identity(), To: send.Manifest.Key.To})
	if !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("old recipient redirected into successor", err)
	}
	if a.loadListener != listener || len(a.loadServer.GetServiceInfo()) != serviceCount {
		t.Fatal("listener registration changed")
	}
}
