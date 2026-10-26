//go:build functional

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/metadata"
)

func ownedApplication(t *testing.T) *application {
	t.Helper()
	var ls listeners
	for _, target := range []*net.Listener{&ls.meta, &ls.load, &ls.http} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		*target = listener
		t.Cleanup(func() { _ = listener.Close() })
	}
	cfg := config{NodeName: "one", NodeType: "worker", ClusterID: "cluster", InstanceID: "initial", Credential: "credential", DataDir: t.TempDir(), TickInterval: 10 * time.Millisecond, SessionTTL: 3 * time.Second, NodeOwnedWAL: true}
	cfg.Peers = []peer{{Name: cfg.NodeName, Type: cfg.NodeType, MemberID: 1, MetaAddress: ls.meta.Addr().String(), LoadAddress: ls.load.Addr().String(), HTTPAddress: ls.http.Addr().String()}}
	a, err := newApplication(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), &ls)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if a.store != nil {
		t.Fatal("owned fixture opened an extra store")
	}
	if err := a.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return a
}

func fixtureRequest(ctx context.Context, a *application, method, path string, body any) *httptest.ResponseRecorder {
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(data)).WithContext(ctx)
	req.Header.Set("X-Gquorum-Test-Credential", a.cfg.Credential)
	w := httptest.NewRecorder()
	a.routes().ServeHTTP(w, req)
	return w
}

func TestOwnedFixtureRetainedRestartAndConcurrentIngress(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	voters := topology.DefaultVoterConfig()
	voters.TargetVoters = 1
	created, err := a.currentCluster().CreateGroup(ctx, gquorum.GroupSpec{ID: "work", VoterConfig: &voters}, gquorum.MutationOptions{ExpectedVersion: 1, Actor: "test", Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.currentCluster().WaitOperation(ctx, created.OperationID); err != nil {
		t.Fatal(err)
	}
	old, found := a.currentNode().LoadGroup("work")
	if !found {
		t.Fatal("no published replica")
	}
	original := a.currentNode().Identity()
	for i := range 2 {
		key := fmt.Sprintf("key-%d", i)
		command, _ := json.Marshal(putCommand{RequestID: key, Key: key, Value: "value"})
		g, found := a.currentNode().LoadGroup("work")
		if !found {
			t.Fatal("no recovered handle")
		}
		apiEventually(t, func() bool { return g.IsLeader() })
		if result, err := g.Propose(ctx, load.Proposal{RequestID: key, Data: command}); err != nil || result.Result.Code != "ok" {
			t.Fatal(result, err)
		}
		if err := g.Snapshot(ctx); err != nil {
			t.Fatal(err)
		}
		if w := fixtureRequest(ctx, a, "POST", "/internal/test/node/stop", nil); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if w := fixtureRequest(ctx, a, "GET", "/health/live", nil); w.Code != 200 {
			t.Fatal("process liveness depended on Node", w.Code)
		}
		if w := fixtureRequest(ctx, a, "GET", "/internal/test/node/status", nil); w.Code != 200 {
			t.Fatal("management stopped with Node", w.Code)
		}
		if w := fixtureRequest(ctx, a, "GET", "/internal/test/local/work", nil); w.Code != 503 {
			t.Fatal("stopped handle remained available", w.Code)
		}
		// Delivery and HTTP requests race the installation of a new generation. The
		// old destination incarnation must never be redirected into that participant.
		from := load.Peer{Node: original, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: old.ReplicaID()}}
		batch := load.RaftBatch{From: from, To: from, Messages: []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: old.ReplicaID(), To: old.ReplicaID()}}}
		payload, _ := json.Marshal(batch)
		frame := &loadpb.Frame{Header: &loadpb.Header{ClusterId: original.ClusterID, NodeName: original.NodeName, InstanceId: original.InstanceID, MemberId: original.MemberID}, GroupId: "work", ToReplicaId: old.ReplicaID(), RaftMessage: payload}
		ingressCtx, stopIngress := context.WithCancel(ctx)
		authenticated := metadata.NewIncomingContext(ingressCtx, metadata.Pairs(credentialHeader, a.cfg.Credential))
		var wg sync.WaitGroup
		failure := make(chan error, 1)
		wg.Go(func() {
			for ingressCtx.Err() == nil {
				reply, err := a.Deliver(authenticated, frame)
				if err != nil || replyError(reply) == nil {
					select {
					case failure <- fmt.Errorf("old delivery accepted: %v %v", reply, err):
					default:
					}
					return
				}
				w := fixtureRequest(ingressCtx, a, "GET", "/internal/test/local/work", nil)
				if w.Code != 200 && w.Code != 503 {
					select {
					case failure <- fmt.Errorf("concurrent observation: %d", w.Code):
					default:
					}
					return
				}
			}
		})
		w := fixtureRequest(ctx, a, "POST", "/internal/test/node/start", nil)
		if w.Code != 200 {
			stopIngress()
			wg.Wait()
			t.Fatal(w.Code, w.Body.String())
		}
		if err := a.currentNode().WaitReady(ctx); err != nil {
			stopIngress()
			wg.Wait()
			t.Fatal(err)
		}
		stopIngress()
		wg.Wait()
		select {
		case err := <-failure:
			t.Fatal(err)
		default:
		}
		if a.currentNode().Identity().InstanceID == original.InstanceID {
			t.Fatal("restart reused process identity")
		}
		apiEventually(t, func() bool { return a.local("work") != nil })
		for j := 0; j <= i; j++ {
			if got := a.local("work").machine.values()[fmt.Sprintf("key-%d", j)]; got != "value" {
				t.Fatal("replay lost application state", i, j, got)
			}
		}
		if old.Running() {
			t.Fatal("old handle redirected into successor")
		}
	}
	local := fixtureRequest(ctx, a, "GET", "/internal/test/local/work", nil)
	var observation localObservation
	if local.Code != 200 || json.Unmarshal(local.Body.Bytes(), &observation) != nil || observation.StorageAvailable || observation.WALControlAvailable || observation.StorageIdentity != nil || observation.GroupIdentity == nil {
		t.Fatal("owned observation fabricated physical evidence", local.Code, local.Body.String())
	}
	for _, route := range []struct{ method, path string }{{"POST", "/internal/test/lose-store/work/1"}, {"PUT", "/internal/test/wal/work"}, {"DELETE", "/internal/test/wal/work"}, {"GET", "/internal/test/collections/work/1"}} {
		w := fixtureRequest(ctx, a, route.method, route.path, nil)
		if w.Code != 400 {
			t.Fatal("unsupported owned-store fault control accepted", route, w.Code, w.Body.String())
		}
	}
}

func TestGenerationStopWaitsForAcceptedRequest(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	call, done, err := a.beginEffect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	short, stopShort := context.WithCancel(ctx)
	stopShort()
	if err := a.stopNodeGeneration(short); err == nil {
		done()
		t.Fatal("stop forgot accepted request")
	}
	select {
	case <-call.Done():
	case <-ctx.Done():
		done()
		t.Fatal("stop did not cancel old request")
	}
	if err := a.restartNodeGeneration(ctx); err != fault.ErrUnavailable {
		done()
		t.Fatal("replacement overlapped old request", err)
	}
	done()
	if err := a.stopNodeGeneration(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.restartNodeGeneration(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFailedGenerationStartReleasesUntransferredListener(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.stopNodeGeneration(ctx); err != nil {
		t.Fatal(err)
	}
	prewrite := false
	store, err := walstore.Open(walstore.Options{RootDir: a.nodeOptions.Loads.Storage.WAL.RootDir, NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, SegmentBytes: 1 << 20, Prewrite: &prewrite})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := a.restartNodeGeneration(ctx); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("contended generation started", err)
	}
	if err := a.stopNodeGeneration(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.restartNodeGeneration(ctx); err != nil {
		t.Fatal("failed startup leaked its listener", err)
	}
	if err := a.currentNode().WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}
