//go:build functional

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSyntheticSnapshotControlsAuthenticateAndFenceSuccessors(t *testing.T) {
	t.Parallel()
	a := ownedApplication(t)
	group := recoveryFixtureGroup(t, a)
	path := fmt.Sprintf("/internal/test/snapshot/work/%d", group.ReplicaID())
	request := httptest.NewRequest("GET", path, nil)
	response := httptest.NewRecorder()
	a.routes().ServeHTTP(response, request)
	if response.Code >= 200 && response.Code < 300 {
		t.Fatal("snapshot controls unauthenticated")
	}
	stale := a.currentNode().Identity()
	stale.InstanceID = "old"
	body := map[string]any{"identity": stale, "request_id": "configure", "spec": syntheticSpec{Version: 1, Seed: 7, EncodedBytes: 256 << 10}}
	if response = fixtureRequest(t.Context(), a, "POST", path+"/generate", body); response.Code == 200 {
		t.Fatal("stale control configured successor")
	}
	body["identity"] = a.currentNode().Identity()
	body["spec"] = syntheticSpec{Version: 1, Seed: 7, EncodedBytes: 1 << 40}
	if reply := fixtureRequest(t.Context(), a, "POST", path+"/generate", body); reply.Code == 200 {
		t.Fatal("oversized generation admitted before allocation")
	}
	body["spec"] = syntheticSpec{Version: 1, Seed: 7, EncodedBytes: 256 << 10}
	response = fixtureRequest(t.Context(), a, "POST", path+"/generate", body)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var generated syntheticObservation
	if err := json.Unmarshal(response.Body.Bytes(), &generated); err != nil {
		t.Fatal(err)
	}
	if generated.ApplicationBytes == 0 || generated.ApplicationSHA256 == "" || generated.Spec.Seed != 7 {
		t.Fatal(generated)
	}
	response = fixtureRequest(t.Context(), a, "GET", path, nil)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var observed snapshotControlObservation
	if err := json.Unmarshal(response.Body.Bytes(), &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Application.ApplicationSHA256 != generated.ApplicationSHA256 {
		t.Fatal("observation lost generated state")
	}
}
func TestSnapshotPhaseCancellationKeepsExactOwnerAndJoins(t *testing.T) {
	t.Parallel()
	id := topology.Identity{NodeName: "node", ClusterID: "cluster", InstanceID: "process", MemberID: 1}
	a := &application{clusterHooks: newClusterHooks()}
	key := walKey{"work", 2}
	control := &snapshotControl{request: snapshotControlRequest{Identity: id, Hold: "before_restore"}, release: make(chan struct{}), hits: make(map[string]uint64)}
	a.clusterHooks.snapshotControls[key] = control
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.snapshotPhase(ctx, id, key, "before_restore", 0) }()
	apiEventually(t, func() bool { a.clusterHooks.mu.Lock(); defer a.clusterHooks.mu.Unlock(); return control.active == 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("phase did not join")
	}
	a.clusterHooks.mu.Lock()
	active := control.active
	hits := control.hits["before_restore"]
	a.clusterHooks.mu.Unlock()
	if active != 0 || hits != 1 {
		t.Fatal(active, hits)
	}
	id.InstanceID = "replacement"
	if err := a.snapshotPhase(t.Context(), id, key, "before_restore", 0); err != nil {
		t.Fatal("old control affected replacement", err)
	}
}
func TestSnapshotWireLossIsTransientAndCorruptionIsBounded(t *testing.T) {
	t.Parallel()
	id := topology.Identity{NodeName: "node", InstanceID: "process"}
	a := &application{clusterHooks: newClusterHooks()}
	key := walKey{"work", 2}
	offset := uint64(0)
	a.clusterHooks.snapshotControls[key] = &snapshotControl{request: snapshotControlRequest{Identity: id, CorruptOffset: &offset, DropAdmissionAck: true, DropFinalAck: true}, hits: make(map[string]uint64)}
	transfer := snapshot.Key{To: load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: key.group, ReplicaID: key.replica}}, DeliveryID: 1}
	data := []byte("unchanged")
	chunk := snapshot.Chunk{Session: snapshot.Session{Key: transfer}, Data: data}
	corrupted, err := a.snapshotWrite(t.Context(), chunk)
	if err != nil || corrupted.Data[0] == data[0] || string(data) != "unchanged" {
		t.Fatal("fixture mutated borrowed chunk", err)
	}
	again, err := a.snapshotWrite(t.Context(), chunk)
	if err != nil || string(again.Data) != string(data) {
		t.Fatal("corruption repeated")
	}
	ack := fixtureSnapshotWireAck{Key: transfer, State: snapshot.DeliveryReceiving}
	if err = a.snapshotWireAck(t.Context(), ack); status.Code(err) != codes.Unavailable {
		t.Fatal("loss became a false success/refusal", err)
	}
	if err = a.snapshotWireAck(t.Context(), ack); err != nil {
		t.Fatal("admission loss repeated", err)
	}
	ack.State = snapshot.DeliveryAccepted
	if err = a.snapshotWireAck(t.Context(), ack); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}
