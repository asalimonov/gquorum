package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func parkedRequest(name, instance string) joinRequest {
	return joinRequest{Identity: Identity{NodeName: name, InstanceID: instance, ClusterID: "cluster", MemberID: 7}, Node: topology.Node{Name: name, Type: "worker"}, Address: "127.0.0.1:1234", LastDocVersion: 3}
}
func TestParkedRegistry_UpsertAttemptsExpiryRemoval(t *testing.T) {
	var registry parkedRegistry
	req := parkedRequest("node2", "a")
	registry.record(req, parkedExcluded, "excluded")
	first := registry.snapshot()
	if len(first) != 1 || first[0].Attempts != 1 || first[0].Reason != "excluded" || first[0].ReportedMemberID != 7 || first[0].ReportedDocVersion != 3 {
		t.Fatal(first)
	}
	firstSeen := first[0].FirstSeen
	req.Address = "127.0.0.1:1235"
	req.LastDocVersion = 4
	registry.record(req, parkedExcluded, "excluded again")
	second := registry.snapshot()
	if len(second) != 1 || second[0].Attempts != 2 || !second[0].FirstSeen.Equal(firstSeen) || second[0].Address != req.Address || second[0].ReportedDocVersion != 4 {
		t.Fatal(second)
	}
	registry.record(parkedRequest("node3", "c"), parkedUnapproved, "not approved")
	registry.record(parkedRequest("", "blank"), parkedExcluded, "nameless")
	snap := registry.snapshot()
	if len(snap) != 2 || snap[0].LastSeen.Before(snap[1].LastSeen) {
		t.Fatal(snap)
	}
	// Snapshots are values, never aliases into the registry.
	snap[0].Reason = "changed"
	if registry.snapshot()[0].Reason == "changed" {
		t.Fatal("snapshot aliases registry")
	}
	key := parkedKey{"node3", "c"}
	entry := registry.entries[key]
	entry.LastSeen = time.Now().Add(-parkedJoinTTL - time.Minute)
	registry.entries[key] = entry
	if after := registry.snapshot(); len(after) != 1 || after[0].Name != "node2" {
		t.Fatal(after)
	}
	registry.clear("node2", "a")
	if registry.snapshot() != nil {
		t.Fatal("admitted instance remains parked")
	}
}
func TestParkedRegistry_KeyedByNameAndInstance(t *testing.T) {
	var registry parkedRegistry
	registry.record(parkedRequest("node2", "holder"), parkedExcluded, "excluded")
	registry.record(parkedRequest("node2", "intruder"), parkedNameConflict, "conflict")
	if len(registry.snapshot()) != 2 {
		t.Fatal("name-only registry collapsed processes")
	}
	for _, kind := range []string{"not_leader", "unavailable", "identity_mismatch", "invalid", ""} {
		registry.record(parkedRequest("other", "transient"), kind, "transient")
	}
	if len(registry.snapshot()) != 2 {
		t.Fatal("transient refusal parked")
	}
	for _, kind := range []string{parkedExcluded, parkedUnapproved, parkedSuperseded} {
		registry.record(parkedRequest("other", kind), kind, "refused")
	}
	if len(registry.snapshot()) != 5 {
		t.Fatal("actionable refusal missing")
	}
	registry.clear("node2", "holder")
	if _, ok := registry.entries[parkedKey{"node2", "intruder"}]; !ok {
		t.Fatal("admitting holder erased intruder warning")
	}
	registry.record(parkedRequest("node2", "old"), parkedExcluded, "stale")
	registry.clear("node2", "new")
	if _, ok := registry.entries[parkedKey{"node2", "old"}]; ok {
		t.Fatal("earlier incarnation survived admission")
	}
}
func TestParkedRegistry_SweepExpiresWithoutAReader(t *testing.T) {
	var registry parkedRegistry
	registry.record(parkedRequest("stale", "old"), parkedExcluded, "excluded")
	registry.record(parkedRequest("retrying", "new"), parkedExcluded, "excluded")
	now := time.Now()
	key := parkedKey{"stale", "old"}
	entry := registry.entries[key]
	entry.LastSeen = now.Add(-parkedJoinTTL - time.Minute)
	registry.entries[key] = entry
	if registry.sweep(now) != 1 || registry.sweep(now) != 0 {
		t.Fatal("sweep is not idempotent")
	}
	if _, ok := registry.entries[parkedKey{"retrying", "new"}]; !ok {
		t.Fatal("retrying process expired")
	}
}
func TestParkedRegistry_CapsCardinalityByEvictingOldest(t *testing.T) {
	var registry parkedRegistry
	for i := 0; i < parkedJoinCap; i++ {
		registry.record(parkedRequest("same", fmt.Sprint(i)), parkedExcluded, "excluded")
	}
	key := parkedKey{"same", "0"}
	entry := registry.entries[key]
	entry.LastSeen = time.Now().Add(-time.Minute)
	registry.entries[key] = entry
	registry.record(parkedRequest("same", "new"), parkedExcluded, "excluded")
	if len(registry.entries) != parkedJoinCap {
		t.Fatal("registry exceeded cap")
	}
	if _, ok := registry.entries[key]; ok {
		t.Fatal("oldest process was not evicted")
	}
}
func TestParkedJoins_NilReceiver(t *testing.T) {
	var g *Group
	records, err := g.ParkedJoins(context.Background())
	if err != nil || records != nil {
		t.Fatal(records, err)
	}
}
func TestHandleJoin_RefusalRecordsParkAndFollowersReadCurrentLeader(t *testing.T) {
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var follower *Group
	for _, g := range groups {
		if g != leader {
			follower = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req := parkedRequest("stranger", "instance")
	data, _ := json.Marshal(req)
	header := &metapb.Header{ClusterId: "cluster", NodeName: req.Identity.NodeName, MemberId: req.Identity.MemberID}
	reply := follower.Call(ctx, "Join", header, data)
	if reply.ErrorCode == "" || len(follower.parked.snapshot()) != 0 {
		t.Fatal("follower redirect parked a process", reply)
	}
	reply = leader.Call(ctx, "Join", header, data)
	if reply.ErrorCode == "" {
		t.Fatal("unapproved node admitted")
	}
	records, err := follower.ParkedJoins(ctx)
	if err != nil || len(records) != 1 || records[0].RefusalKind != parkedUnapproved || records[0].ReportedMemberID != 7 || records[0].ReportedDocVersion != 3 {
		t.Fatal(records, err)
	}
	before := len(leader.parked.snapshot())
	header.ClusterId = "other"
	leader.Call(ctx, "Join", header, data)
	header.ClusterId = "cluster"
	header.NodeName = ""
	req = parkedRequest("", "bad")
	data, _ = json.Marshal(req)
	leader.Call(ctx, "Join", header, data)
	if len(leader.parked.snapshot()) != before {
		t.Fatal("malformed/foreign-cluster join parked")
	}
	// A follower refuses its local registry over the wire, even when it has old entries.
	follower.parked.record(parkedRequest("old-leader-only", "old"), parkedExcluded, "stale")
	if err = metatransport.ReplyError(follower.Call(ctx, "ParkedRead", follower.transport.Header(), nil)); err == nil {
		t.Fatal("follower published stale local registry")
	}
}
